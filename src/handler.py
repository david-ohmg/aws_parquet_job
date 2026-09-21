"""Daily Parquet log refresh + VoIP download writeback.

Migrated from the Cowork scheduled task "Refresh Parquet table + VoIP download writeback".

Part 1  Drop and rebuild the trailing 7 complete UTC days of
        ohmg_pub_logs.s3_access_logs_parquet from the raw S3 access-log table.
Part 2  Reconcile open VoIP productions against the rebuilt table and PUT the
        earliest genuine download timestamp back to the production API.

Event flags (all optional):
    {"dry_run": true}      Read-only. Skips S3 deletes, partition drops, the
                           INSERT, and API PUTs, and reports what it would do.
    {"skip_part1": true}   Skip the rebuild and run only the reconciliation.
"""

import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------- #
# Configuration (all overridable through Lambda environment variables)
# --------------------------------------------------------------------------- #
REGION = os.environ.get("AWS_REGION", "us-east-2")
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "ohmg_pub_logs")
RAW_TABLE = os.environ.get("RAW_TABLE", "s3_access_logs")
PARQUET_TABLE = os.environ.get("PARQUET_TABLE", "s3_access_logs_parquet")
PARQUET_BUCKET = os.environ.get("PARQUET_BUCKET", "ohmg-pub-athena-output")
PARQUET_PREFIX = os.environ.get("PARQUET_PREFIX", "converted/s3_access_logs_parquet/")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://ohmg-pub-athena-output/")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
ATHENA_POLL_SECONDS = float(os.environ.get("ATHENA_POLL_SECONDS", "3"))
REBUILD_DAYS = int(os.environ.get("REBUILD_DAYS", "7"))

VOIP_API_BASE = os.environ.get("VOIP_API_BASE", "https://test.onholdmediagroup.com").rstrip("/")
VOIP_TOKEN_PARAM = os.environ.get("VOIP_TOKEN_PARAM", "/ohmg/voip-api/token")
EXCLUDED_IPS = {
    ip.strip() for ip in os.environ.get("EXCLUDED_IPS", "143.110.210.93").split(",") if ip.strip()
}
KEY_PREFIX = os.environ.get("VOIP_KEY_PREFIX", "static/voip-download/")
KEY_CHUNK_SIZE = int(os.environ.get("KEY_CHUNK_SIZE", "400"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))

SAFE_FILENAME = re.compile(r"^[A-Za-z0-9._\-]+$")
LOG_TS_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

_s3 = boto3.client("s3", region_name=REGION)
_glue = boto3.client("glue", region_name=REGION)
_athena = boto3.client("athena", region_name=REGION)
_ssm = boto3.client("ssm", region_name=REGION)


class RunReport:
    """Collects hard failures (raise at the end) and warnings (log only)."""

    def __init__(self, dry_run):
        self.dry_run = dry_run
        self.failures = []
        self.warnings = []
        self.summary = {}

    def fail(self, msg):
        logger.error("FAILURE: %s", msg)
        self.failures.append(msg)

    def warn(self, msg):
        logger.warning("WARNING: %s", msg)
        self.warnings.append(msg)


# --------------------------------------------------------------------------- #
# Athena helpers
# --------------------------------------------------------------------------- #
def run_athena(sql):
    """Run a query and block until it finishes. Returns the query execution id."""
    resp = _athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    qid = resp["QueryExecutionId"]
    while True:
        status = _athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return qid
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "no reason given")
            raise RuntimeError(f"Athena query {qid} {state}: {reason}")
        time.sleep(ATHENA_POLL_SECONDS)


def athena_rows(qid):
    """Return result rows as a list of dicts keyed by column name."""
    rows = []
    header = None
    for page in _athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            values = [d.get("VarCharValue") for d in row["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values)))
    return rows


def sql_quote(value):
    return "'" + value.replace("'", "''") + "'"


# --------------------------------------------------------------------------- #
# Part 1: rebuild trailing days of the Parquet table
# --------------------------------------------------------------------------- #
def rebuild_dates(today=None):
    """The N complete UTC days strictly before today, oldest first."""
    today = today or datetime.now(timezone.utc).date()
    return [(today - timedelta(days=n)).isoformat() for n in range(REBUILD_DAYS, 0, -1)]


def delete_partition_objects(day, report):
    prefix = f"{PARQUET_PREFIX}log_date={day}/"
    keys = []
    for page in _s3.get_paginator("list_objects_v2").paginate(Bucket=PARQUET_BUCKET, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    if report.dry_run:
        return len(keys)
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        resp = _s3.delete_objects(
            Bucket=PARQUET_BUCKET,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        for err in resp.get("Errors", []):
            report.fail(f"S3 delete failed for {err.get('Key')}: {err.get('Code')} {err.get('Message')}")
    return len(keys)


def drop_partitions(dates, report):
    if report.dry_run:
        return
    resp = _glue.batch_delete_partition(
        DatabaseName=GLUE_DATABASE,
        TableName=PARQUET_TABLE,
        PartitionsToDelete=[{"Values": [d]} for d in dates],
    )
    for err in resp.get("Errors", []):
        detail = err.get("ErrorDetail", {})
        # A date with no registered partition is expected and harmless.
        if detail.get("ErrorCode") == "EntityNotFoundException":
            continue
        report.fail(f"Glue BatchDeletePartition error for {err.get('PartitionValues')}: {detail}")


def build_insert_sql(oldest):
    return f"""INSERT INTO {GLUE_DATABASE}.{PARQUET_TABLE}
SELECT
  bucketowner, bucket_name, requestdatetime, remoteip, requester, requestid, operation, key, request_uri, httpstatus, errorcode, bytessent, objectsize, totaltime, turnaroundtime, referrer, useragent, versionid, hostid, sigv, ciphersuite, authtype, endpoint, tlsversion, accesspointarn, aclrequired,
  date_format(date_parse(requestdatetime, '%d/%b/%Y:%H:%i:%s +0000'), '%Y-%m-%d') AS log_date
FROM {GLUE_DATABASE}.{RAW_TABLE}
WHERE date_parse(requestdatetime, '%d/%b/%Y:%H:%i:%s +0000') >= date_parse('{oldest}', '%Y-%m-%d')
  AND date_parse(requestdatetime, '%d/%b/%Y:%H:%i:%s +0000') < date_trunc('day', current_timestamp)"""


def part1_refresh(report, today=None):
    dates = rebuild_dates(today)
    oldest, newest = dates[0], dates[-1]

    deleted = {d: delete_partition_objects(d, report) for d in dates}
    logger.info("Parquet objects %s per date: %s", "found" if report.dry_run else "deleted", deleted)

    drop_partitions(dates, report)

    if report.failures:
        # Do not rebuild on top of a partially cleaned prefix: that would duplicate rows.
        logger.error("Skipping INSERT because cleanup reported errors")
        report.summary["part1"] = {"dates": dates, "rebuilt": False}
        return

    if report.dry_run:
        logger.info("Dry run: would run INSERT for %s to %s", oldest, newest)
        report.summary["part1"] = {"dates": dates, "rebuilt": False, "dry_run": True}
        return

    run_athena(build_insert_sql(oldest))

    qid = run_athena(
        f"SELECT log_date, count(*) AS n FROM {GLUE_DATABASE}.{PARQUET_TABLE} "
        f"WHERE log_date BETWEEN '{oldest}' AND '{newest}' GROUP BY log_date ORDER BY log_date"
    )
    counts = {r["log_date"]: int(r["n"]) for r in athena_rows(qid)}
    for d in dates:
        if counts.get(d, 0) == 0:
            report.fail(f"Parquet rebuild produced no rows for log_date={d}")
    report.summary["part1"] = {"dates": dates, "rebuilt": True, "row_counts": counts}


# --------------------------------------------------------------------------- #
# Part 2: VoIP download reconciliation
# --------------------------------------------------------------------------- #
def get_token():
    return _ssm.get_parameter(Name=VOIP_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]


def api_call(method, path, token, body=None, retries=3):
    """Returns (status_code, parsed_json_or_None). Retries 5xx and network errors."""
    url = f"{VOIP_API_BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Token {token}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    last_exc = None
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                raw = resp.read().decode() or "null"
                return resp.status, json.loads(raw)
        except urllib.error.HTTPError as exc:
            if exc.code < 500:
                return exc.code, None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        time.sleep(2 ** attempt)
    raise RuntimeError(f"{method} {path} failed after {retries} attempts: {last_exc}")


def filename_from_url(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def parse_log_ts(value):
    return datetime.strptime(value, LOG_TS_FORMAT).astimezone(timezone.utc)


def earliest_downloads(keys):
    """Query the Parquet table for GETs of the given keys.

    Returns (confirmed, excluded_only) where confirmed maps key -> earliest UTC
    datetime from a non-excluded IP, and excluded_only lists keys whose only
    matching downloads came from an excluded IP.
    """
    confirmed, seen_any = {}, set()
    for i in range(0, len(keys), KEY_CHUNK_SIZE):
        chunk = keys[i:i + KEY_CHUNK_SIZE]
        in_list = ", ".join(sql_quote(k) for k in chunk)
        qid = run_athena(
            "SELECT key, requestdatetime, remoteip, httpstatus "
            f"FROM {GLUE_DATABASE}.{PARQUET_TABLE} "
            "WHERE operation = 'REST.GET.OBJECT' AND httpstatus IN ('200','206') "
            f"AND key IN ({in_list})"
        )
        for row in athena_rows(qid):
            seen_any.add(row["key"])
            if row["remoteip"] in EXCLUDED_IPS:
                continue
            ts = parse_log_ts(row["requestdatetime"])
            if row["key"] not in confirmed or ts < confirmed[row["key"]]:
                confirmed[row["key"]] = ts
    excluded_only = sorted(seen_any - set(confirmed))
    return confirmed, excluded_only


def part2_reconcile(report):
    token = get_token()

    status, productions = api_call("GET", "/api/voip-list/", token)
    if status != 200 or not isinstance(productions, list):
        report.fail(f"voip-list returned HTTP {status}")
        return

    key_to_prod = {}
    for p in productions:
        name = filename_from_url(p.get("url", ""))
        if not SAFE_FILENAME.match(name):
            report.warn(f"Skipping prod_id={p.get('prod_id')}: unexpected filename {name!r}")
            continue
        key_to_prod[KEY_PREFIX + name] = p

    confirmed, excluded_only = earliest_downloads(list(key_to_prod))
    for key in excluded_only:
        report.warn(
            f"Only excluded-IP downloads for prod_id={key_to_prod[key].get('prod_id')} "
            f"({key}); nothing written. Review manually."
        )

    written = skipped = 0
    for key, ts in confirmed.items():
        prod_id = key_to_prod[key]["prod_id"]
        stamp = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        status, lookup = api_call("GET", f"/api/voip-lookup/{prod_id}/", token)
        if status != 200 or not lookup or "id" not in lookup:
            report.warn(f"voip-lookup for prod_id={prod_id} returned HTTP {status}; skipped")
            skipped += 1
            continue
        if report.dry_run:
            logger.info("Dry run: would PUT downloaded_date=%s for prod_id=%s (id=%s)", stamp, prod_id, lookup["id"])
            written += 1
            continue
        status, _ = api_call("PUT", f"/api/voip-update/{lookup['id']}/", token, {"downloaded_date": stamp})
        if status == 200:
            written += 1
        else:
            report.fail(f"voip-update for prod_id={prod_id} (id={lookup['id']}) returned HTTP {status}")

    report.summary["part2"] = {
        "productions": len(productions),
        "downloaded": len(confirmed),
        "written": written,
        "skipped": skipped,
        "excluded_ip_only": len(excluded_only),
    }


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def lambda_handler(event, context):
    event = event or {}
    report = RunReport(dry_run=bool(event.get("dry_run")))
    logger.info("Starting run (dry_run=%s, skip_part1=%s, api=%s)",
                report.dry_run, bool(event.get("skip_part1")), VOIP_API_BASE)

    if not event.get("skip_part1"):
        try:
            part1_refresh(report)
        except Exception as exc:  # keep going: Part 2 can still use the existing table
            logger.exception("Part 1 crashed")
            report.fail(f"Part 1 crashed: {exc}")

    try:
        part2_reconcile(report)
    except Exception as exc:
        logger.exception("Part 2 crashed")
        report.fail(f"Part 2 crashed: {exc}")

    result = {
        "dry_run": report.dry_run,
        "summary": report.summary,
        "warnings": report.warnings,
        "failures": report.failures,
    }
    logger.info("Run complete: %s", json.dumps(result, default=str))

    if report.failures:
        # Raising marks the invocation as an error so the CloudWatch alarm fires.
        raise RuntimeError("; ".join(report.failures))
    return result
