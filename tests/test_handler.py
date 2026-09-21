import os
import sys
import unittest
from datetime import date, datetime, timezone
from unittest import mock

os.environ.setdefault("AWS_REGION", "us-east-2")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ["ATHENA_POLL_SECONDS"] = "0"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import handler  # noqa: E402


class FakeAthena:
    """Answers start_query_execution/get_query_execution/get_query_results from a script."""

    def __init__(self, results_by_match=None, fail_on=None):
        self.queries = []
        self.results_by_match = results_by_match or {}
        self.fail_on = fail_on
        self._results = {}

    def start_query_execution(self, QueryString, **kwargs):
        qid = f"q{len(self.queries)}"
        self.queries.append(QueryString)
        rows = []
        for needle, r in self.results_by_match.items():
            if needle in QueryString:
                rows = r
        self._results[qid] = (QueryString, rows)
        return {"QueryExecutionId": qid}

    def get_query_execution(self, QueryExecutionId):
        sql, _ = self._results[QueryExecutionId]
        if self.fail_on and self.fail_on in sql:
            return {"QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "boom"}}}
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_paginator(self, name):
        assert name == "get_query_results"
        outer = self

        class P:
            def paginate(self, QueryExecutionId):
                _, rows = outer._results[QueryExecutionId]
                if not rows:
                    return [{"ResultSet": {"Rows": []}}]
                header = list(rows[0].keys())
                data = [{"Data": [{"VarCharValue": h} for h in header]}]
                for r in rows:
                    data.append({"Data": [{"VarCharValue": r[h]} for h in header]})
                return [{"ResultSet": {"Rows": data}}]

        return P()


class FakeS3:
    def __init__(self, keys_by_prefix=None, delete_errors=None):
        self.keys_by_prefix = keys_by_prefix or {}
        self.deleted = []
        self.delete_errors = delete_errors or []

    def get_paginator(self, name):
        outer = self

        class P:
            def paginate(self, Bucket, Prefix):
                keys = outer.keys_by_prefix.get(Prefix, [])
                return [{"Contents": [{"Key": k} for k in keys]}] if keys else [{}]

        return P()

    def delete_objects(self, Bucket, Delete):
        self.deleted.extend(o["Key"] for o in Delete["Objects"])
        return {"Errors": self.delete_errors}


def log_row(key, ts, ip, status="200"):
    return {"key": key, "requestdatetime": ts, "remoteip": ip, "httpstatus": status}


class HelperTests(unittest.TestCase):
    def test_rebuild_dates_are_seven_complete_days_before_today(self):
        dates = handler.rebuild_dates(date(2026, 9, 20))
        self.assertEqual(dates[0], "2026-09-13")
        self.assertEqual(dates[-1], "2026-09-19")
        self.assertEqual(len(dates), 7)

    def test_parse_log_ts_converts_offset_to_utc(self):
        ts = handler.parse_log_ts("08/Sep/2026:12:05:55 +0000")
        self.assertEqual(ts, datetime(2026, 9, 8, 12, 5, 55, tzinfo=timezone.utc))
        ts = handler.parse_log_ts("08/Sep/2026:05:05:55 -0700")
        self.assertEqual(ts, datetime(2026, 9, 8, 12, 5, 55, tzinfo=timezone.utc))

    def test_sql_quote_escapes_single_quotes(self):
        self.assertEqual(handler.sql_quote("a'b"), "'a''b'")

    def test_filename_from_url(self):
        url = "https://ohmg-pub.s3.amazonaws.com/static/voip-download/X12345678_0.wav"
        self.assertEqual(handler.filename_from_url(url), "X12345678_0.wav")

    def test_insert_sql_keeps_percent_format_strings_and_oldest_date(self):
        sql = handler.build_insert_sql("2026-09-13")
        self.assertIn("date_parse('2026-09-13', '%Y-%m-%d')", sql)
        self.assertIn("'%d/%b/%Y:%H:%i:%s +0000'", sql)
        self.assertIn("AS log_date", sql)


class Part1Tests(unittest.TestCase):
    def setUp(self):
        self.report = handler.RunReport(dry_run=False)
        self.today = date(2026, 9, 20)
        prefix = handler.PARQUET_PREFIX
        self.s3 = FakeS3({f"{prefix}log_date=2026-09-19/": ["a", "b"]})
        self.glue = mock.Mock()
        self.glue.batch_delete_partition.return_value = {"Errors": []}
        counts = [{"log_date": d, "n": "10"} for d in handler.rebuild_dates(self.today)]
        self.athena = FakeAthena({"GROUP BY log_date": counts})
        p = mock.patch.multiple(handler, _s3=self.s3, _glue=self.glue, _athena=self.athena)
        p.start()
        self.addCleanup(p.stop)

    def test_happy_path_deletes_drops_inserts_and_checks(self):
        handler.part1_refresh(self.report, self.today)
        self.assertEqual(self.s3.deleted, ["a", "b"])
        args = self.glue.batch_delete_partition.call_args.kwargs
        self.assertEqual(len(args["PartitionsToDelete"]), 7)
        self.assertTrue(self.athena.queries[0].startswith("INSERT INTO"))
        self.assertEqual(self.report.failures, [])

    def test_entity_not_found_is_ignored(self):
        self.glue.batch_delete_partition.return_value = {
            "Errors": [{"PartitionValues": ["2026-09-13"], "ErrorDetail": {"ErrorCode": "EntityNotFoundException"}}]
        }
        handler.part1_refresh(self.report, self.today)
        self.assertEqual(self.report.failures, [])

    def test_other_glue_error_blocks_insert(self):
        self.glue.batch_delete_partition.return_value = {
            "Errors": [{"PartitionValues": ["2026-09-13"], "ErrorDetail": {"ErrorCode": "AccessDeniedException"}}]
        }
        handler.part1_refresh(self.report, self.today)
        self.assertEqual(len(self.report.failures), 1)
        self.assertEqual(self.athena.queries, [])

    def test_s3_delete_error_blocks_insert(self):
        self.s3.delete_errors = [{"Key": "a", "Code": "AccessDenied", "Message": "no"}]
        handler.part1_refresh(self.report, self.today)
        self.assertTrue(self.report.failures)
        self.assertEqual(self.athena.queries, [])

    def test_missing_date_is_a_failure(self):
        counts = [{"log_date": d, "n": "10"} for d in handler.rebuild_dates(self.today)[:-1]]
        self.athena.results_by_match = {"GROUP BY log_date": counts}
        handler.part1_refresh(self.report, self.today)
        self.assertEqual(len(self.report.failures), 1)
        self.assertIn("2026-09-19", self.report.failures[0])

    def test_dry_run_changes_nothing(self):
        report = handler.RunReport(dry_run=True)
        handler.part1_refresh(report, self.today)
        self.assertEqual(self.s3.deleted, [])
        self.glue.batch_delete_partition.assert_not_called()
        self.assertEqual(self.athena.queries, [])


class Part2Tests(unittest.TestCase):
    PRODS = [
        {"prod_id": "X1", "url": "https://ohmg-pub.s3.amazonaws.com/static/voip-download/X1_0.wav"},
        {"prod_id": "X2", "url": "https://ohmg-pub.s3.amazonaws.com/static/voip-download/X2_0.wav"},
        {"prod_id": "X3", "url": "https://ohmg-pub.s3.amazonaws.com/static/voip-download/X3_0.wav"},
        {"prod_id": "BAD", "url": "https://ohmg-pub.s3.amazonaws.com/static/voip-download/x'; drop.wav"},
    ]

    def setUp(self):
        self.calls = []
        rows = [
            # X1: excluded IP earlier, real download later -> real one wins
            log_row("static/voip-download/X1_0.wav", "01/Sep/2026:01:00:00 +0000", "143.110.210.93"),
            log_row("static/voip-download/X1_0.wav", "03/Sep/2026:10:00:00 +0000", "1.2.3.4"),
            log_row("static/voip-download/X1_0.wav", "02/Sep/2026:10:00:00 +0000", "5.6.7.8"),
            # X2: only excluded IP -> nothing written, flagged
            log_row("static/voip-download/X2_0.wav", "01/Sep/2026:01:00:00 +0000", "143.110.210.93"),
        ]
        self.athena = FakeAthena({"REST.GET.OBJECT": rows})
        patches = [
            mock.patch.multiple(handler, _athena=self.athena),
            mock.patch.object(handler, "get_token", return_value="tok"),
            mock.patch.object(handler, "api_call", side_effect=self.fake_api),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.put_status = 200

    def fake_api(self, method, path, token, body=None, retries=3):
        self.calls.append((method, path, body))
        if path == "/api/voip-list/":
            return 200, self.PRODS
        if path.startswith("/api/voip-lookup/"):
            return 200, {"id": 900 + int(path.split("X")[1].strip("/"))}
        if path.startswith("/api/voip-update/"):
            return self.put_status, {}
        raise AssertionError(path)

    def test_writes_earliest_non_excluded_timestamp_using_internal_id(self):
        report = handler.RunReport(dry_run=False)
        handler.part2_reconcile(report)
        puts = [c for c in self.calls if c[0] == "PUT"]
        self.assertEqual(puts, [("PUT", "/api/voip-update/901/", {"downloaded_date": "2026-09-02T10:00:00Z"})])
        self.assertEqual(report.failures, [])

    def test_excluded_only_and_bad_filename_are_warnings_not_writes(self):
        report = handler.RunReport(dry_run=False)
        handler.part2_reconcile(report)
        joined = " ".join(report.warnings)
        self.assertIn("X2", joined)
        self.assertIn("unexpected filename", joined)
        self.assertNotIn("X2_0.wav", [c[1] for c in self.calls if c[0] == "PUT"])
        self.assertEqual(report.summary["part2"]["excluded_ip_only"], 1)

    def test_bad_filename_never_reaches_sql(self):
        handler.part2_reconcile(handler.RunReport(dry_run=False))
        self.assertTrue(all("drop" not in q for q in self.athena.queries))

    def test_put_failure_is_a_hard_failure(self):
        self.put_status = 500
        report = handler.RunReport(dry_run=False)
        handler.part2_reconcile(report)
        self.assertEqual(len(report.failures), 1)

    def test_dry_run_does_not_put(self):
        report = handler.RunReport(dry_run=True)
        handler.part2_reconcile(report)
        self.assertFalse([c for c in self.calls if c[0] == "PUT"])
        self.assertEqual(report.summary["part2"]["written"], 1)

    def test_lookup_404_is_a_warning(self):
        orig = self.fake_api

        def api(method, path, token, body=None, retries=3):
            if path.startswith("/api/voip-lookup/"):
                return 404, None
            return orig(method, path, token, body, retries)

        with mock.patch.object(handler, "api_call", side_effect=api):
            report = handler.RunReport(dry_run=False)
            handler.part2_reconcile(report)
        self.assertEqual(report.failures, [])
        self.assertTrue(any("voip-lookup" in w for w in report.warnings))


class HandlerTests(unittest.TestCase):
    def test_failures_raise_after_both_parts_run(self):
        calls = []

        def p1(report, today=None):
            calls.append("p1")
            report.fail("rebuild broke")

        def p2(report):
            calls.append("p2")

        with mock.patch.object(handler, "part1_refresh", side_effect=p1), \
                mock.patch.object(handler, "part2_reconcile", side_effect=p2):
            with self.assertRaises(RuntimeError) as ctx:
                handler.lambda_handler({}, None)
        self.assertEqual(calls, ["p1", "p2"])
        self.assertIn("rebuild broke", str(ctx.exception))

    def test_clean_run_returns_summary_and_skip_part1_works(self):
        with mock.patch.object(handler, "part1_refresh") as p1, \
                mock.patch.object(handler, "part2_reconcile"):
            result = handler.lambda_handler({"skip_part1": True}, None)
        p1.assert_not_called()
        self.assertEqual(result["failures"], [])


if __name__ == "__main__":
    unittest.main()
