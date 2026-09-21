# Parquet refresh + VoIP writeback (Lambda)

Replaces the Cowork scheduled task "Refresh Parquet table + VoIP download writeback".
Runs daily at 08:00 UTC (1:00 AM Arizona), the same time as the old task.

**Part 1** deletes and rebuilds the trailing 7 complete UTC days of
`ohmg_pub_logs.s3_access_logs_parquet` from the raw `s3_access_logs` table.
**Part 2** reads the production list from the VoIP API, finds each file's earliest
genuine download in the rebuilt table (ignoring `143.110.210.93`), and PUTs it back
as `downloaded_date`.

If anything fails hard (S3 or Glue cleanup error, Athena failure, a date with zero
rows after the rebuild, a failed API PUT), the invocation errors and the CloudWatch
alarm emails you. Softer cases (a 404 on lookup, a download seen only from the
excluded IP) are logged as warnings and do not alert.

## Files

- `src/handler.py` the Lambda function (Python 3.12, no third-party dependencies)
- `template.yaml` SAM template: function, least-privilege IAM role, schedule, log group, SNS topic, alarm
- `tests/test_handler.py` unit tests with stubbed AWS and API clients

## Deploy

1. **Store the API token in SSM** (a SecureString, never in the template or code):

   ```bash
   aws ssm put-parameter --region us-east-2 --name /ohmg/voip-api/token \
     --type SecureString --value '<token>'
   ```

   The current token sits in plain text in the Cowork task prompt, so consider
   rotating it once the Lambda is live and removing it from that prompt.

2. **Build and deploy** (the schedule starts disabled):

   ```bash
   sam build
   sam deploy --guided --region us-east-2 --stack-name ohmg-parquet-refresh
   ```

   Confirm the SNS subscription email sent to `AlertEmail`, or alarms will not reach you.

3. **Dry run** (read-only: no deletes, no INSERT, no PUTs):

   ```bash
   aws lambda invoke --region us-east-2 \
     --function-name ohmg-parquet-refresh-voip-writeback \
     --cli-binary-format raw-in-base64-out \
     --payload '{"dry_run": true}' out.json && cat out.json
   ```

   Check that `part2.downloaded` looks right and `failures` is empty. This exercises
   the token, the API, Athena reads, and the SELECT-side permissions. The write-side
   permissions (S3 delete, Glue partitions, INSERT) are exercised by the first live run.

## Cutover

Do these in order so the Cowork task and the Lambda never rebuild the same partitions at once.

1. Disable the Cowork task (next run is 08:00 UTC).
2. Run the Lambda once for real: `--payload '{}'`. It is safe to re-run if it fails,
   since every run drops and rebuilds the same 7 days.
3. Enable the schedule: `sam deploy --parameter-overrides ScheduleState=ENABLED`
   (keep your other parameters).

Rollback is the reverse: set `ScheduleState=DISABLED` and re-enable the Cowork task.

## Configuration

| Setting | Where | Default |
| --- | --- | --- |
| API base URL | `VoipApiBase` parameter | `https://test.onholdmediagroup.com` |
| Token location | `VoipTokenParam` parameter | `/ohmg/voip-api/token` |
| Alert email | `AlertEmail` parameter | `david@onholdwizard.com` |
| Excluded download IPs | `EXCLUDED_IPS` env var (comma separated) | `143.110.210.93` |
| Days rebuilt | `REBUILD_DAYS` env var | `7` |

Event flags: `{"dry_run": true}` for a read-only run, `{"skip_part1": true}` to run only the reconciliation.

## Differences from the Cowork version

- If the S3 or Glue cleanup reports an error, the INSERT is skipped instead of run, because
  inserting on top of a partly cleaned prefix would duplicate rows.
- File names from the API must match `[A-Za-z0-9._-]+` before they are placed in the SQL query;
  anything else is skipped with a warning.
- API calls retry up to 3 times on 5xx or network errors.
- Failures raise an alarm instead of only appearing in run output.

## Run the tests

```bash
pip install boto3
python -m unittest discover -s tests -v
```
