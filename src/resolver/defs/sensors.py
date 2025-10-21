# sensors.py
import glob, hashlib, os
from dagster import sensor, RunRequest, SkipReason, DefaultSensorStatus


def _digest_paths(paths: list[str]) -> str:
  """Stable digest over file names + sizes + mtimes + bytes (cheap-ish)."""
  h = hashlib.sha256()
  for p in sorted(paths):
    if not os.path.exists(p):
      continue
    st = os.stat(p)
    h.update(p.encode())  # path
    h.update(str(st.st_size).encode())  # size
    h.update(str(int(st.st_mtime)).encode())  # mtime (sec precision)
    # if files are small, include bytes; comment out if huge
    with open(p, "rb") as f:
      h.update(f.read(1024 * 1024))  # first 1MB
  return h.hexdigest()


@sensor(
  name="source_companies_changed",
  minimum_interval_seconds=30,
  default_status=DefaultSensorStatus.STOPPED,
  job_name="er_pipeline_job",
)
def source_companies_changed_sensor(context):
  paths = [os.getenv("COMPANY_FILE")]
  print(f"Context is {type(context)}")

  if not paths:
    return SkipReason("No source files found.")

  digest = _digest_paths(paths)

  # Use the cursor to avoid duplicate runs
  if context.cursor == digest:
    return SkipReason("No change detected.")
  context.update_cursor(digest)

  # You can pass run_config here if your job/asset expects any config
  return RunRequest(
    run_key=digest,  # idempotent
    run_config={},  # or provide config as needed
    tags={
      "source_digest": digest,
      "source_count": str(len(paths))
    },
  )
