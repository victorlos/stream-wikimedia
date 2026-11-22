"""Deploy or cancel one Flink SQL job in the local Docker stack.

    python flink/deploy_job.py flink/sql/jobs/clean_events.sql           deploy
    python flink/deploy_job.py --cancel flink/sql/jobs/clean_events.sql  cancel

Run from the project root. Terraform calls it (terraform/flink_jobs.tf), but
it works on its own too.

Deploying is safe to repeat: a running job with the same name is cancelled
first, so there is never more than one copy of a job.

The job name comes from the file's  SET 'pipeline.name' = '...';  line.
Standard library only, so it runs with any Python 3.10+.
"""

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

FLINK_URL = "http://localhost:8081"
JOBMANAGER_CONTAINER = "flink-jobmanager"  # container_name in docker-compose.yml
CONTAINER_SQL_DIR = "/opt/flink/sql"  # where ./flink/sql is mounted
HOST_SQL_DIR = Path("flink/sql")
ACTIVE_STATES = {"CREATED", "INITIALIZING", "RUNNING", "RESTARTING", "RECONCILING", "CANCELLING"}
READY_TIMEOUT = 180  # seconds to wait for Flink after docker compose up
JOB_TIMEOUT = 60  # seconds to wait for a job to start or stop

def log(message):
    print(f"[deploy_job] {message}", flush=True)


def api(method, path):
    request = urllib.request.Request(FLINK_URL + path, method=method)
    with urllib.request.urlopen(request, timeout=10) as response:
        body = response.read()
        return json.loads(body) if body else {}


def job_name(sql_file):
    match = re.search(r"SET\s+'pipeline\.name'\s*=\s*'([^']+)'", sql_file.read_text(), re.IGNORECASE)
    if not match:
        sys.exit(f"{sql_file} has no  SET 'pipeline.name' = '...';  line. Add one so the job can be found.")
    return match.group(1)


def wait_for_flink():
    """Wait until the job manager answers and a task manager has a free slot."""
    deadline = time.monotonic() + READY_TIMEOUT
    while True:
        try:
            overview = api("GET", "/overview")
            if overview.get("taskmanagers", 0) >= 1 and overview.get("slots-available", 0) >= 1:
                return
            reason = "no free task slot yet"
        except (urllib.error.URLError, OSError) as exc:
            reason = f"not reachable ({exc})"
        if time.monotonic() > deadline:
            sys.exit(f"Flink at {FLINK_URL} is not ready after {READY_TIMEOUT}s: {reason}")
        log(f"Waiting for Flink: {reason}")
        time.sleep(5)


def active_jobs(name):
    jobs = api("GET", "/jobs/overview").get("jobs", [])
    return [job for job in jobs if job["name"] == name and job["state"] in ACTIVE_STATES]


def cancel(name):
    for job in active_jobs(name):
        log(f"Cancelling {name} ({job['jid']})")
        api("PATCH", f"/jobs/{job['jid']}?mode=cancel")
    deadline = time.monotonic() + JOB_TIMEOUT
    while active_jobs(name):
        if time.monotonic() > deadline:
            sys.exit(f"{name} did not stop within {JOB_TIMEOUT}s")
        time.sleep(2)


def submit(sql_file, name):
    container_path = f"{CONTAINER_SQL_DIR}/{sql_file.resolve().relative_to(HOST_SQL_DIR.resolve()).as_posix()}"
    log(f"Submitting {name} from {container_path}")
    result = subprocess.run(
        ["docker", "exec", JOBMANAGER_CONTAINER, "./bin/sql-client.sh", "-f", container_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or "[ERROR]" in result.stdout:
        print(result.stdout, result.stderr, sep="\n", file=sys.stderr)
        sys.exit(f"The SQL client could not submit {name}")

    deadline = time.monotonic() + JOB_TIMEOUT
    while True:
        running = [job for job in active_jobs(name) if job["state"] == "RUNNING"]
        if running:
            log(f"{name} is running ({running[0]['jid']}). See {FLINK_URL}")
            return
        if time.monotonic() > deadline:
            sys.exit(f"{name} was submitted but is not RUNNING after {JOB_TIMEOUT}s. Check {FLINK_URL}")
        time.sleep(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sql_file", type=Path)
    parser.add_argument("--cancel", action="store_true", help="cancel the job instead of deploying it")
    args = parser.parse_args()

    if not args.sql_file.is_file():
        sys.exit(f"{args.sql_file} not found. Run from the project root.")
    name = job_name(args.sql_file)

    if args.cancel:
        try:
            cancel(name)
        except (urllib.error.URLError, OSError):
            log(f"Flink is not reachable, so {name} is not running")
        return

    wait_for_flink()
    cancel(name)
    submit(args.sql_file, name)


if __name__ == "__main__":
    main()