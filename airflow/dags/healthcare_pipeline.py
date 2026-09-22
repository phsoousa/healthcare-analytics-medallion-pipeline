"""
============================================================================
 HEALTHCARE PIPELINE DAG  —  the Airflow schedule that RUNS dbt
============================================================================
WHAT THIS FILE DOES (in plain English):
  After the gatekeeper loads clean data into RAW (Bronze), this DAG runs dbt
  to transform it into the Silver and Gold layers. Its four tasks run in order:

     dbt_run        -> build the Silver (STAGING) and Gold (MARTS) tables
     dbt_snapshot   -> capture the SCD2 doctor history
     dbt_test       -> run the data-quality tests
     reconciliation -> a final "pipeline complete" marker

  This DAG is normally TRIGGERED by the gatekeeper DAG right after a load,
  and also runs on its own 10-minute schedule as a backup.

ROOT-CAUSE ALERTING:
  Each dbt task writes its output to a log file. If a task fails, the failure
  handler reads that log, finds the ACTUAL failing model (e.g.
  STAGING.stg_admissions), and puts that real cause into both the alert email
  and the PIPELINE_ERROR_LOG row - so you see exactly what broke, not a
  generic message.
============================================================================
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Paths inside the Airflow container (mapped from your project by docker-compose)
PROJECT = "/opt/airflow/project"
PROFILES_DIR = "/opt/airflow/config"     # where dbt finds profiles.yml
ALERT_EMAIL = "ph.sousa92@gmail.com"

# Each dbt task writes its console output here so the failure handler can read it.
LOG_DIR = "/tmp/dbt_task_logs"


def _snowflake_session():
    """Open a Snowflake connection (key-pair auth) for the failure handler to
       send the alert email and write to the error-log table."""
    import os
    from snowflake.snowpark import Session
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    k = open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb").read()
    pk = serialization.load_pem_private_key(k, password=None, backend=default_backend())
    pkb = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    return Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"], "user": os.environ["SNOWFLAKE_USER"],
        "private_key": pkb, "role": "ACCOUNTADMIN", "warehouse": "HEALTHCARE_WH",
        "database": "HEALTHCARE_DB", "schema": "AUDIT"}).create()


def extract_dbt_error(task_id):
    """Read the failed task's dbt log and pull out the REAL error line, e.g.
       'Model failed to build: STAGING.stg_admissions'. Looks for four common
       dbt error patterns. If none match (or there's no log), returns a
       sensible generic message instead."""
    import os, re
    log_path = os.path.join(LOG_DIR, f"{task_id}.log")
    # Fallback messages if we can't find a specific cause in the log.
    generic = {
        "dbt_run":        "dbt failed to build the Silver/Gold models (broken SQL or model error).",
        "dbt_snapshot":   "dbt failed to capture SCD2 snapshot history.",
        "dbt_test":       "One or more dbt data-quality tests failed.",
        "reconciliation": "The reconciliation step failed.",
    }.get(task_id, "A pipeline task failed.")

    if not os.path.exists(log_path):
        return generic

    try:
        with open(log_path, "r", errors="ignore") as f:
            text = f.read()
    except Exception:
        return generic

    # Remove the colour codes dbt prints to the console (e.g. the red \x1b[31m).
    clean = re.sub(r"\x1b\[[0-9;]*m", "", text)
    lines = clean.splitlines()

    hits = []

    # Pattern 1 - a model failed to build: "ERROR creating ... model STAGING.stg_admissions"
    for ln in lines:
        m = re.search(r"ERROR creating.*model\s+([A-Za-z0-9_.]+)", ln)
        if m:
            hits.append("Model failed to build: " + m.group(1))

    # Pattern 2 - a compilation error in a model/test/snapshot
    for ln in lines:
        m = re.search(r"Compilation Error in (model|test|snapshot)\s+([A-Za-z0-9_.]+)", ln)
        if m:
            hits.append("Compilation error in " + m.group(1) + ": " + m.group(2))

    # Pattern 3 - a database error in a model/test/snapshot
    for ln in lines:
        m = re.search(r"Database Error in (model|test|snapshot)\s+([A-Za-z0-9_.]+)", ln)
        if m:
            hits.append("Database error in " + m.group(1) + ": " + m.group(2))

    # Pattern 4 - a failing data-quality test
    for ln in lines:
        m = re.search(r"Failure in test\s+([A-Za-z0-9_.]+)", ln)
        if m:
            hits.append("Test failed: " + m.group(1))

    if not hits:
        return generic

    # Remove duplicates (keep order) and keep the message tidy.
    seen, unique = set(), []
    for h in hits:
        if h not in seen:
            seen.add(h)
            unique.append(h)
    return "  ".join(unique[:5])


def send_failure_email(context):
    """Runs automatically when any task in this DAG fails. It finds the real
       cause, emails an alert, and writes a matching row to PIPELINE_ERROR_LOG
       (so the table mirrors the email, including the action to take)."""
    import datetime
    ti = context["task_instance"]
    task_id = ti.task_id
    dag_id = ti.dag_id
    try_no = ti.try_number
    max_tries = ti.max_tries + 1
    run_type = str(context["dag_run"].run_type).split(".")[-1].lower()
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Find the REAL failing model/test from the task's dbt log.
    root_cause = extract_dbt_error(task_id)

    action_text = ("Open Airflow > " + dag_id + " > " + task_id + " > Logs for the full "
                   "stack trace, fix the issue, then re-run the pipeline.")

    subject = "Airflow Pipeline FAILED - " + task_id
    body = (
        "Healthcare AIRFLOW pipeline FAILED - transform halted.\n\n"
        "  DAG:            " + dag_id + "\n"
        "  Failed task:    " + task_id + "\n"
        "  Status:         FAILED (downstream steps skipped)\n"
        "  Triggered by:   " + run_type + "\n"
        "  Attempt:        " + str(try_no) + " of " + str(max_tries) + "\n"
        "  Failed at:      " + ts + "\n\n"
        "  Root cause:\n"
        "    - " + root_cause + "\n\n"
        "  Action: " + action_text
    )
    safe_subject = subject.replace("'", "")
    safe_body = body.replace("'", "")
    try:
        s = _snowflake_session()
        s.sql("call system$send_email(?, ?, ?, ?)",
              params=["HEALTHCARE_EMAIL_INT", ALERT_EMAIL, safe_subject, safe_body]).collect()
        # Save the same details (real cause + action) to the error-log table.
        s.sql(
            "INSERT INTO HEALTHCARE_DB.AUDIT.PIPELINE_ERROR_LOG "
            "(dag_id, task_id, status, triggered_by, attempt, error_summary, action, failed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
            params=[dag_id, task_id, "FAILED", run_type,
                    str(try_no) + " of " + str(max_tries), root_cause, action_text]).collect()
        s.close()
    except Exception as e:
        print("failure email/log could not be written: " + str(e))


# Default settings applied to every task in this DAG.
default_args = {
    "owner": "priyanka",
    "retries": 1,                               # retry once before failing
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_failure_email,   # call the handler above on failure
}


def dbt_cmd(task_id, dbt_sub):
    """Build the shell command for a dbt task. It runs the dbt command AND tees
       (copies) its output to a log file so the failure handler can read the real
       error. 'set -o pipefail' makes sure the task still FAILS if dbt fails."""
    return (
        "mkdir -p " + LOG_DIR + " && set -o pipefail && "
        "cd " + PROJECT + " && "
        "dbt " + dbt_sub + " --profiles-dir " + PROFILES_DIR + " --project-dir " + PROJECT + " "
        "2>&1 | tee " + LOG_DIR + "/" + task_id + ".log"
    )


# Define the DAG (the workflow) and its four tasks.
with DAG(
    dag_id="healthcare_pipeline",
    description="Transform pipeline: builds Silver and Gold from validated Bronze data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=10),   # backup schedule; usually triggered by the gatekeeper
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "dbt", "snowflake"],
) as dag:

    # TASK 1: build the Silver (STAGING) and Gold (MARTS) tables.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=dbt_cmd("dbt_run", "run"),
    )
    # TASK 2: capture the SCD2 doctor-history snapshot.
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt_cmd("dbt_snapshot", "snapshot"),
    )
    # TASK 3: run all the data-quality tests.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd("dbt_test", "test"),
    )
    # TASK 4: a simple "pipeline complete" marker at the end.
    reconciliation = BashOperator(
        task_id="reconciliation",
        bash_command="cd " + PROJECT + " && echo 'Pipeline complete - see AUDIT logs'",
    )

    # ORDER: run -> snapshot -> test -> reconciliation.
    # Each task only runs if the one before it succeeded.
    dbt_run >> dbt_snapshot >> dbt_test >> reconciliation