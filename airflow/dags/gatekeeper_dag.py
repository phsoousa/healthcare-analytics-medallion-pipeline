"""
============================================================================
 GATEKEEPER DAG  —  the Airflow schedule that RUNS the gatekeeper
============================================================================
WHAT THIS FILE DOES:
  This is an Airflow DAG (a scheduled workflow). It does two things, in order:

     Task 1  gatekeeper_validate       -> runs snowpark/gatekeeper.py
                                          (validate files, load good ones,
                                           quarantine bad ones)
     Task 2  trigger_healthcare_pipeline -> starts the transform pipeline
                                          (so the new data becomes Gold right away)

  This file does NOT do the validation itself — it just RUNS the gatekeeper
  script and then triggers the next pipeline.

TWO KINDS OF GATEKEEPER EMAILS (don't confuse them):
  1) QUARANTINE email  - sent by gatekeeper.py when a FILE fails the 12 checks.
                         This is normal operation (bad data was caught).
  2) DAG-FAILURE email - sent by THIS file (below) only when the gatekeeper
                         TASK ITSELF crashes (e.g. Snowflake unreachable, bad
                         key, code error) - a real system problem, not a
                         quarantine.
============================================================================
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Paths inside the Airflow container (mapped from your project by docker-compose)
PROJECT = "/opt/airflow/project"
ALERT_EMAIL = "priyankapandey000111@gmail.com"   # who receives DAG-crash alerts


def _snowflake_session():
    """Open a Snowflake connection (key-pair auth) so the failure handler below
       can send an alert email and write a row to the error log."""
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


def send_failure_email(context):
    """Runs ONLY when a task in this DAG CRASHES (Airflow calls this automatically
       via on_failure_callback below). It emails an alert AND writes a matching row
       to PIPELINE_ERROR_LOG. Note: a file quarantine is NOT a crash, so it does
       not trigger this - quarantines are handled inside gatekeeper.py instead."""
    import datetime as _dt
    ti = context["task_instance"]
    task_id = ti.task_id
    dag_id = ti.dag_id
    try_no = ti.try_number
    max_tries = ti.max_tries + 1
    run_type = str(context["dag_run"].run_type).split(".")[-1].lower()
    ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # A plain-English explanation of what each task failing means.
    stage_meaning = {
        "gatekeeper_validate":
            "The gatekeeper worker itself failed to run (e.g. Snowflake unreachable, "
            "bad credentials, or a script error). This is NOT a file quarantine - "
            "if a file had failed the 12 checks you'd get a QUARANTINE email instead.",
        "trigger_healthcare_pipeline":
            "Failed to trigger the downstream healthcare_pipeline transform DAG.",
    }.get(task_id, "A gatekeeper task failed.")

    action_text = (f"Open Airflow > {dag_id} > {task_id} > Logs for the full error, "
                   f"fix the issue, then re-run.")

    subject = f"Gatekeeper Pipeline FAILED - {task_id}"
    body = (
        f"Healthcare GATEKEEPER pipeline FAILED - ingestion halted.\n\n"
        f"  DAG:            {dag_id}\n"
        f"  Failed task:    {task_id}\n"
        f"  Status:         FAILED (downstream steps skipped)\n"
        f"  Triggered by:   {run_type}\n"
        f"  Attempt:        {try_no} of {max_tries}\n"
        f"  Failed at:      {ts}\n\n"
        f"  What happened:\n"
        f"    - {stage_meaning}\n\n"
        f"  Action: {action_text}"
    )
    safe_subject = subject.replace("'", "")
    safe_body = body.replace("'", "")
    try:
        s = _snowflake_session()
        # 1) send the alert email
        s.sql("call system$send_email(?, ?, ?, ?)",
              params=["HEALTHCARE_EMAIL_INT", ALERT_EMAIL, safe_subject, safe_body]).collect()
        # 2) write the same details to the permanent error-log table
        s.sql(
            "INSERT INTO HEALTHCARE_DB.AUDIT.PIPELINE_ERROR_LOG "
            "(dag_id, task_id, status, triggered_by, attempt, error_summary, action, failed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
            params=[dag_id, task_id, "FAILED", run_type,
                    f"{try_no} of {max_tries}", stage_meaning, action_text]).collect()
        s.close()
    except Exception as e:
        print(f"failure email/log could not be written: {e}")


# Default settings applied to every task in this DAG.
default_args = {
    "owner": "priyanka",
    "retries": 1,                              # retry once before failing
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_failure_email,  # call the handler above on a crash
}

# Define the DAG (the workflow) and its two tasks.
with DAG(
    dag_id="gatekeeper_pipeline",
    description="Validate-before-load gatekeeper for incoming files",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=10),   # also runs automatically every 10 minutes
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "data-quality", "gatekeeper"],
) as dag:

    # TASK 1: run the gatekeeper script (validate + load + quarantine).
    gatekeeper = BashOperator(
        task_id="gatekeeper_validate",
        bash_command=f"cd {PROJECT} && python snowpark/gatekeeper.py",
    )

    # TASK 2: once the gatekeeper succeeds, start the transform pipeline so the
    # newly-loaded data is turned into Gold immediately (no waiting for its schedule).
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_healthcare_pipeline",
        trigger_dag_id="healthcare_pipeline",   # <-- the DAG this one triggers
        wait_for_completion=False,              # fire-and-forget; it runs on its own
        reset_dag_run=True,
    )

    # ORDER: run the gatekeeper first, THEN trigger the pipeline.
    # (If Task 1 crashes, Task 2 is skipped and the pipeline is not triggered.)
    gatekeeper >> trigger_dbt