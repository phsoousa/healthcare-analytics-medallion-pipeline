"""
============================================================================
 GATEKEEPER  —  the data-quality "front door" for the pipeline
============================================================================
WHAT THIS FILE DOES:
  Every data file (patient_admissions, treatment_records, insurance_claims)
  is uploaded to an S3 "incoming" folder. This script checks each file
  BEFORE it is allowed into the database:

     read the file's columns  ->  run 12 quality checks
        PASS  ->  load the data into the RAW table  ->  archive the file to "processed"
        FAIL  ->  move the file to "quarantine"  +  send an alert email  +  log it

  So good data gets in, bad data is stopped at the door. This is the ONLY
  path data takes into the warehouse.
============================================================================
"""
import os
import uuid
import datetime
from dataclasses import dataclass
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


# ---------------------------------------------------------------------------
# COMMON SETTINGS — shared by every data feed
# These point at the S3 stages (folders), the CSV formats, and the thresholds
# used by the quality checks.
# ---------------------------------------------------------------------------
COMMON = {
    "incoming_stage":    "HEALTHCARE_DB.RAW.GK_INCOMING",    # where new files arrive
    "processed_stage":   "HEALTHCARE_DB.RAW.GK_PROCESSED",   # archive for good files
    "quarantine_stage":  "HEALTHCARE_DB.RAW.GK_QUARANTINE",  # holding pen for bad files
    "file_format":       "HEALTHCARE_DB.RAW.HEALTHCARE_CSV",           # skips header, loads data
    "file_format_nohdr": "HEALTHCARE_DB.RAW.HEALTHCARE_CSV_NOHEADER",  # reads header AS data
    "email_integration": "HEALTHCARE_EMAIL_INT",             # used to send alert emails
    "min_rows":          1,           # a file must have at least this many rows
    "max_null_pct":      5.0,         # a required column may be at most 5% empty
    "date_min":          "2020-01-01",  # dates must fall in this range
    "date_max":          "2030-12-31",
}

# ---------------------------------------------------------------------------
# PER-FEED SETTINGS
# Each of the 3 data types has its own rules: which table to load, which
# columns are expected/required, the primary key, which columns must be
# numeric, the date column, and the valid category values.
# "file_pattern" is how we recognise which feed a file belongs to (by its name).
# ---------------------------------------------------------------------------
CONFIGS = {
    "patient_admissions": {
        "file_pattern":     "patient_admissions",
        "target_table":     "HEALTHCARE_DB.RAW.PATIENT_ADMISSIONS",
        "expected_columns": ["admission_id", "patient_id", "doctor_id", "hospital_id",
                             "admit_date", "department", "admission_type",
                             "diagnosis_code", "length_of_stay", "readmission_flag"],
        "required_columns": ["admission_id", "patient_id", "doctor_id", "hospital_id"],
        "pk_column":        "admission_id",   # this column must be unique
        "numeric_columns":  ["admission_id", "length_of_stay"],
        "date_column":      "admit_date",
        "category_column":  "admission_type",
        "valid_categories": ["EMG", "URG", "ELC"],   # only these values are allowed
        "load_columns":     "admission_id, patient_id, doctor_id, hospital_id, admit_date, "
                            "department, admission_type, diagnosis_code, length_of_stay, "
                            "readmission_flag, file_name, upload_dttm, load_dttm",
        "load_select":      "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10",   # file columns 1..10
    },
    "treatment_records": {
        "file_pattern":     "treatment_records",
        "target_table":     "HEALTHCARE_DB.RAW.TREATMENT_RECORDS",
        "expected_columns": ["treatment_id", "admission_id", "doctor_id", "procedure_code",
                             "treatment_date", "cost", "outcome"],
        "required_columns": ["treatment_id", "admission_id", "doctor_id"],
        "pk_column":        "treatment_id",
        "numeric_columns":  ["treatment_id", "admission_id", "cost"],
        "date_column":      "treatment_date",
        "category_column":  "outcome",
        "valid_categories": ["P", "F", "S"],
        "load_columns":     "treatment_id, admission_id, doctor_id, procedure_code, "
                            "treatment_date, cost, outcome, file_name, upload_dttm, load_dttm",
        "load_select":      "$1,$2,$3,$4,$5,$6,$7",
    },
    "insurance_claims": {
        "file_pattern":     "insurance_claims",
        "target_table":     "HEALTHCARE_DB.RAW.INSURANCE_CLAIMS",
        "expected_columns": ["claim_id", "admission_id", "insurance_id", "claim_amount",
                             "approved_amount", "claim_status", "claim_date", "settle_date"],
        "required_columns": ["claim_id", "admission_id", "insurance_id"],
        "pk_column":        "claim_id",
        "numeric_columns":  ["claim_id", "admission_id", "claim_amount", "approved_amount"],
        "date_column":      "claim_date",
        "category_column":  "claim_status",
        "valid_categories": ["P", "A", "R"],
        "load_columns":     "claim_id, admission_id, insurance_id, claim_amount, "
                            "approved_amount, claim_status, claim_date, settle_date, "
                            "file_name, upload_dttm, load_dttm",
        "load_select":      "$1,$2,$3,$4,$5,$6,$7,$8",
    },
}


@dataclass
class DQResult:
    """One result from a single quality check: its name, tier, pass/fail, and detail."""
    check_name: str
    tier: str
    passed: bool
    detail: str


def get_session():
    """Open a secure connection to Snowflake using key-pair authentication.
       Reads the private key file and the account/user from environment variables
       (set by docker-compose) — no passwords are hard-coded."""
    key = open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb").read()
    pk = serialization.load_pem_private_key(key, password=None, backend=default_backend())
    pkb = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": pkb,
        "role": "ACCOUNTADMIN",
        "warehouse": "HEALTHCARE_WH",
        "database": "HEALTHCARE_DB",
        "schema": "RAW",
    }).create()


def detect_config(file_name):
    """Look at the file name and return the matching feed settings.
       e.g. a file called 'patient_admissions.csv' returns the admissions config."""
    for cfg in CONFIGS.values():
        if cfg["file_pattern"] in file_name.lower():
            return cfg
    return None


def list_incoming(session):
    """List every recognised .csv file currently sitting in the incoming folder."""
    rows = session.sql(f"LIST @{COMMON['incoming_stage']}").collect()
    files = []
    for r in rows:
        name = r["name"].split("/")[-1]
        if name.endswith(".csv") and detect_config(name) is not None:
            files.append(name)
    return files


def read_header(session, file_name, cfg):
    """Peek at the FIRST row of the file (the header) to learn its column names.
       Uses the NO-HEADER format so the header line is read as plain data.
       Returns a clean, lower-cased list of column names."""
    selects = ",".join([f"${i}" for i in range(1, len(cfg["expected_columns"]) + 2)])
    rows = session.sql(f"""
        SELECT {selects}
        FROM @{COMMON['incoming_stage']}/{file_name}
        (FILE_FORMAT => '{COMMON['file_format_nohdr']}')
        LIMIT 1
    """).collect()
    if not rows:
        return []
    return [str(v).strip().lower() for v in rows[0].as_dict().values()
            if v is not None and str(v).strip() != ""]


def load_to_temp(session, file_name, n_cols):
    """Load the whole file into a TEMPORARY table (all text columns) just so the
       quality checks can inspect it. Nothing official yet — this is a scratch copy.
       ON_ERROR = CONTINUE means even messy rows load, so the checks can see them."""
    cols_ddl = ", ".join([f"C{i} VARCHAR" for i in range(n_cols)])
    session.sql(f"CREATE OR REPLACE TEMP TABLE GK_TEMP_RAW ({cols_ddl})").collect()
    session.sql(f"""
        COPY INTO GK_TEMP_RAW
        FROM @{COMMON['incoming_stage']}/{file_name}
        FILE_FORMAT = (FORMAT_NAME = '{COMMON['file_format']}')
        ON_ERROR = CONTINUE
    """).collect()


def col_ref(header, name):
    """Find which temp-table column (C0, C1, C2 ...) holds a given named column."""
    name = name.lower()
    return f"C{header.index(name)}" if name in header else None


def run_checks(session, file_name, header, cfg):
    """Run the 12 quality checks on the temp copy of the file, grouped in 3 tiers:
         GATE (3)      - must pass or the file is rejected immediately
         THRESHOLD (5) - data-quality limits (nulls, types, duplicates, categories)
         ADVISORY (2)  - warnings only, do not block the load
       Returns a list of DQResult objects (one per check)."""
    results = []
    R = results.append

    # ---- GATE tier: structural checks. If any fail, stop here. ----
    size_rows = session.sql(f"LIST @{COMMON['incoming_stage']}/{file_name}").collect()
    size = size_rows[0]["size"] if size_rows else 0
    R(DQResult("file_not_empty", "GATE", size > 0, f"size={size} bytes"))   # 1. file has content

    R(DQResult("column_count", "GATE",                                      # 2. right number of columns
               len(header) == len(cfg["expected_columns"]),
               f"found {len(header)}, expected {len(cfg['expected_columns'])}"))

    missing = [c for c in cfg["required_columns"] if c.lower() not in header]
    R(DQResult("required_columns", "GATE", len(missing) == 0,               # 3. required columns present
               f"missing={missing}" if missing else "all present"))

    # If any GATE check failed, the file is unusable — return now (quarantine it).
    if any(r.tier == "GATE" and not r.passed for r in results):
        return results

    total = session.sql("SELECT COUNT(*) c FROM GK_TEMP_RAW").collect()[0]["C"]

    # ---- THRESHOLD tier: data-quality limits. ----
    R(DQResult("row_count", "THRESHOLD", total >= COMMON["min_rows"], f"rows={total}"))  # 4. has rows

    # 5. required columns must not be too empty (<= 5% null)
    null_details, null_ok = [], True
    for c in cfg["required_columns"]:
        ref = col_ref(header, c)
        n = session.sql(f"SELECT COUNT(*) c FROM GK_TEMP_RAW WHERE {ref} IS NULL").collect()[0]["C"]
        pct = (n / total * 100) if total else 0
        null_details.append(f"{c}={pct:.1f}%")
        if pct > COMMON["max_null_pct"]:
            null_ok = False
    R(DQResult("null_percentage", "THRESHOLD", null_ok, ", ".join(null_details)))

    # 6. numeric columns must actually contain numbers
    num_checks = " OR ".join(
        [f"TRY_CAST({col_ref(header, c)} AS NUMBER) IS NULL" for c in cfg["numeric_columns"]])
    bad_num = session.sql(f"SELECT COUNT(*) c FROM GK_TEMP_RAW WHERE {num_checks}").collect()[0]["C"]
    R(DQResult("data_types", "THRESHOLD", bad_num == 0, f"non-numeric rows={bad_num}"))

    # 7. the primary key must be unique (no duplicate IDs)
    pk = col_ref(header, cfg["pk_column"])
    dupe_pk = session.sql(
        f"SELECT COUNT(*) c FROM (SELECT {pk} FROM GK_TEMP_RAW "
        f"GROUP BY {pk} HAVING COUNT(*) > 1)").collect()[0]["C"]
    R(DQResult("pk_uniqueness", "THRESHOLD", dupe_pk == 0, f"duplicate ids={dupe_pk}"))

    # 8. the category column must only contain allowed values
    cat = col_ref(header, cfg["category_column"])
    bad_cat = session.sql(
        f"SELECT COUNT(*) c FROM GK_TEMP_RAW WHERE {cat} NOT IN ("
        + ",".join([f"'{v}'" for v in cfg["valid_categories"]]) + ")").collect()[0]["C"]
    R(DQResult(f"valid_{cfg['category_column']}", "THRESHOLD", bad_cat == 0,
               f"invalid values={bad_cat}"))

    # ---- ADVISORY tier: warnings only (do NOT block the load). ----
    # 11. fully duplicate rows
    all_refs = ",".join([f"C{i}" for i in range(len(header))])
    dupe_rows = session.sql(
        "SELECT COUNT(*) c FROM (SELECT *, COUNT(*) OVER "
        f"(PARTITION BY {all_refs}) n FROM GK_TEMP_RAW) WHERE n > 1").collect()[0]["C"]
    R(DQResult("duplicate_rows", "ADVISORY", dupe_rows == 0, f"dup rows={dupe_rows}"))

    # 12. dates fall within the allowed range
    dref = col_ref(header, cfg["date_column"])
    bad_date = session.sql(
        f"SELECT COUNT(*) c FROM GK_TEMP_RAW WHERE TRY_TO_DATE({dref}) "
        f"NOT BETWEEN '{COMMON['date_min']}' AND '{COMMON['date_max']}'").collect()[0]["C"]
    R(DQResult("date_range", "ADVISORY", bad_date == 0, f"out-of-range dates={bad_date}"))

    return results


def load_file(session, file_name, cfg):
    """The file PASSED — load its rows into the REAL RAW table.
       As it loads, it stamps 3 tracking columns on every row:
         file_name    - which file the row came from
         upload_dttm  - when the file arrived in S3
         load_dttm    - when Snowflake loaded it (now, in UTC)
       Reads FROM the incoming folder (never from processed).
       Returns how many rows THIS file added."""
    before = session.sql(
        f"SELECT COUNT(*) c FROM {cfg['target_table']}").collect()[0]["C"]
    session.sql(f"""
        COPY INTO {cfg['target_table']}
        ({cfg['load_columns']})
        FROM (
            SELECT {cfg['load_select']},
                   METADATA$FILENAME,
                   METADATA$FILE_LAST_MODIFIED::TIMESTAMP_NTZ,
                   CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
            FROM @{COMMON['incoming_stage']}/{file_name}
        )
        FILE_FORMAT = (FORMAT_NAME = '{COMMON['file_format']}')
        ON_ERROR = ABORT_STATEMENT
    """).collect()
    after = session.sql(
        f"SELECT COUNT(*) c FROM {cfg['target_table']}").collect()[0]["C"]
    return after - before


def move_file(session, file_name, dest_stage):
    """Move the file out of the incoming folder into a timestamped subfolder of the
       destination (processed/ for good files, quarantine/ for bad ones).
       The timestamp means the same filename can be uploaded again tomorrow
       without overwriting today's copy. Copies first, then removes the original."""
    stamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    session.sql(f"COPY FILES INTO @{dest_stage}/{stamp}/ "
                f"FROM @{COMMON['incoming_stage']}/{file_name}").collect()
    session.sql(f"REMOVE @{COMMON['incoming_stage']}/{file_name}").collect()


def get_recipients(session):
    """Read the list of people who should receive quarantine alerts from the
       EMAIL_RECIPIENT_LOG table (only active recipients for DQ_FAILURE alerts)."""
    rows = session.sql(
        "SELECT recipient_email FROM HEALTHCARE_DB.AUDIT.EMAIL_RECIPIENT_LOG "
        "WHERE alert_type='DQ_FAILURE' AND is_active=TRUE").collect()
    return [r["RECIPIENT_EMAIL"] for r in rows]


def send_email(session, recipients, subject, body):
    """Send an alert email to each recipient using Snowflake's email integration."""
    safe_subject = subject.replace("'", "")
    safe_body = body.replace("'", "")
    for to in recipients:
        session.sql("CALL SYSTEM$SEND_EMAIL(?, ?, ?, ?)",
                    params=[COMMON["email_integration"], to, safe_subject, safe_body]).collect()


def log_file(session, run_id, file_name, status, rows, passed, failed,
             feed="", failed_checks="", action=""):
    """Write one summary row per file to FILE_PROCESSING_LOG (the 'report card').
       Records the status (PASSED/QUARANTINED), row counts, the feed, the list of
       failed checks, and the recommended action — so the table mirrors the email."""
    session.sql(
        "INSERT INTO HEALTHCARE_DB.AUDIT.FILE_PROCESSING_LOG "
        "(run_id, file_name, status, rows_loaded, checks_passed, checks_failed, "
        " feed_type, failed_checks, action, processed_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
        params=[run_id, file_name, status, rows, passed, failed,
                feed, failed_checks, action]).collect()


def log_checks(session, run_id, file_name, results):
    """Write one row per individual check to DQ_METRICS_LOG (the 'answer sheet').
       This is the detailed evidence behind the report card above."""
    for r in results:
        session.sql(
            "INSERT INTO HEALTHCARE_DB.AUDIT.DQ_METRICS_LOG "
            "VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
            params=[run_id, file_name, r.check_name, r.tier,
                    "PASS" if r.passed else "FAIL", r.detail]).collect()


def main():
    """The orchestrator — runs the whole gatekeeper, file by file:
         1. connect to Snowflake
         2. list the files waiting in the incoming folder
         3. for each file: read header -> load to temp -> run 12 checks -> log checks
         4. decide: PASS -> load + archive + log;  FAIL -> quarantine + email + log
         5. if any file was quarantined, raise an error at the end so Airflow
            marks the run for attention."""
    session = get_session()
    run_id = str(uuid.uuid4())[:8]     # short id linking all logs from this run
    print(f"[{run_id}] Gatekeeper starting")

    files = list_incoming(session)
    if not files:
        print(f"[{run_id}] No recognized files in incoming/ — nothing to do")
        session.close()
        return

    any_quarantined = False
    for file_name in files:
        cfg = detect_config(file_name)
        feed = cfg["file_pattern"]
        print(f"[{run_id}] Processing {file_name}  (feed: {feed})")

        # --- inspect the file ---
        header = read_header(session, file_name, cfg)
        load_to_temp(session, file_name, max(len(header), 1))
        results = run_checks(session, file_name, header, cfg)
        log_checks(session, run_id, file_name, results)   # save every check result

        # --- tally the results ---
        gate_fail = [r for r in results if r.tier == "GATE" and not r.passed]
        thresh_fail = [r for r in results if r.tier == "THRESHOLD" and not r.passed]
        passed_n = sum(1 for r in results if r.passed)
        failed_n = sum(1 for r in results if not r.passed)

        # --- decide: quarantine or load ---
        if gate_fail or thresh_fail:
            # BAD FILE: move to quarantine, log it, and email an alert. NOT loaded.
            any_quarantined = True
            move_file(session, file_name, COMMON["quarantine_stage"])
            failed_block = "\n".join([f"    - {r.check_name} [{r.tier}]: {r.detail}"
                                      for r in (gate_fail + thresh_fail)])
            quarantine_action = ("File moved to quarantine/ folder. "
                                 "Review and re-upload a corrected file.")
            log_file(session, run_id, file_name, "QUARANTINED", 0, passed_n, failed_n,
                     feed=feed, failed_checks=failed_block, action=quarantine_action)
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            email_body = (
                f"Healthcare GATEKEEPER validation FAILED - file quarantined.\n\n"
                f"  File:           {file_name}\n"
                f"  Feed:           {feed}\n"
                f"  Status:         QUARANTINED (not loaded)\n"
                f"  Checks passed:  {passed_n}\n"
                f"  Checks failed:  {failed_n}\n"
                f"  Quarantined at: {ts}\n\n"
                f"  Failed checks:\n{failed_block}\n\n"
                f"  Action: {quarantine_action}"
            )
            send_email(session, get_recipients(session),
                       f"GATEKEEPER QUARANTINE - {file_name}", email_body)
            print(f"[{run_id}] QUARANTINED {file_name}")
        else:
            # GOOD FILE: load the rows into RAW, then archive the file to processed.
            rows = load_file(session, file_name, cfg)
            move_file(session, file_name, COMMON["processed_stage"])
            log_file(session, run_id, file_name, "PASSED", rows, passed_n, failed_n,
                     feed=feed, failed_checks="",
                     action="Loaded successfully; file archived to processed/.")
            advisory_warn = [r for r in results if r.tier == "ADVISORY" and not r.passed]
            warn = (" (advisories: " + ", ".join(r.check_name for r in advisory_warn) + ")"
                    if advisory_warn else "")
            print(f"[{run_id}] PASSED {file_name}: loaded {rows} rows{warn}")

    session.close()

    # If anything was quarantined, raise an error so the Airflow run is flagged.
    if any_quarantined:
        raise RuntimeError(f"[{run_id}] Gatekeeper: one or more files quarantined.")
    print(f"[{run_id}] Gatekeeper complete — all files passed")


if __name__ == "__main__":
    main()