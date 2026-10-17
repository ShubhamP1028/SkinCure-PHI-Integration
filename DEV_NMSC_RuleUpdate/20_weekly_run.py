# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Weekly VOB check — four folders, six accounts, eight AccountIds
# MAGIC
# MAGIC Import into **DEV_NMSC**, beside the monthly notebooks — the
# MAGIC `dbutils.notebook.run("./…")` calls below are relative, so it has to sit in the
# MAGIC same folder.
# MAGIC
# MAGIC Runs only what a weekly VOB read needs. `02`, `05`, `04_account_map`, `07`,
# MAGIC `07b`, `08`, `09` and `10` are deliberately **not** here:
# MAGIC
# MAGIC | notebook | why / why not |
# MAGIC |---|---|
# MAGIC | `01_ingest` | extracts the new files. Manifest-driven, so it re-reads nothing. Handles scanned files itself via `ai_parse_document` — there is no separate OCR step |
# MAGIC | `03_patient_days` | the matcher joins on patient-days, not lesions. Also where the facility routing fires |
# MAGIC | `04b_new_account_preflight` | the gate. Reads `staged.patient_days`, so it must run AFTER 03 |
# MAGIC | `06_match` | the actual VOB match |
# MAGIC | ~~`02_classify`~~ | `staged.lesion_classified` is a VIEW built by `01`. Cheap insurance, not a requirement |
# MAGIC | `05_vob_source` | **[10 Sep] run it ONCE now.** Everything it builds is a VIEW over live Salesforce, so `raw.vob` is normally already current — but the view DEFINITIONS changed when the treatment-log SIMULATION join was added, so they have to be replaced. After that, back to never: re-run only to widen `vob_to` past 2027-01-01, or if the record type or excluded statuses change |
# MAGIC | ~~`04_account_map`~~ | runs when a folder is genuinely NEW. The four weekly folders route onto existing accounts, so they need no map row of their own |
# MAGIC | ~~`07`…`10`~~ | the monthly package |
# MAGIC
# MAGIC **Two lesion-level sheets, both off the `weekly_lesions` temp view built in 3c:**
# MAGIC
# MAGIC - **4** — the conversion read. Masked names, every copy of every lesion, VOB scope
# MAGIC   (own site / sibling / enterprise), `Window closed`.
# MAGIC - **4d** — the working read. Real names, ONE row per lesion, with the Action / Plan,
# MAGIC   Comments, Notifications and Notes from **every** copy concatenated, so a patient
# MAGIC   who moved from *pending completion* to *completed* reads as one line with both
# MAGIC   sets of notes on it.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("window_start", "2026-04-01", "Biopsy window start (inclusive)")
dbutils.widgets.text("window_end",   "2026-10-01", "Biopsy window end (exclusive)")
dbutils.widgets.text("gap_after",    "180", "Days a biopsy may still produce a VOB")
dbutils.widgets.dropdown("run_ingest",  "yes", ["yes", "no"], "1  run 01_ingest")
dbutils.widgets.dropdown("seed_routes", "yes", ["yes", "no"], "2  seed facility_route")
dbutils.widgets.dropdown("run_days",    "yes", ["yes", "no"], "3  run 03_patient_days")
dbutils.widgets.dropdown("run_preflight","yes",["yes", "no"], "4  run 04b preflight")
dbutils.widgets.dropdown("run_match",   "yes", ["yes", "no"], "5  run 06_match")
# [10 Sep] 05 used to be "never needed" -- everything it builds is a VIEW over live
# Salesforce, so raw.vob was always current. That changed when the treatment-log
# SIMULATION join was added: the view DEFINITIONS moved, so 05 has to run ONCE to
# replace them. It is view DDL only, seconds, and changes no stored data.
dbutils.widgets.dropdown("run_vob_source", "no", ["yes", "no"], "5a run 05_vob_source")

C   = dbutils.widgets.get("catalog")
W0  = dbutils.widgets.get("window_start")
W1  = dbutils.widgets.get("window_end")
GAP = dbutils.widgets.get("gap_after")

_common = {"catalog": C}
_window = {"catalog": C, "window_start": W0, "window_end": W1, "gap_after": GAP}

# The SIX ACCOUNT names, not the four folder names. By the time the sheet runs, the
# facility routing and FOLDER_CANON have replaced the folder names with these.
ACCOUNTS = [
    "Farmington Valley Dermatology - Avon",
    "Farmington Valley Dermatology - South Windsor",
    "Commonwealth Dermatology - Worcester",
    "Kane Dermatology - Somers Point",
    "Seraly Dermatology - McMurray #1 & 2",
    "Schaberg Dermatology - Maryville",
]
FOLDER_LIKE = "Weekly - %"

# [11 Sep, Becky] "previous VOB within the last 3 months". 90 days rather than
# add_months(-3) so the window is a fixed length regardless of month lengths.
# Anchored on the ROW'S BIOPSY DATE, not on today -- "had this patient been verified
# in the 3 months before this biopsy". Anchoring on today would make the column mean
# something different every week the report is re-run, and an old export would stop
# agreeing with itself.
PRIOR_VOB_DAYS = 90

# The EIGHT AccountIds behind the six accounts. Two are POOLED and every VOB count
# must sum across both ids. Schaberg's sister is ALTON — a different city, which is
# what makes it the one people miss.
ACCOUNT_IDS = {
    "Farmington Valley Dermatology - Avon":          ["0013h00000qUA5NAAW"],
    "Farmington Valley Dermatology - South Windsor": ["001Do00000Kg32SIAR"],
    "Commonwealth Dermatology - Worcester":          ["0013h00001YV1rcAAD"],
    "Kane Dermatology - Somers Point":               ["0013h00001yOD8SAAW"],
    "Seraly Dermatology - McMurray #1 & 2":          ["001UN0000096FHOYA2",   # McMurray #1
                                                      "001UN00000TiqWsYAJ"],  # McMurray #2
    "Schaberg Dermatology - Maryville":              ["0013h00000SbamkAAB",   # Maryville
                                                      "001Do00000Hi5RcIAJ"],  # ALTON
}
_ids = ", ".join("'" + i + "'" for v in ACCOUNT_IDS.values() for i in v)

_in = ", ".join("'" + a.replace("'", "''") + "'" for a in ACCOUNTS)

# ---------------------------------------------------------------------------------
# [11 Sep] THESE TWO EXPRESSIONS MUST MATCH 03_patient_days EXACTLY.
#
# Cell 3c used to carry hand-written versions and they had drifted, which is why a
# patient with a real VOB and a real vob_match row read `Any VOB for this patient` =
# No: weekly_lesions built a different (patient_id, biopsy_date) than patient_days
# did, so the join to staged.vob_match found nothing. The check at the end of 3c now
# proves they agree instead of assuming it.
#
# DATE: 00_vocab.date_parse_sql. A bare try_to_date() only accepts yyyy-MM-dd, so
# `7/15/2026` becomes NULL -- and a NULL date is silently FILTERED OUT of the sheet
# rather than showing up wrong.
_DATE_FORMATS = ["M/d/yyyy", "M/d/yy", "d-MMM-yyyy", "d-MMM-yy",
                 "MMMM d, yyyy", "MMM d, yyyy", "MMMM d yyyy"]
def _dparse(c):
    return "coalesce(" + ", ".join([f"try_to_date({c})"]
           + [f"try_to_date({c}, '{f}')" for f in _DATE_FORMATS]) + ")"

# PATIENT ID: 03_patient_days lines 184, 203-208. The accession IS the EMA MRN, but
# header text leaks into that column ('MRN', 'Accession', 'DOB' ...) and so do 'nan',
# 'None' and '0'. 03 rejects all of them and falls back to the NAME. A copy that does
# not reject them keys those rows on the junk string -- and every such patient at an
# account collapses onto the SAME id.
_MRN_JUNK = ("'mrn', 'accession', 'accession #', 'acc', 'acc #', 'patient', "
             "'patient id', 'id', 'name', 'dob', 'chart', 'chart #', 'mr #', 'mr'")
_PID = f"""CASE
    WHEN coalesce(trim(l.accession), '') NOT IN ('', 'nan', 'None', '0')
     AND lower(trim(l.accession)) NOT IN ({_MRN_JUNK})
      THEN regexp_replace(trim(l.accession), '\\\\.0$', '')
    ELSE regexp_replace(lower(trim(coalesce(l.patient_name, ''))), '\\\\s+', ' ')
  END"""
print(f"catalog {C} · window {W0} → {W1} · gap_after {GAP}")
print(f"{len(ACCOUNTS)} accounts:\n  " + "\n  ".join(ACCOUNTS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Pre-flight the landing table
# MAGIC
# MAGIC The upstream Lakeflow job lands file BINARIES into
# MAGIC `raw.pathology_monthly_exports`. `01_ingest` is what turns them into rows.
# MAGIC
# MAGIC `01` takes `account_name` from the **second-to-last path segment**, so the weekly
# MAGIC files must sit *directly* inside their `Weekly - …` folder. One level too deep
# MAGIC (`…/Weekly - Kane/Phase1/x.pdf`) and `account_name` becomes `Phase1`, which maps
# MAGIC to no AccountId and reads as a practice converting nobody.

# COMMAND ----------

display(spark.sql(f"""
SELECT element_at(split(path, '/'), -2)               AS would_be_account_name,
       count(*)                                       AS files,
       min(element_at(split(path, '/'), -1))          AS example_file
FROM {C}.raw.pathology_monthly_exports
WHERE lower(path) LIKE '%weekly%'
GROUP BY 1 ORDER BY 1
"""))
# PASS: exactly four groups — Dr Waqar / Kane / Schaberg / Seraly.
# Percent-encoding (Weekly%20-%20Kane) is fine, 01 url_decodes it.

# COMMAND ----------

if dbutils.widgets.get("run_ingest") == "yes":
    dbutils.notebook.run("./01_ingest", 7200, _common)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Did the extract land?
# MAGIC
# MAGIC One row per folder / facility / layout with a verdict. The facility text is what
# MAGIC the routing in step 2 keys on, so this has to be right before the routes go in.
# MAGIC
# MAGIC **9 Sep run:** all six came back `TABLE` with zero scanned rows — including Avon
# MAGIC and Worcester, which are image-only in the MONTHLY Phase-1 exports. The weekly
# MAGIC export is a different artefact and carries a text layer, so `ai_parse_document`
# MAGIC is not needed. If a future week shows `AI_PARSE` rows that is fine too; what is
# MAGIC NOT fine is a scanned file producing **no** rows, which `01` does not error on.

# COMMAND ----------

display(spark.sql(f"""
WITH g AS (
  SELECT account_name                                    AS folder,
         coalesce(nullif(trim(facility), ''), '(BLANK)') AS facility,
         layout,
         count(*)                                        AS row_count,
         count(DISTINCT file_name)                       AS files,
         sum(CASE WHEN is_scanned THEN 1 ELSE 0 END)     AS scanned_rows,
         sum(CASE WHEN pathology RLIKE '^__AI_PARSE_ERROR__' THEN 1 ELSE 0 END)
                                                         AS parse_errors,
         min(collected_date) AS first_biopsy, max(collected_date) AS last_biopsy
  FROM {C}.raw.path_log_rows
  WHERE account_name LIKE '{FOLDER_LIKE}'
  GROUP BY account_name, coalesce(nullif(trim(facility), ''), '(BLANK)'), layout
)
SELECT *, CASE
    WHEN layout IS NULL       THEN 'FAIL - nothing parsed out of this file'
    WHEN parse_errors > 0     THEN concat('FAIL - ', parse_errors, ' AI_PARSE errors')
    WHEN facility = '(BLANK)' AND folder LIKE '%Waqar%'
      THEN 'FAIL - blank facility under Waqar cannot be routed to one of its 3 accounts'
    WHEN facility = '(BLANK)' THEN 'WARN - blank facility, needs the FOLDER_CANON fallback'
    WHEN facility NOT IN ('Farmington Valley Dermatology - Avon',
                          'Farmington Valley Dermatology - South Windsor',
                          'Commonwealth Dermatology - Worcester',
                          'Kane Dermatology', 'Seraly Dermatology',
                          'Schaberg Dermatology - Maryville')
      THEN 'CHECK - facility not in the expected six'
    ELSE 'ok' END AS verdict
FROM g ORDER BY folder, facility, layout
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The facility routes
# MAGIC
# MAGIC **`Weekly - Dr Waqar` is one folder over THREE accounts** in two practices in two
# MAGIC states. `FOLDER_CANON` is folder→account and cannot express one folder over three,
# MAGIC so the split is keyed on **facility**. The three strings are mutually exclusive.
# MAGIC The other three folders get one broad rule each — `Schaberg Dermatology` rather
# MAGIC than `… - Maryville`, so an Alton row routes instead of stranding.
# MAGIC
# MAGIC Idempotent: only rules that are not already present are inserted, so re-running
# MAGIC this notebook cannot create the duplicates that make `03` raise on overlaps.
# MAGIC
# MAGIC The two Polley seeds are included because `03` writes them **only when it finds
# MAGIC the table empty** — pre-populating with just the weekly six would lose them, and
# MAGIC with them the Fayetteville #1/#2 misfiling fix.

# COMMAND ----------

FACROUTE = f"{C}.raw.facility_route"

ROUTES = [
    # ---- the two seeds 03_patient_days would write itself
    ("Fay 110", "Polley Clinic of Dermatology - Fayetteville #1",
     "Polley Clinic of Dermatology - Fayetteville%",
     "site 110 is Fayetteville #1 (21 Aug - 3 files misfiled into #2)"),
    ("Fay 106", "Polley Clinic of Dermatology - Fayetteville #2",
     "Polley Clinic of Dermatology - Fayetteville%",
     "site 106 is Fayetteville #2"),
    # ---- Dr Waqar: one folder, three accounts
    ("Farmington Valley Dermatology - Avon", "Farmington Valley Dermatology - Avon",
     "Weekly - Dr Waqar%",
     "Waqar covers 3 accounts in 2 practices - facility is what splits them"),
    ("Farmington Valley Dermatology - South Windsor",
     "Farmington Valley Dermatology - South Windsor", "Weekly - Dr Waqar%",
     "Actions coverage 32.5% - read Appropriate to VOB, not Marked to VOB"),
    ("Commonwealth Dermatology - Worcester", "Commonwealth Dermatology - Worcester",
     "Weekly - Dr Waqar%",
     "Sister Fall River (001Do00000RLE6cIAH) has no workbook row"),
    # ---- the three 1:1 folders
    ("Kane Dermatology", "Kane Dermatology - Somers Point", "Weekly - Kane%",
     "Facility carries no city, so the substring is the practice name alone"),
    ("Seraly Dermatology", "Seraly Dermatology - McMurray #1 & 2", "Weekly - Seraly%",
     "Pooled #1 + #2. Facility cannot separate the sites - harmless, it is pooled"),
    ("Schaberg Dermatology", "Schaberg Dermatology - Maryville", "Weekly - Schaberg%",
     "Pooled Maryville + ALTON. Broad substring on purpose so an Alton row routes"),
]
SCHEMA = ("facility_substring STRING, owner_account STRING, "
          "applies_within STRING, note STRING")

if dbutils.widgets.get("seed_routes") == "yes":
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {FACROUTE} (
      facility_substring STRING, owner_account STRING,
      applies_within STRING, note STRING) USING DELTA""")
    have = {(r["facility_substring"], r["owner_account"], r["applies_within"])
            for r in spark.table(FACROUTE).collect()}
    todo = [r for r in ROUTES if (r[0], r[1], r[2]) not in have]
    if todo:
        spark.createDataFrame(todo, SCHEMA).write.mode("append").saveAsTable(FACROUTE)
        print(f"inserted {len(todo)} route(s); {len(have)} were already there")
    else:
        print(f"all {len(ROUTES)} routes already present — nothing inserted")

display(spark.table(FACROUTE).orderBy("applies_within", "facility_substring"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b · Dry-run the rules before `03` does
# MAGIC
# MAGIC The same join `03_patient_days` uses, so whatever comes back here is exactly what
# MAGIC `03` will do. Catches a bad rule in seconds instead of after a 10-minute run.
# MAGIC
# MAGIC **PASS:** six rows, every `rules_matched = 1`, and no `routed_to` still reading
# MAGIC `Weekly - …`.
# MAGIC
# MAGIC - `routed_to` still `Weekly - …` → no rule fired. The facility text does not match.
# MAGIC - `rules_matched = 2` → `03` will refuse to run; two rules would fan one lesion
# MAGIC   into two rows. Narrow one substring.

# COMMAND ----------

display(spark.sql(f"""
SELECT account_name, file_name, count(*) AS rows,
       sum(CASE WHEN facility  IS NULL THEN 1 ELSE 0 END) AS null_fac,
       sum(CASE WHEN pathology IS NULL THEN 1 ELSE 0 END) AS null_path,
       round(100.0 * avg(CASE WHEN facility IS NULL OR pathology IS NULL
                              THEN 1.0 ELSE 0.0 END), 1) AS pct_damaged
FROM {C}.staged.lesion_classified
WHERE is_scanned
GROUP BY 1, 2
HAVING pct_damaged > 0
ORDER BY pct_damaged DESC, rows DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · `03_patient_days`
# MAGIC
# MAGIC **The window must cover every month you want to keep matched.** It writes with
# MAGIC `replaceWhere ym IN (…)`, so it replaces whole months — and because
# MAGIC `staged.lesion_classified` is a VIEW over the append-only bronze table, every
# MAGIC account ever ingested is recomputed, not just this week's six. A window that
# MAGIC skips a month leaves that month stale.
# MAGIC
# MAGIC Watch its output for **`rerouted N lesion rows of M`** — N must be non-zero.
# MAGIC Zero means no rule fired and every weekly row is still on a `Weekly - …` folder.

# COMMAND ----------

if dbutils.widgets.get("run_days") == "yes":
    dbutils.notebook.run("./03_patient_days", 3600, _window)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b · Did the routing land?  No `Weekly - …` account may survive.

# COMMAND ----------

try:
    print([f.name for f in spark.table(f"{C}.raw.facility_route").schema.fields])
    display(spark.table(f"{C}.raw.facility_route"))
except Exception as e:
    print("facility_route:", type(e).__name__, e)

# COMMAND ----------

display(spark.sql(f"""
SELECT l.account_name                                  AS folder,
       concat('[', l.facility, ']')                    AS facility_bracketed,
       length(l.facility)                              AS len,
       length(trim(l.facility))                        AS len_trimmed,
       regexp_replace(l.facility, '[^ -~]', '<?>')     AS non_ascii_shown,
       max(r.owner_account)                            AS routes_to,
       max(ci.owner_account)                           AS would_route_if_case_insensitive,
       count(*)                                        AS lesion_rows,
       count(DISTINCT l.file_name)                     AS files,
       min(l.file_name)                                AS example_file
FROM {C}.staged.lesion_classified l
LEFT JOIN {C}.raw.facility_route r
       ON l.account_name LIKE r.applies_within
      AND l.facility     LIKE concat('%', r.facility_substring, '%')
LEFT JOIN {C}.raw.facility_route ci
       ON lower(l.account_name) LIKE lower(ci.applies_within)
      AND lower(l.facility)     LIKE concat('%', lower(ci.facility_substring), '%')
WHERE l.account_name LIKE 'Weekly - %'
GROUP BY 1,2,3,4,5
ORDER BY routes_to NULLS FIRST, lesion_rows DESC
"""))

# COMMAND ----------

if dbutils.widgets.get("run_preflight") == "yes":
    dbutils.notebook.run("./04b_new_account_preflight", 1800, {**_common, "write": "no"})
# PASS: unmapped (BLOCKING, will score zero VOBs) : 0

# COMMAND ----------

# 05 FIRST when it runs at all -- 06 reads raw.vob, so a stale view definition here
# means simulation_actual is simply absent from the snapshot and cells 4/4d fail on
# an unresolved column.
if dbutils.widgets.get("run_vob_source") == "yes":
    dbutils.notebook.run("./05_vob_source", 1800, _common)

if dbutils.widgets.get("run_match") == "yes":
    dbutils.notebook.run("./06_match", 3600, _window)
    # ASSERT the new column actually landed. Cheaper to fail here than three cells on.
    _cols = set(spark.table(f"{C}.staged.vob_match").columns)
    _need = {"simulation_scheduled", "simulation_actual", "group_has_sim",
             "group_simulated_actual"}
    _miss = sorted(_need - _cols)
    if _miss:
        raise AssertionError(
            f"staged.vob_match is missing {_miss}. Re-run with '5a run 05_vob_source' = "
            f"yes -- 06 carries simulation_actual through from raw.vob, so a stale 05 "
            f"view definition silently drops it.")
    print("vob_match carries both simulation stages")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c · Resolve the folder names ONCE — `weekly_lesions`
# MAGIC
# MAGIC `staged.lesion_classified.account_name` is the **FOLDER** name. Facility routing
# MAGIC and `FOLDER_CANON` are applied downstream, in `03_patient_days` (line 361) — so a
# MAGIC lesion-level query that filters on the six real account names matches nothing.
# MAGIC The `Weekly - Dr Waqar` folder alone carries three accounts.
# MAGIC
# MAGIC This cell does at lesion grain what `03` does at patient-day grain, once, into a
# MAGIC temp view every sheet below reads. `routes` is pre-aggregated so the join cannot
# MAGIC fan out; `rules_matched > 1` would mean two rules claim one facility.
# MAGIC
# MAGIC `status_rank` is how far along the path-log workflow a copy sits, taken from the
# MAGIC file name: Completed 5 · Pending Completion 4 · Pending Plan 3 · Pending 2 ·
# MAGIC Unresolved 1.

# COMMAND ----------

display(spark.sql(f"""
WITH w AS (SELECT DISTINCT account_resolved AS acct, patient_id AS pid, biopsy_date AS dt
           FROM weekly_lesions),
     p AS (SELECT DISTINCT account_name AS acct, patient_id AS pid, biopsy_date AS dt
           FROM {C}.staged.patient_days),
   orph AS (SELECT w.* FROM w LEFT ANTI JOIN p
            ON p.acct = w.acct AND p.pid = w.pid AND p.dt = w.dt)
SELECT o.acct, o.dt AS weekly_date,
       length(o.pid) AS pid_len,
       CASE WHEN o.pid IS NULL THEN 'null'
            WHEN upper(trim(o.pid)) IN ('MRN','DOB','NAME','PATIENT') THEN 'HEADER WORD'
            WHEN o.pid RLIKE '^[0-9]+$' THEN 'all digits'
            WHEN o.pid RLIKE '^[A-Za-z]' THEN 'starts with a letter'
            ELSE 'other' END AS pid_shape,
       (a.pid IS NOT NULL) AS same_patient_exists_other_date,
       (b.dt  IS NOT NULL) AS same_date_exists_other_patient
FROM orph o
LEFT JOIN (SELECT DISTINCT acct, pid FROM p) a ON a.acct = o.acct AND a.pid = o.pid
LEFT JOIN (SELECT DISTINCT acct, dt  FROM p) b ON b.acct = o.acct AND b.dt  = o.dt
"""))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW weekly_lesions AS
WITH routes AS (
  SELECT d.account_name, d.facility,
         max(fr.owner_account)   AS owner_account,
         count(fr.owner_account) AS rules_matched
  FROM (SELECT DISTINCT account_name, facility
        FROM {C}.staged.lesion_classified
        WHERE account_name LIKE '{FOLDER_LIKE}') d
  LEFT JOIN {C}.raw.facility_route fr
         ON d.account_name LIKE fr.applies_within
        AND lower(coalesce(d.facility, '')) LIKE concat('%', lower(fr.facility_substring), '%')
        AND d.account_name <> fr.owner_account
  GROUP BY d.account_name, d.facility
)
SELECT
  l.*,
  coalesce(rt.owner_account, l.account_name)  AS account_resolved,
  coalesce(rt.rules_matched, 0)               AS rules_matched,
  {_dparse('l.collected_date')}               AS biopsy_dt,
  {_dparse('l.result_date')}                  AS result_dt,
  {_PID}                                      AS patient_id,
  CASE
    WHEN lower(l.file_name) LIKE '%unresolved%'         THEN 'Unresolved'
    WHEN lower(l.file_name) LIKE '%pending plan%'       THEN 'Pending Plan'
    WHEN lower(l.file_name) LIKE '%pending completion%' THEN 'Pending Completion'
    WHEN lower(l.file_name) LIKE '%pending%'            THEN 'Pending'
    WHEN lower(l.file_name) LIKE '%completed%'          THEN 'Completed'
    ELSE 'Unknown'
  END                                         AS path_status,
  CASE
    WHEN lower(l.file_name) LIKE '%unresolved%'         THEN 1
    WHEN lower(l.file_name) LIKE '%pending plan%'       THEN 3
    WHEN lower(l.file_name) LIKE '%pending completion%' THEN 4
    WHEN lower(l.file_name) LIKE '%pending%'            THEN 2
    WHEN lower(l.file_name) LIKE '%completed%'          THEN 5
    ELSE 0
  END                                         AS status_rank
FROM {C}.staged.lesion_classified l
LEFT JOIN routes rt
       ON rt.account_name = l.account_name
      AND coalesce(rt.facility, '') <=> coalesce(l.facility, '')
WHERE l.account_name LIKE '{FOLDER_LIKE}'
  AND {_dparse('l.collected_date')} >= date('{W0}')
  AND {_dparse('l.collected_date')} <  date('{W1}')
""")

_r = spark.sql(f"""
SELECT account_resolved, count(*) AS lesion_rows,
       count(DISTINCT file_name) AS files,
       max(rules_matched)        AS max_rules_matched
FROM weekly_lesions GROUP BY 1 ORDER BY 1
""")
display(_r)

# ASSERT: the six accounts, nothing still called "Weekly - ...", no double-claimed facility
_bad = spark.sql(f"""
SELECT count(*) AS unrouted FROM weekly_lesions
WHERE account_resolved LIKE '{FOLDER_LIKE}' OR rules_matched > 1
""").first()["unrouted"]
if _bad:
    raise AssertionError(f"{_bad} lesion rows unrouted or double-claimed — fix facility_route in 2")
_seen = {r["account_resolved"] for r in _r.collect()}
_missing = [a for a in ACCOUNTS if a not in _seen]
print(f"resolved cleanly · {len(_seen)} accounts" +
      (f"\n  NO ROWS: " + ", ".join(_missing) if _missing else ""))

# ---------------------------------------------------------------------------------
# [11 Sep] THE KEY RECONCILIATION. This is the check that was missing.
#
# Cell 4 joins staged.vob_match on (account, patient_id, biopsy_date). vob_match gets
# those keys from staged.patient_days; weekly_lesions builds its own. If the two ever
# disagree the join silently finds nothing and the report reads `Any VOB for this
# patient` = No for a patient who plainly has one. No error, no warning -- just a No.
#
# So prove they agree. An orphan here is a key weekly_lesions invented that
# patient_days never made.
_orph = spark.sql(f"""
WITH wl AS (
  SELECT DISTINCT account_resolved AS account_name, patient_id, biopsy_dt AS biopsy_date
  FROM weekly_lesions WHERE biopsy_dt IS NOT NULL
),
pd AS (
  SELECT DISTINCT account_name, patient_id, biopsy_date
  FROM {C}.staged.patient_days
  WHERE account_name IN ({_in})
)
SELECT (SELECT count(*) FROM wl)                                   AS weekly_keys,
       (SELECT count(*) FROM pd)                                   AS patient_day_keys,
       (SELECT count(*) FROM wl LEFT ANTI JOIN pd
          ON pd.account_name = wl.account_name
         AND pd.patient_id   = wl.patient_id
         AND pd.biopsy_date  = wl.biopsy_date)                     AS orphans
""").first()
print(f"key reconciliation · {_orph['weekly_keys']:,} weekly keys vs "
      f"{_orph['patient_day_keys']:,} patient-day keys")
if _orph["orphans"]:
    print(f"  *** {_orph['orphans']:,} ORPHAN KEYS — these patient-days CANNOT match a VOB. "
          f"Every one will read 'Any VOB for this patient' = No regardless of the truth. ***")
    display(spark.sql(f"""
    WITH wl AS (
      SELECT DISTINCT account_resolved AS account_name, patient_id, biopsy_dt AS biopsy_date
      FROM weekly_lesions WHERE biopsy_dt IS NOT NULL
    ),
    pd AS (
      SELECT DISTINCT account_name, patient_id, biopsy_date
      FROM {C}.staged.patient_days WHERE account_name IN ({_in})
    )
    SELECT wl.*,
           -- which half is wrong? A name-shaped id beside a numeric one at the same
           -- account and date means the accession fell back differently.
           CASE WHEN wl.patient_id RLIKE '^[0-9.]+$' THEN 'numeric (accession)'
                ELSE 'name' END                              AS id_shape,
           (SELECT count(*) FROM pd p WHERE p.account_name = wl.account_name
                                        AND p.biopsy_date  = wl.biopsy_date) AS pd_rows_same_day
    FROM wl LEFT ANTI JOIN pd
      ON pd.account_name = wl.account_name AND pd.patient_id = wl.patient_id
     AND pd.biopsy_date  = wl.biopsy_date
    ORDER BY account_name, biopsy_date LIMIT 50
    """))
    raise AssertionError(
        f"{_orph['orphans']:,} weekly keys have no patient_day. weekly_lesions and 03 "
        f"are building (patient_id, biopsy_date) differently — compare _PID and _dparse "
        f"in the config cell against 03_patient_days lines 184, 203-215.")
print("  every weekly key exists as a patient-day — the VOB join can resolve")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · THE WEEKLY REPORT
# MAGIC
# MAGIC [10 Sep, Becky] The 16 columns of `Weekly Reporting 1.xlsx`, in her order, plus
# MAGIC the two simulation dates on the end. This IS the deliverable — what displays, and
# MAGIC what section 5 writes out.
# MAGIC
# MAGIC | # | column | |
# MAGIC |---|---|---|
# MAGIC | 1 | Path Status | from the file name. Was blank on 42 of 131 rows in the sample; now populated for every row |
# MAGIC | 2–7 | Account · Month · Week · Date · Patient (masked) · lesions Identified | |
# MAGIC | 8–13 | Anatomical location · Result (Pathology) · Actions and Plans · Comments / Assigned To · Flags · Notes | |
# MAGIC | 14–16 | Appropriate (lesion) · SRT signal (lesion) · Any VOB for this patient | |
# MAGIC | 17–18 | **Sim Date · Sim Scheduled Date** | new |
# MAGIC | 19 | **Patient VOB (other date)** | this row says No, but the patient has a counted VOB on another biopsy date |
# MAGIC | 20 | **Previous VOB (90d)** | the patient was already verified in the 90 days BEFORE this biopsy |
# MAGIC
# MAGIC **`Any VOB for this patient` is the MATCHED answer** — account, date window and
# MAGIC name. The uploaded sheet used a bare name lookup against the whole verification
# MAGIC pull, which answers *"does someone with this name appear on any VOB anywhere"* and
# MAGIC overstates. Expect fewer `Yes` values than the sample, each one a verification
# MAGIC actually tied to that biopsy.
# MAGIC
# MAGIC **One row per lesion per file, NOT deduplicated** — same as the uploaded sheet. A
# MAGIC lesion on both a pending and a completed log gives two rows with different
# MAGIC `Path Status`, which is the point of having that column. Cell **4d** is the
# MAGIC deduplicated view with the note history concatenated.
# MAGIC
# MAGIC **The two sim dates come from the VOBs matched to that patient-day.**
# MAGIC `Sim Scheduled Date` is when the patient was meant to come in; `Sim Date` is the
# MAGIC treatment log saying they did. Scheduled filled with `Sim Date` empty, once the
# MAGIC scheduled date has passed, is a **no-show**. Header order follows Becky's wording;
# MAGIC swap the two lines to read chronologically.
# MAGIC
# MAGIC Two notes on fidelity to the uploaded file. The header `Any VOB for this patient`
# MAGIC had a **trailing space**; dropped, because a trailing space silently breaks
# MAGIC lookups keyed on the header. And `weekly_full` below keeps the working columns
# MAGIC (`VOB match`, `Days observed`, `Window closed`) that 4b's still-open vs
# MAGIC closed-no-VOB split needs — it is not a second report, just the helper the check
# MAGIC reads.

# COMMAND ----------

# The working view. NOT the deliverable -- 4b reads it for the maturity split, which
# needs columns the report does not carry.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW weekly_full AS
WITH vob AS (
  SELECT matched_folder AS account_name, patient_id, biopsy_date,
         count(DISTINCT vob_id)             AS vobs,
         min(vob_created)                   AS first_vob_created,
         min(gap_days)                      AS gap_days,
         min(status)                        AS vob_status,
         -- TWO STAGES. simulation_scheduled is when the patient was meant to come in;
         -- simulation_actual is the treatment log saying they did.
         min(to_date(simulation_scheduled)) AS sim_sched_date,
         min(to_date(simulation_actual))    AS sim_actual_date
  FROM {C}.staged.vob_match WHERE matched AND matched_folder IS NOT NULL
  GROUP BY 1,2,3
),
vob_any_date AS (
  -- [11 Sep] SAME PATIENT, SAME ACCOUNT, ANY BIOPSY DATE.
  --
  -- staged.vob_match holds ONE row per VOB, bound to exactly ONE patient-day by
  -- 06_match's _ranked (cell_state, appropriate, gap sign, |gap|). A patient
  -- biopsied twice with one VOB therefore has that VOB on ONE of the two dates --
  -- and the other date legitimately counts as unverified.
  --
  -- That is correct for conversion, and misleading for a work list: the therapist
  -- reads No and chases a patient who is already verified. So the fact is surfaced
  -- as its own column rather than folded into `Any VOB for this patient`, which
  -- stays strictly patient-DAY and keeps the rates intact.
  SELECT matched_folder AS account_name, patient_id,
         count(DISTINCT vob_id) AS vobs_any_date,
         concat_ws(', ', array_sort(collect_set(date_format(biopsy_date, 'yyyy-MM-dd'))))
                                AS vob_biopsy_dates
  FROM {C}.staged.vob_match WHERE matched AND matched_folder IS NOT NULL
  GROUP BY 1,2
),
prior_vob AS (
  -- [11 Sep, Becky] Had this patient ALREADY been verified in the {PRIOR_VOB_DAYS}
  -- days before this biopsy? A repeat patient is a different conversation from a
  -- first-time candidate, and the therapist should see it on the row.
  --
  -- Anchored on vob_created, not on the prior VOB's biopsy date: the question is
  -- when the VERIFICATION happened, which is what makes chasing them redundant.
  -- Strictly BEFORE this biopsy -- a VOB created after it is not "previous".
  -- Same account only; a sibling-site VOB does not count here.
  SELECT d.account_name, d.patient_id, d.biopsy_date,
         count(DISTINCT vm.vob_id)            AS prior_vobs,
         max(to_date(vm.vob_created))         AS last_prior_vob
  FROM (SELECT DISTINCT account_resolved AS account_name, patient_id,
                        biopsy_dt AS biopsy_date
        FROM weekly_lesions WHERE biopsy_dt IS NOT NULL) d
  JOIN {C}.staged.vob_match vm
    ON vm.matched_folder = d.account_name
   AND vm.patient_id     = d.patient_id
   AND vm.matched
   AND to_date(vm.vob_created) <  d.biopsy_date
   AND to_date(vm.vob_created) >= date_sub(d.biopsy_date, {PRIOR_VOB_DAYS})
  GROUP BY 1,2,3
),
scope AS (
  SELECT account_name, patient_id, biopsy_date,
         vob_own_site, vob_sharing_site, vob_enterprise
  FROM {C}.staged.patient_vob_scope
),
anyname AS (
  SELECT DISTINCT lower(trim(patient_name_raw)) AS nm FROM {C}.raw.vob
  WHERE trim(coalesce(patient_name_raw,'')) <> ''
)
SELECT
  k.path_status,
  k.account_resolved,
  k.biopsy_dt,
  k.patient_name,
  k.body_site, k.pathology, k.actions, k.comments, k.flags, k.notes,
  k.lesion_appropriate, k.srt_marked,
  coalesce(v.vobs, 0)                              AS vobs,
  v.first_vob_created, v.gap_days, v.vob_status,
  v.sim_sched_date, v.sim_actual_date,
  coalesce(va.vobs_any_date, 0)                    AS vobs_any_date,
  va.vob_biopsy_dates,
  coalesce(pv.prior_vobs, 0)                       AS prior_vobs,
  pv.last_prior_vob,
  CASE WHEN s.vob_own_site     THEN 'VOB at this site'
       WHEN s.vob_sharing_site THEN 'VOB at a sibling'
       WHEN s.vob_enterprise   THEN 'VOB elsewhere'
       WHEN a.nm IS NOT NULL   THEN 'name is on a VOB, but out of window'
       ELSE                         'no VOB found' END     AS vob_match,
  datediff(current_date(), k.biopsy_dt)                    AS days_observed,
  CASE WHEN datediff(current_date(), k.biopsy_dt) >= {GAP} THEN 'Yes' ELSE 'No' END
                                                           AS window_closed,
  count(*) OVER (PARTITION BY k.account_resolved, k.patient_id, k.biopsy_dt) AS n_lesions
FROM weekly_lesions k
LEFT JOIN scope   s ON s.account_name = k.account_resolved
                   AND s.patient_id   = k.patient_id AND s.biopsy_date = k.biopsy_dt
LEFT JOIN vob     v ON v.account_name = k.account_resolved
                   AND v.patient_id   = k.patient_id AND v.biopsy_date = k.biopsy_dt
LEFT JOIN vob_any_date va ON va.account_name = k.account_resolved
                         AND va.patient_id   = k.patient_id
LEFT JOIN prior_vob    pv ON pv.account_name = k.account_resolved
                         AND pv.patient_id   = k.patient_id
                         AND pv.biopsy_date  = k.biopsy_dt
LEFT JOIN anyname a ON a.nm = lower(trim(k.patient_name))
""")

# COMMAND ----------

# ---- THE REPORT. 18 columns, Becky's order. This is what section 5 writes. ----
sheet = spark.sql("""
SELECT
  path_status                                      AS `Path Status`,
  account_resolved                                 AS `Account`,
  date_format(biopsy_dt, 'MMMM')                   AS `Month`,
  cast(floor((dayofmonth(biopsy_dt) - 1) / 7) + 1 AS INT) AS `Week`,
  biopsy_dt                                        AS `Date`,
  --concat(substr(split(patient_name, ',')[0], 1, 1),
  --       repeat('*', greatest(length(trim(split(patient_name, ',')[0])) - 1, 0)), ', ',
  --       substr(trim(split(patient_name, ',')[1]), 1, 1),
  --       repeat('*', greatest(length(trim(split(patient_name, ',')[1])) - 1, 0)))
                               patient_name                    AS `Patient (masked)`,
  n_lesions                                        AS `lesions Identified`,
  body_site                                        AS `Anatomical location`,
  pathology                                        AS `Result (Pathology)`,
  actions                                          AS `Actions and Plans`,
  comments                                         AS `Comments / Assigned To`,
  flags                                            AS `Flags`,
  notes                                            AS `Notes`,
  CASE WHEN lesion_appropriate THEN 'Yes' ELSE 'No' END AS `Appropriate (lesion)`,
  CASE WHEN srt_marked         THEN 'Yes' ELSE 'No' END AS `SRT signal (lesion)`,
  -- MATCHED, not the bare name lookup: account + date window + name.
  CASE WHEN vobs > 0 THEN 'Yes' ELSE 'No' END      AS `Any VOB for this patient`,
  sim_actual_date                                  AS `Sim Date`,
  sim_sched_date                                   AS `Sim Scheduled Date`,
  -- [11 Sep] Empty unless the column to its left says No AND this patient DOES have
  -- a counted VOB at this account on a different biopsy date. Stops a therapist
  -- chasing an already-verified patient, without touching the conversion grain.
  CASE WHEN vobs = 0 AND vobs_any_date > 0
       THEN concat('VOB on ', vob_biopsy_dates)
       ELSE NULL END                               AS `Patient VOB (other date)`,
  -- [11 Sep] Deliberately NOT suppressed when the row has its own VOB: a patient
  -- verified twice inside 90 days is exactly who you want to see.
  CASE WHEN prior_vobs > 0
       THEN concat(date_format(last_prior_vob, 'yyyy-MM-dd'), '  (',
                   cast(datediff(biopsy_dt, last_prior_vob) AS STRING),
                   'd before this biopsy',
                   CASE WHEN prior_vobs > 1
                        THEN concat(', ', cast(prior_vobs AS STRING), ' VOBs')
                        ELSE '' END, ')')
       ELSE NULL END                               AS `Previous VOB (90d)`
FROM weekly_full
ORDER BY `Account`, `Date`, `Patient (masked)`, `Anatomical location`
""")
sheet.createOrReplaceTempView("weekly_sheet")
display(sheet)

_n = sheet.count()
_c = len(sheet.columns)
print(f"{_n:,} rows x {_c} columns")
assert _c == 20, f"expected 20 columns, got {_c}"
_blank = spark.sql("SELECT count(*) c FROM weekly_sheet WHERE `Path Status` = 'Unknown'").first()["c"]
if _blank:
    print(f"  {_blank:,} rows read Path Status 'Unknown' — the file name matched none of "
          f"completed / pending completion / pending plan / pending / unresolved. "
          f"Check the naming on this week's export.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b · Read the No's before believing them
# MAGIC
# MAGIC A `no VOB found` on a three-week-old biopsy is not a lost patient — it is an
# MAGIC unanswered question. **This week's pull runs 24 Aug – 3 Sep**, so nearly every
# MAGIC September row will sit in `still_open`. That is the sheet working, not a
# MAGIC conversion problem.

# COMMAND ----------

# reads weekly_full, not the report -- the report deliberately drops these columns
display(spark.sql("""
SELECT account_resolved AS Account, vob_match, window_closed,
       count(*)                        AS lesions,
       count(DISTINCT patient_name)    AS patients,
       round(avg(days_observed), 0)    AS avg_days_observed
FROM weekly_full GROUP BY 1,2,3 ORDER BY Account, vob_match, window_closed
"""))

_m = spark.sql(f"""
SELECT sum(CASE WHEN vob_match = 'no VOB found' AND window_closed = 'No'  THEN 1 ELSE 0 END) AS still_open,
       sum(CASE WHEN vob_match = 'no VOB found' AND window_closed = 'Yes' THEN 1 ELSE 0 END) AS closed_no_vob,
       sum(CASE WHEN vob_match <> 'no VOB found' THEN 1 ELSE 0 END)                          AS verified,
       count(*) AS lesions
FROM weekly_full WHERE lesion_appropriate
""").first()
print(f"appropriate lesions: {_m['lesions']:,}")
print(f"  verified                        {_m['verified']:,}")
print(f"  no VOB, window still open       {_m['still_open']:,}  <- too early to call")
print(f"  no VOB, {GAP} days elapsed        {_m['closed_no_vob']:,}  <- these are the real gap")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c · Schaberg / Alton — the open question
# MAGIC
# MAGIC Its files are named `maryville alton completed 1.pdf` etc., but every row the
# MAGIC monthly run produced — and every row of the 9 Sep weekly pull — carries facility
# MAGIC `Schaberg Dermatology - Maryville`. Not one says Alton.
# MAGIC
# MAGIC - **Alton rows appear** → the pooled figures are symmetric. Nothing to do.
# MAGIC - **No Alton rows** → the VOB numerator sums Maryville + Alton while the
# MAGIC   appropriate/marked denominator is Maryville only, so **Marked → VOB is
# MAGIC   overstated**: 42% pooled against 24% on Maryville's own VOBs.

# COMMAND ----------

display(spark.sql(f"""
SELECT coalesce(nullif(trim(facility),''), '(blank)') AS facility,
       count(*) AS row_count, count(DISTINCT file_name) AS files,
       collect_set(account_name) AS folders
FROM {C}.raw.path_log_rows
WHERE lower(coalesce(facility, ''))     LIKE '%schaberg%'
   OR lower(coalesce(account_name, '')) LIKE '%schaberg%'
GROUP BY 1 ORDER BY 2 DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d · One row per lesion, with the note history from EVERY copy
# MAGIC
# MAGIC The case this handles: a patient shows on *pending completion* in one run and on
# MAGIC *completed* in the next. Two rows for one lesion, and the Action / Plan, Comments,
# MAGIC Notifications and Notes may have moved on between them. Here the four text fields
# MAGIC read as the full history, oldest stage first, joined by `   ||   ` and stamped
# MAGIC with the stage each version appeared at:
# MAGIC
# MAGIC `[Pending Completion] Referred for SRT, awaiting VOB   ||   [Completed] VOB received 9/2`
# MAGIC
# MAGIC **One deliberate departure from "keep the latest."** Which copy survives is decided
# MAGIC by the pipeline's own rule — `srt_marked DESC, vob_mentioned DESC, has_actions DESC`,
# MAGIC then file and page — not by recency. `Lesion_Dedup_Rule_Centralised_31Aug2026`
# MAGIC measured why on the Apr–Jul corpus: **371 lesions across 77 accounts carry the SRT
# MAGIC mark on ONE copy only**, and a file-order tie-break loses all 371, because a
# MAGIC practice publishing a pending log then a completed one puts the *Referred for SRT*
# MAGIC copy on the later page. Polley Roanoke Rapids reported zero marked patient-days in
# MAGIC August for exactly that reason.
# MAGIC
# MAGIC So the two halves of "latest" are split apart:
# MAGIC
# MAGIC - `Appropriate`, `SRT`, `VOB mentioned in log` — a Yes on ANY copy is a Yes here.
# MAGIC - `Path Status` — the furthest-along stage the lesion reached, which is what
# MAGIC   "latest" is meant to mean.
# MAGIC - `Path Status (all copies)` — the progression itself, `Pending Completion -> Completed`.
# MAGIC - `Copies Disagree On` — non-empty ONLY when the copies contradicted each other.
# MAGIC   That column *is* the 371-lesion class, made visible.
# MAGIC
# MAGIC The lesion key is the pipeline's verbatim, and it includes **pathology**. A lesion
# MAGIC whose RESULT changed between runs will not collapse. Drop
# MAGIC `lower(coalesce(pathology, ''))` from the three partition/group lists to merge
# MAGIC them — but then two different lesions at the same body site on the same day merge too.
# MAGIC
# MAGIC **Real patient names, unmasked.** Weekly view only — the monthly pull keeps one row
# MAGIC per lesion with no history.

# COMMAND ----------

# The whole loaded window by default. Set to '2026-08-24' for this week's pull only.
H0 = W0

history = spark.sql(f"""
WITH weekly AS (
  SELECT *, biopsy_dt AS biopsy_date FROM weekly_lesions
  WHERE biopsy_dt >= date('{H0}')
),
ranked AS (
  SELECT w.*,
    row_number() OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))
      ORDER BY w.srt_marked DESC, w.vob_mentioned DESC, w.has_actions DESC,
               w.file_name, w.page_no)                AS rn,
    count(*) OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))) AS copies,
    max(w.status_rank) OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))) AS best_status_rank,
    -- max <> min means one copy knew and another did not
    max(CASE WHEN w.srt_marked         THEN 1 ELSE 0 END) OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))) AS srt_max,
    min(CASE WHEN w.srt_marked         THEN 1 ELSE 0 END) OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))) AS srt_min,
    max(CASE WHEN w.lesion_appropriate THEN 1 ELSE 0 END) OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))) AS appr_max,
    min(CASE WHEN w.lesion_appropriate THEN 1 ELSE 0 END) OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))) AS appr_min,
    max(CASE WHEN w.vob_mentioned      THEN 1 ELSE 0 END) OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))) AS vobm_max,
    min(CASE WHEN w.vob_mentioned      THEN 1 ELSE 0 END) OVER (
      PARTITION BY w.account_resolved, w.patient_id, w.biopsy_date,
                   lower(coalesce(w.body_site, '')), lower(coalesce(w.pathology, ''))) AS vobm_min
  FROM weekly w
),
-- One row per (lesion, field, DISTINCT text). Identical text repeated across copies
-- collapses to a single entry stamped with the EARLIEST stage it appeared at, so an
-- unchanged note reads once rather than twice.
occurrences AS (
  SELECT account_resolved, patient_id, biopsy_date,
         lower(coalesce(body_site, '')) AS bs, lower(coalesce(pathology, '')) AS pa,
         'Action / Plan' AS field, trim(actions) AS txt, status_rank, path_status
  FROM weekly WHERE trim(coalesce(actions, '')) <> ''
  UNION ALL
  SELECT account_resolved, patient_id, biopsy_date,
         lower(coalesce(body_site, '')), lower(coalesce(pathology, '')),
         'Comments', trim(comments), status_rank, path_status
  FROM weekly WHERE trim(coalesce(comments, '')) <> ''
  UNION ALL
  SELECT account_resolved, patient_id, biopsy_date,
         lower(coalesce(body_site, '')), lower(coalesce(pathology, '')),
         'Notifications', trim(notifications), status_rank, path_status
  FROM weekly WHERE trim(coalesce(notifications, '')) <> ''
  UNION ALL
  SELECT account_resolved, patient_id, biopsy_date,
         lower(coalesce(body_site, '')), lower(coalesce(pathology, '')),
         'Notes', trim(notes), status_rank, path_status
  FROM weekly WHERE trim(coalesce(notes, '')) <> ''
),
first_seen AS (
  SELECT account_resolved, patient_id, biopsy_date, bs, pa, field, txt,
         min(status_rank)                 AS first_rank,
         min_by(path_status, status_rank)  AS first_status
  FROM occurrences
  GROUP BY account_resolved, patient_id, biopsy_date, bs, pa, field, txt
),
hist AS (
  SELECT account_resolved, patient_id, biopsy_date, bs, pa, field,
         count(*)                                     AS versions,
         array_join(
           transform(sort_array(collect_list(struct(first_rank, txt, first_status))),
                     x -> concat('[', x.first_status, '] ', x.txt)),
           '   ||   ')                                AS combined
  FROM first_seen
  GROUP BY account_resolved, patient_id, biopsy_date, bs, pa, field
),
histp AS (
  SELECT account_resolved, patient_id, biopsy_date, bs, pa,
         max(CASE WHEN field = 'Action / Plan' THEN combined END) AS actions_all,
         max(CASE WHEN field = 'Comments'      THEN combined END) AS comments_all,
         max(CASE WHEN field = 'Notifications' THEN combined END) AS notifications_all,
         max(CASE WHEN field = 'Notes'         THEN combined END) AS notes_all,
         sum(versions)                                           AS text_versions
  FROM hist
  GROUP BY account_resolved, patient_id, biopsy_date, bs, pa
),
statuses AS (
  SELECT account_resolved, patient_id, biopsy_date,
         lower(coalesce(body_site, '')) AS bs, lower(coalesce(pathology, '')) AS pa,
         array_join(transform(sort_array(collect_list(struct(status_rank, path_status))),
                              x -> x.path_status), ' -> ')        AS status_path
  FROM (SELECT DISTINCT account_resolved, patient_id, biopsy_date, body_site, pathology,
                        status_rank, path_status FROM weekly)
  GROUP BY account_resolved, patient_id, biopsy_date,
           lower(coalesce(body_site, '')), lower(coalesce(pathology, ''))
),
daycount AS (
  SELECT account_resolved, patient_id, biopsy_date,
         count(DISTINCT concat_ws('|', lower(coalesce(body_site, '')),
                                       lower(coalesce(pathology, '')))) AS n_lesions
  FROM weekly
  GROUP BY account_resolved, patient_id, biopsy_date
),
vob AS (
  SELECT patient_id, biopsy_date,
         count(DISTINCT vob_id)               AS vob_count,
         concat_ws(', ', collect_set(vob_id)) AS vob_ids,
         -- [10 Sep] scheduled and attended, same reason as cell 4
         max(CASE WHEN simulation_scheduled IS NOT NULL THEN 1 ELSE 0 END) = 1 AS sim_scheduled,
         max(CASE WHEN simulation_actual    IS NOT NULL THEN 1 ELSE 0 END) = 1 AS sim_attended,
         min(to_date(simulation_scheduled))      AS sim_sched_date,
         min(to_date(simulation_actual))         AS sim_actual_date
  FROM {C}.staged.vob_match
  WHERE matched AND matched_folder IS NOT NULL
  GROUP BY patient_id, biopsy_date
),
anyname AS (
  SELECT DISTINCT lower(trim(patient_name_raw)) AS nm
  FROM {C}.raw.vob
  WHERE trim(coalesce(patient_name_raw, '')) <> ''
)
SELECT
    CASE k.best_status_rank WHEN 5 THEN 'Completed'
                            WHEN 4 THEN 'Pending Completion'
                            WHEN 3 THEN 'Pending Plan'
                            WHEN 2 THEN 'Pending'
                            WHEN 1 THEN 'Unresolved'
                            ELSE        'Unknown' END  AS `Path Status`,
    st.status_path                                     AS `Path Status (all copies)`,
    k.copies                                           AS `Copies`,
    k.account_resolved                                 AS `Account`,
    k.facility                                         AS `Facility`,
    date_format(k.biopsy_date, 'MMMM')                 AS `Month Name`,
    cast(floor((dayofmonth(k.biopsy_date) - 1) / 7) + 1 AS INT) AS `Week`,
    k.biopsy_date                                      AS `BiopsyDate`,
    k.result_dt                                        AS `ResultDate`,
    k.patient_name                                     AS `Patient Name`,
    dc.n_lesions                                       AS `Lesion Identified`,
    k.body_site                                        AS `Anatomical Location (Lesion)`,
    k.pathology                                        AS `Result`,
    coalesce(h.actions_all,       k.actions)           AS `Action / Plan`,
    coalesce(h.comments_all,      k.comments)          AS `Comments / Assigned To`,
    coalesce(h.notifications_all, k.notifications)     AS `Notifications`,
    coalesce(h.notes_all,         k.notes)             AS `Notes`,
    k.flags                                            AS `Flags`,
    CASE WHEN k.appr_max = 1 THEN 'Yes' ELSE 'No' END  AS `Appropriate`,
    CASE WHEN k.srt_max  = 1 THEN 'Yes' ELSE 'No' END  AS `SRT`,
    CASE WHEN coalesce(v.vob_count, 0) > 0 THEN 'Yes'
         WHEN a.nm IS NOT NULL             THEN 'Name on a VOB, out of window'
         ELSE                                   'No' END  AS `Any VOB`,
    v.vob_ids                                          AS `VOB #`,
    v.sim_sched_date                                   AS `Sim Scheduled`,
    v.sim_actual_date                                  AS `Simulation on`,
    CASE WHEN v.sim_attended AND v.sim_scheduled              THEN 'showed'
         WHEN v.sim_attended                                  THEN 'sim, no schedule recorded'
         WHEN v.sim_scheduled AND v.sim_sched_date >= current_date()
                                                              THEN 'scheduled, not yet due'
         WHEN v.sim_scheduled                                 THEN 'NO SHOW'
         WHEN coalesce(v.vob_count, 0) > 0                    THEN 'not scheduled'
         ELSE NULL END                                  AS `Sim outcome`,
    k.path_class                                       AS `Path Class`,
    CASE WHEN k.vobm_max = 1 THEN 'Yes' ELSE 'No' END  AS `VOB mentioned in log`,
    nullif(concat_ws(', ',
      CASE WHEN k.srt_max  <> k.srt_min  THEN 'SRT'         END,
      CASE WHEN k.appr_max <> k.appr_min THEN 'Appropriate' END,
      CASE WHEN k.vobm_max <> k.vobm_min THEN 'VOB mention' END), '')
                                                       AS `Copies Disagree On`,
    h.text_versions                                    AS `Text Versions`,
    k.rules_matched                                    AS `Rules Matched`
FROM ranked k
LEFT JOIN histp    h ON h.account_resolved = k.account_resolved
                    AND h.patient_id       = k.patient_id
                    AND h.biopsy_date      = k.biopsy_date
                    AND h.bs               = lower(coalesce(k.body_site, ''))
                    AND h.pa               = lower(coalesce(k.pathology, ''))
LEFT JOIN statuses st ON st.account_resolved = k.account_resolved
                     AND st.patient_id       = k.patient_id
                     AND st.biopsy_date      = k.biopsy_date
                     AND st.bs               = lower(coalesce(k.body_site, ''))
                     AND st.pa               = lower(coalesce(k.pathology, ''))
LEFT JOIN daycount dc ON dc.account_resolved = k.account_resolved
                     AND dc.patient_id       = k.patient_id
                     AND dc.biopsy_date      = k.biopsy_date
LEFT JOIN vob      v ON v.patient_id = k.patient_id AND v.biopsy_date = k.biopsy_date
LEFT JOIN anyname  a ON a.nm = lower(trim(k.patient_name))
WHERE k.rn = 1                                        -- <<< the surviving copy
ORDER BY `Account`, `BiopsyDate`, `Patient Name`, `Anatomical Location (Lesion)`
""")
history.createOrReplaceTempView("weekly_history")
display(history)

# COMMAND ----------

# The two things worth reading off it: which lesions actually MOVED between runs, and
# which ones the copies contradicted each other about.
display(spark.sql("""
SELECT `Account`,
       count(*)                                                        AS lesions,
       sum(CASE WHEN `Copies` > 1 THEN 1 ELSE 0 END)                   AS seen_twice,
       sum(CASE WHEN `Text Versions` > 1 THEN 1 ELSE 0 END)            AS notes_changed,
       sum(CASE WHEN `Copies Disagree On` IS NOT NULL THEN 1 ELSE 0 END) AS copies_disagree,
       sum(CASE WHEN `Copies Disagree On` LIKE '%SRT%' THEN 1 ELSE 0 END) AS srt_on_one_copy_only,
       -- [10 Sep] the simulation funnel: scheduled -> showed, and who did not
       sum(CASE WHEN `Sim outcome` = 'showed'                THEN 1 ELSE 0 END) AS showed,
       sum(CASE WHEN `Sim outcome` = 'NO SHOW'               THEN 1 ELSE 0 END) AS no_show,
       sum(CASE WHEN `Sim outcome` = 'scheduled, not yet due' THEN 1 ELSE 0 END) AS upcoming,
       sum(CASE WHEN `Sim outcome` = 'sim, no schedule recorded' THEN 1 ELSE 0 END) AS sim_unscheduled,
       -- denominator is showed + no_show ONLY. `upcoming` is excluded deliberately:
       -- a patient whose date has not arrived has neither shown nor failed to.
       round(100.0 * sum(CASE WHEN `Sim outcome` = 'showed' THEN 1 ELSE 0 END)
             / nullif(sum(CASE WHEN `Sim outcome` IN ('showed','NO SHOW')
                               THEN 1 ELSE 0 END), 0), 0)                       AS show_rate_pct
FROM weekly_history GROUP BY 1 ORDER BY 1
"""))
# no_show is the actionable one: the practice scheduled the patient and they never
# came in. That is a different conversation from a practice that never scheduled --
# and note VOB -> Sim on Pam's Summary counts SCHEDULED, so it already credits these.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Write it out
# MAGIC
# MAGIC The 18-column report from cell 4, one CSV. `weekly_full` is a temp view and
# MAGIC is deliberately NOT written — it carries the same PHI with more columns and
# MAGIC no one asked for it.
# MAGIC
# MAGIC **This file is PHI.** Masking a name is not anonymising it — a masked name with an
# MAGIC account, a date and a body site identifies a person. Keep it in the volume,
# MAGIC download to a controlled machine, and do not attach it to a message or a ticket.

# COMMAND ----------

import datetime as _dt
_stamp = _dt.date.today().isoformat()
_path = f"/Volumes/{C}/gold/exports/weekly_report_{_stamp}.csv"
(sheet.coalesce(1).write.mode("overwrite").option("header", True).csv(_path + ".dir"))
print(f"wrote {_path}.dir")