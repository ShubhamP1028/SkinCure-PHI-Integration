# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — review views: path log ⟷ VOB, patient by patient

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
# Added 28 Aug: staged.patient_days now holds every month the pipeline has
# computed, so this notebook has to say which one it is reviewing.
dbutils.widgets.text("window_start", "2026-05-01", "Window start (inclusive)")
dbutils.widgets.text("window_end",   "2026-09-01", "Window end (exclusive)")
C  = dbutils.widgets.get("catalog")
W0 = dbutils.widgets.get("window_start")
W1 = dbutils.widgets.get("window_end")
print(f"catalog {C} · window {W0} .. {W1}")
DAYS = f"{C}.staged.patient_days"

# ---- WINDOW SCOPE --------------------------------------------------------------. See wiki.
_DAYS_TABLE = DAYS
_DAYS_PRED  = f"biopsy_date >= date('{W0}') AND biopsy_date < date('{W1}')"
DAYS = f"(SELECT * FROM {_DAYS_TABLE} WHERE {_DAYS_PRED})"

_held = spark.table(_DAYS_TABLE).count()
_dw = spark.sql(
    "SELECT count(*) AS n, count(DISTINCT date_format(biopsy_date,'yyyy-MM')) AS m "
    f"FROM {_DAYS_TABLE} WHERE {_DAYS_PRED}").first()
print(f"patient-days: {_dw['n']:,} in window across {_dw['m']} month(s), "
      f"{_held:,} held in total")

VOBM = f"{C}.staged.vob_match"
print(f"{_DAYS_TABLE}: {_dw['n']:,} patient-days in window")
print(f"{VOBM}: {spark.table(VOBM).count():,} VOBs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The vocabulary notebook

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------

# The scope CASE and the window test, bound to this notebook's aliases and widgets.
_scope  = vob_scope_case_sql("v")
_inwin  = vob_in_window_sql(W0, W1, "v")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `staged.appropriate_patients` — the one to review

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {C}.staged.appropriate_patients AS
SELECT
  d.account_name,
  d.patient_id,
  d.patient_name,
  d.biopsy_date,
  d.month_label,
  d.result_date,
  concat_ws(' | ', d.facilities)              AS facility,
  d.marked,
  d.vob_mentioned                             AS vob_mentioned_in_log,
  d.has_actions,
  d.n_lesions,
  d.keyed_on_mrn,
  -- the VOB side; NULL means no VOB was found for this patient-day
  v.vob_id,
  v.patient_name_raw                          AS vob_patient_name,
  v.vob_created,
  v.status                                    AS vob_status,
  v.simulation_scheduled,
  v.gap_days                                  AS days_biopsy_to_vob,
  (v.vob_id IS NOT NULL)                      AS has_vob,
  -- group_has_sim, not simulation_scheduled. The row that survives the §6 collapse
  -- is the EARLIEST verification, and the simulation may sit on the later one --
  -- 240 of 310 duplicate groups contain a sim, so reading the survivor's own
  -- column verbatim would delete real conversions from this sheet.
  coalesce(v.group_has_sim, false)            AS converted_to_sim,
  -- how many verifications this biopsy produced, so a collapsed duplicate is
  -- visible rather than looking like a VOB that went missing
  coalesce(v.vobs_for_this_biopsy, 0)         AS vobs_for_this_biopsy,
  CASE WHEN v.vob_id IS NULL          THEN 'no VOB found'
       WHEN v.account_id = m.account_id
        AND m.role = 'primary'                THEN 'VOB on primary ID'
       ELSE 'VOB on a pooled ID' END          AS vob_source,
  -- [21 Aug] the three nested scopes (notebook 04 §6a). `vob_own_site` is the same
  -- fact as `has_vob` above, restated inside the funnel so the three read together;
  -- the assertion below proves they never disagree. NULL means no VOB anywhere, so
  -- coalesce to false rather than letting a NULL read as "unknown" in a filter.
  coalesce(s.vob_own_site,     false)         AS vob_at_own_site,
  coalesce(s.vob_sharing_site, false)         AS vob_at_a_sharing_site,
  coalesce(s.vob_enterprise,   false)         AS vob_anywhere_in_enterprise,
  coalesce(s.n_enterprise_folders, 0)         AS other_folders_with_a_vob,
  s.vob_at_other_folders,
  -- what the widening actually buys, as one readable label
  CASE WHEN coalesce(s.vob_own_site, false)                        THEN '1 own site'
       WHEN coalesce(s.vob_sharing_site, false)                     THEN '2 sibling site'
       WHEN coalesce(s.vob_enterprise, false)                       THEN '3 elsewhere in enterprise'
       ELSE                                                             '4 no VOB anywhere'
  END                                         AS vob_scope
FROM {DAYS} d
LEFT JOIN {VOBM} v
       ON v.matched_folder = d.account_name
      AND v.patient_id     = d.patient_id
      AND v.biopsy_date    = d.biopsy_date
      AND v.matched
      -- [26 Aug] §6. WITHOUT THIS THE JOIN FANS OUT: a patient with two matched
      -- verifications for one biopsy produced TWO rows for one patient-day, so
      -- the tab carried 52,492 rows against 52,177 real patient-days and its own
      -- row count contradicted the summary above it. The excluded row is the
      -- later verification; `vobs_for_this_biopsy` above says it existed and
      -- `converted_to_sim` carries its simulation if it had one.
      AND NOT v.is_duplicate_patient_vob
LEFT JOIN {C}.raw.account_map m
       ON m.account_id = v.account_id
LEFT JOIN {C}.staged.patient_vob_scope s
       ON s.account_name = d.account_name
      AND s.patient_id   = d.patient_id
      AND s.biopsy_date  = d.biopsy_date
WHERE d.appropriate
""")

# has_vob comes from the vob_match join, vob_at_own_site from the scope roll-up. They
# are the same fact by two routes, so a disagreement means one of the two joins is
# wrong — and since the report counts has_vob while people will read vob_scope, a
# disagreement would show as two different answers to one question.
_dis = spark.sql(f"""
SELECT count(*) AS n FROM {C}.staged.appropriate_patients
WHERE has_vob <> vob_at_own_site
""").first()["n"]
assert _dis == 0, (
    f"{_dis:,} appropriate patient-days where has_vob and vob_at_own_site disagree. "
    "Both describe a VOB owned by this folder; one of the two joins is wrong.")
print("has_vob and vob_at_own_site agree on every row")

# ---- GRAIN ASSERTION ------------------------------------------------------. See wiki.
_g = spark.sql(f"""
SELECT count(*)                                                      AS rows,
       count(DISTINCT concat_ws('|', account_name, patient_id,
                                cast(biopsy_date AS STRING)))        AS patient_days
FROM {C}.staged.appropriate_patients
""").first()
assert _g["rows"] == _g["patient_days"], (
    f"appropriate_patients holds {_g['rows']:,} rows for {_g['patient_days']:,} "
    f"patient-days -- {_g['rows'] - _g['patient_days']:,} rows too many. The join to "
    "vob_match has fanned out: a patient-day matched more than one verification and "
    "`AND NOT v.is_duplicate_patient_vob` is missing or not doing its job.")
print(f"grain OK: {_g['rows']:,} rows = {_g['patient_days']:,} patient-days")

_dupv = spark.sql(f"""
SELECT sum(CASE WHEN vobs_for_this_biopsy > 1 THEN 1 ELSE 0 END) AS collapsed,
       sum(CASE WHEN vobs_for_this_biopsy > 1 AND converted_to_sim
                THEN 1 ELSE 0 END)                               AS collapsed_with_sim
FROM {C}.staged.appropriate_patients WHERE has_vob
""").first()
print(f"  {_dupv['collapsed'] or 0:,} patient-days produced more than one verification; "
      f"{_dupv['collapsed_with_sim'] or 0:,} of those reached simulation")
print("  (filter vobs_for_this_biopsy > 1 to review them)")

print("created", f"{C}.staged.appropriate_patients")

display(spark.sql(f"""
SELECT account_name,
       count(*)                                                AS appropriate_days,
       sum(CASE WHEN marked THEN 1 ELSE 0 END)                 AS marked,
       sum(CASE WHEN has_vob THEN 1 ELSE 0 END)                AS with_vob,
       sum(CASE WHEN NOT has_vob THEN 1 ELSE 0 END)            AS NO_vob,
       sum(CASE WHEN marked AND NOT has_vob THEN 1 ELSE 0 END) AS marked_but_no_vob,
       sum(CASE WHEN converted_to_sim THEN 1 ELSE 0 END)       AS to_sim,
       round(100.0 * avg(CASE WHEN has_vob THEN 1.0 ELSE 0.0 END), 0) AS appr_to_vob_pct,
       percentile_approx(days_biopsy_to_vob, 0.5)              AS median_days,
       -- [21 Aug] the same denominator, three scopes wide
       sum(CASE WHEN vob_at_a_sharing_site AND NOT vob_at_own_site
                THEN 1 ELSE 0 END)                             AS vob_at_sibling_only,
       sum(CASE WHEN vob_anywhere_in_enterprise AND NOT vob_at_a_sharing_site
                THEN 1 ELSE 0 END)                             AS vob_outside_practice_only,
       round(100.0 * avg(CASE WHEN vob_at_a_sharing_site THEN 1.0 ELSE 0.0 END), 0)
                                                               AS appr_to_vob_sharing_pct,
       sum(CASE WHEN marked AND NOT vob_anywhere_in_enterprise
                THEN 1 ELSE 0 END)                             AS marked_no_vob_anywhere
FROM {C}.staged.appropriate_patients
GROUP BY account_name ORDER BY account_name
"""))
print("`marked_but_no_vob` is the therapist's work queue: the practice flagged SRT")
print("interest and no verification of benefits followed.")
print()
print("`marked_no_vob_anywhere` is the STRICTER queue — no VOB at this site, at a")
print("sibling, or anywhere else in the enterprise. Where the two differ, the patient")
print("was verified somewhere; that is a reporting question, not a call to make.")

# COMMAND ----------

print(f"catalog = {C}")

display(spark.sql(f"""
SELECT has_vob, vob_at_own_site, count(*) AS n
FROM {C}.staged.appropriate_patients
WHERE has_vob <> vob_at_own_site
GROUP BY has_vob, vob_at_own_site
ORDER BY n DESC
"""))

display(spark.sql(f"""
SELECT account_name,
       count(*) AS n,
       sum(CASE WHEN has_vob AND NOT vob_at_own_site THEN 1 ELSE 0 END) AS match_only,
       sum(CASE WHEN vob_at_own_site AND NOT has_vob THEN 1 ELSE 0 END) AS scope_only
FROM {C}.staged.appropriate_patients
WHERE has_vob <> vob_at_own_site
GROUP BY account_name
ORDER BY n DESC
LIMIT 25
"""))

for t in ("vob_match", "patient_vob_scope", "patient_days"):
    last = spark.sql(f"DESCRIBE HISTORY {C}.staged.{t}").select("timestamp", "operation").first()
    print(f"{t:22} last written {last['timestamp']}  ({last['operation']})")

# COMMAND ----------

for t in ("vob_match", "patient_vob_scope", "vob_candidates",
          "vob_leak_detail", "vob_leak_review"):
    try:
        n = spark.table(f"{C}.staged.{t}").count()
        print(f"{t:22} {n:>10,}{'   *** EMPTY ***' if n == 0 else ''}")
    except Exception as e:
        print(f"{t:22} {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Review one account, row by row

# COMMAND ----------

dbutils.widgets.text("account", "", "Account to review (blank = the largest)")
_a = dbutils.widgets.get("account").strip()
if not _a:
    _a = spark.sql(f"""SELECT account_name FROM {C}.staged.appropriate_patients
                       GROUP BY account_name ORDER BY count(*) DESC LIMIT 1""").first()[0]
print("reviewing:", _a)

display(spark.sql(f"""
SELECT biopsy_date, patient_name, facility, marked, vob_mentioned_in_log,
       has_vob, vob_patient_name, vob_created, days_biopsy_to_vob,
       vob_status, simulation_scheduled, vob_source
FROM {C}.staged.appropriate_patients
WHERE account_name = '{_a.replace("'", "''")}'
ORDER BY has_vob, biopsy_date
"""))
print("Sorted with the NO-VOB rows first — those are the ones worth reading.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Spot-check the name matches that DID happen

# COMMAND ----------

display(spark.sql(f"""
SELECT account_name, patient_name, vob_patient_name, biopsy_date, vob_created,
       days_biopsy_to_vob, vob_status
FROM {C}.staged.appropriate_patients
WHERE has_vob
  AND regexp_replace(lower(patient_name),     '[^a-z]', '')
   <> regexp_replace(lower(vob_patient_name), '[^a-z]', '')
ORDER BY abs(days_biopsy_to_vob) DESC
LIMIT 300
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `staged.vob_mentioned_no_vob` — sheet 5

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {C}.staged.vob_mentioned_no_vob AS
SELECT d.account_name, d.patient_id, d.patient_name, d.biopsy_date, d.facility,
       d.marked, d.n_lesions, d.result_date,
       -- [26 Aug] THE EVIDENCE, not just the assertion. Counting rules §11 says a
       -- log mentioning a VOB does not mean one was submitted -- "we will obtain a
       -- VOB if she elects treatment" is a conditional, not a verification -- and
       -- asks that mentions be graded. This sheet gave the flag and no way to
       -- grade it, so every row had to be looked up by hand in the raw log.
       --
       -- `vob_mention_fields`  which of the six searched columns held the phrase
       -- `vob_mention_snippet` ~110 characters either side of the match, labelled
       --                       with the column it came from
       -- `vob_mention_raw`     the full searched text of the mentioning lesions
       p.vob_mention_fields,
       p.vob_mention_snippet,
       p.vob_mention_raw
FROM {C}.staged.appropriate_patients d
JOIN {DAYS} p
  ON  p.account_name = d.account_name
  AND p.patient_id   = d.patient_id
  AND p.biopsy_date  = d.biopsy_date
WHERE d.vob_mentioned_in_log AND NOT d.has_vob
""")

_n = spark.table(f"{C}.staged.vob_mentioned_no_vob").count()
print(f"{C}.staged.vob_mentioned_no_vob: {_n:,} patient-days")

# The join to patient_days is on the full patient-day key, so it must not change
# the row count. If it does, patient_days is not unique on that key and every
# count in the pipeline is suspect, not just this sheet.
_base = spark.sql(f"""
SELECT count(*) AS n FROM {C}.staged.appropriate_patients
WHERE vob_mentioned_in_log AND NOT has_vob
""").first()["n"]
assert _n == _base, (
    f"joining the evidence columns changed the row count ({_base:,} -> {_n:,}). "
    "patient_days is not unique on (account_name, patient_id, biopsy_date).")

# Evidence has to actually arrive, or the sheet silently reverts to what it was.
_ev = spark.sql(f"""
SELECT sum(CASE WHEN vob_mention_snippet IS NULL
                  OR vob_mention_snippet = '' THEN 1 ELSE 0 END) AS no_snippet
FROM {C}.staged.vob_mentioned_no_vob
""").first()["no_snippet"]
if _ev:
    print(f"  !! {_ev} of {_n} rows carry no snippet. Every row here was flagged BY "
          f"that pattern, so an empty snippet means 03's extract and 02's flag are "
          f"using different vocabularies -- check the %run in notebook 03.")
else:
    print(f"  all {_n} rows carry the text that triggered the flag")
if _n:
    display(spark.sql(f"""
      SELECT account_name, count(*) AS mentions_with_no_vob
      FROM {C}.staged.vob_mentioned_no_vob
      GROUP BY account_name ORDER BY mentions_with_no_vob DESC
    """))
    display(spark.table(f"{C}.staged.vob_mentioned_no_vob").orderBy("account_name", "biopsy_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reconciliation — the numbers must tie

# COMMAND ----------

_a1 = spark.sql(f"SELECT count(*) c FROM {DAYS} d WHERE d.appropriate").first()["c"]
_a2 = spark.table(f"{C}.staged.appropriate_patients").count()
_v1 = spark.sql(f"SELECT count(*) c FROM {VOBM} WHERE matched").first()["c"]
_v2 = spark.sql(f"SELECT count(DISTINCT vob_id) c FROM {C}.staged.appropriate_patients WHERE has_vob").first()["c"]

print(f"appropriate patient-days   in patient_days : {_a1:,}")
print(f"rows in appropriate_patients               : {_a2:,}")
print(f"  {'MATCH' if _a1 == _a2 else '*** FAN-OUT — a patient-day matched 2+ VOBs ***'}")
print()
print(f"matched VOBs in vob_match                  : {_v1:,}")
print(f"distinct VOBs visible in this review       : {_v2:,}")
print(f"  {'MATCH' if _v1 == _v2 else 'differ — matched VOBs whose patient-day is NOT appropriate (see below)'}")

if _a1 != _a2:
    display(spark.sql(f"""
      SELECT account_name, patient_id, biopsy_date, count(*) AS vobs_on_this_day,
             collect_list(vob_id) AS vob_ids
      FROM {C}.staged.appropriate_patients WHERE has_vob
      GROUP BY account_name, patient_id, biopsy_date HAVING count(*) > 1
      ORDER BY vobs_on_this_day DESC LIMIT 100
    """))
    print("§6 calls this 'Duplicate patient' — the 2nd and later matched VOB for the same")
    print("patient at the same account, keeping the earliest. Decide the rule before")
    print("reporting a conversion built on it.")

if _v1 != _v2:
    display(spark.sql(f"""
      SELECT v.matched_folder, v.vob_id, v.patient_id, v.biopsy_date, v.marked
      FROM {VOBM} v
      LEFT JOIN {DAYS} d
             ON d.account_name = v.matched_folder AND d.patient_id = v.patient_id
            AND d.biopsy_date = v.biopsy_date AND d.appropriate
      WHERE v.matched AND d.patient_id IS NULL LIMIT 100
    """))
    print("These VOBs matched a patient-day that is NOT appropriate — cleared pathology,")
    print("or dropped by the facility rule. Expected, but worth knowing the size of.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `staged.vob_disposition` — every VOB in exactly one bucket

# COMMAND ----------

# The name-reachability test — the same one behind the 1,487 analysis. A VOB whose
# name shares no token with any patient-day in its own folder was unreachable, and
# separating those from "name found but the dates do not line up" is the difference
# between a collection problem and a matching problem.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _vd_reach AS
WITH vt AS (
  SELECT DISTINCT vob_id, vob_folder, tok FROM (
    SELECT vob_id, vob_folder,
           explode(split(regexp_replace(lower(coalesce(patient_name_raw, '')),
                         '[^a-z ]', ' '), ' +')) AS tok
      FROM {VOBM}
     WHERE vob_folder IS NOT NULL AND NOT matched AND NOT is_leak)
   WHERE length(tok) > 1
),
dt AS (
  SELECT DISTINCT account_name, tok FROM (
    SELECT account_name,
           explode(split(regexp_replace(lower(coalesce(patient_name, '')),
                         '[^a-z ]', ' '), ' +')) AS tok
      FROM {DAYS} pd)
   WHERE length(tok) > 1
)
SELECT DISTINCT v.vob_id
  FROM vt v JOIN dt d ON d.tok = v.tok AND d.account_name = v.vob_folder
""")

# A PERSISTENT TABLE, not a temp view. See wiki.
spark.sql(f"""
CREATE OR REPLACE TABLE {C}.staged.vob_bucket USING DELTA AS
WITH {leaked_to_sister_sql(C, VOBM)}
SELECT
  coalesce(v.vob_folder, v.matched_folder, '(no folder mapped)') AS account_name,
  v.vob_id,
  -- [31 Aug] THE BUCKETS NOW COME FROM THE SHARED SCOPE RULE (00_vocab),
  -- the same one gold.vob_scope uses. What they replace, and why:
  --
  --   '2. Matched a sibling' was `matched AND matched_folder <> vob_folder`, which the
  --   matcher can never produce -- it sets `matched` only for a biopsy at the
  --   verification's OWN folder. The bucket read zero on all 184 accounts and looked
  --   like a finding. A real sister match is found through leak_folders and the
  --   practice group: 159 of them in this window.
  --
  --   '6. Ambiguous namesake' counted every verification carrying a namesake-flagged
  --   CANDIDATE rather than those whose winning match was ambiguous -- 598 against
  --   221 -- and pulled rows out of the cross-site buckets on the way past.
  --
  -- Duplicate stays tested FIRST and stays exclusive: a second verification for the
  -- same patient-day is real, but counting it again would double the practice.
  CASE
    WHEN v.is_duplicate_patient_vob                     THEN '7. Duplicate patient'
    WHEN {_scope} = 'own site'                          THEN '1. Matched own site'
    WHEN {_scope} = 'sister site'                       THEN '2. Matched a sibling'
    WHEN {_scope} = 'enterprise'                        THEN '3. Matched elsewhere (enterprise)'
    WHEN {_scope} = 'ambiguous namesake'                THEN '6. Ambiguous namesake'
    -- The scope rule stops at 'no biopsy found'. Splitting that on name reachability
    -- is this sheet's own contribution and the reason the bucket table still exists:
    -- "we hold no patient of that name" is a collection problem, "the name is here but
    -- no biopsy lines up" is a matching problem, and they get fixed by different people.
    WHEN r.vob_id IS NULL                               THEN '5. No patient of that name'
    ELSE                                                     '4. Name found, no biopsy in range'
  END                                                   AS bucket,
  -- Placed by biopsy month when we have one, by creation month when we do not.
  {_inwin}                                              AS in_window,
  v.cohort_biopsy_date,
  v.vob_created,
  (v.simulation_scheduled IS NOT NULL)                  AS reached_sim
FROM {VOBM} v
LEFT JOIN lk           ON lk.vob_id = v.vob_id
LEFT JOIN _vd_reach r  ON r.vob_id  = v.vob_id
""")
print(f"wrote {C}.staged.vob_bucket: "
      f"{spark.table(f'{C}.staged.vob_bucket').count():,} VOBs, one bucket each")

# The sheet is the WINDOW's verifications, not the whole pool. Before 31 Aug this
# view counted every row in vob_bucket -- 36,910 -- under a header naming a four-month
# window that holds 8,471, so every rate computed against it was meaningless.
spark.sql(f"""
CREATE OR REPLACE VIEW {C}.staged.vob_disposition AS
WITH b AS (SELECT * FROM {C}.staged.vob_bucket WHERE in_window),
piv AS (
  SELECT account_name,
         count(*)                                                          AS total_vob,
         sum(CASE WHEN bucket LIKE '1.%' THEN 1 ELSE 0 END)                AS matched_own_site,
         sum(CASE WHEN bucket LIKE '2.%' THEN 1 ELSE 0 END)                AS matched_a_sibling,
         sum(CASE WHEN bucket LIKE '3.%' THEN 1 ELSE 0 END)                AS matched_elsewhere,
         sum(CASE WHEN bucket LIKE '4.%' THEN 1 ELSE 0 END)                AS name_found_no_biopsy_in_range,
         sum(CASE WHEN bucket LIKE '5.%' THEN 1 ELSE 0 END)                AS no_patient_of_that_name,
         sum(CASE WHEN bucket LIKE '6.%' THEN 1 ELSE 0 END)                AS ambiguous_namesake,
         sum(CASE WHEN bucket LIKE '7.%' THEN 1 ELSE 0 END)                AS duplicate_patient,
         sum(CASE WHEN reached_sim THEN 1 ELSE 0 END)                      AS reached_simulation
    FROM b GROUP BY account_name
),
ment AS (
  SELECT account_name, count(*) AS vob_mentioned_no_vob_found
    FROM {C}.staged.vob_mentioned_no_vob GROUP BY account_name
)
SELECT p.account_name,
       p.total_vob,
       p.matched_own_site, p.matched_a_sibling, p.matched_elsewhere,
       p.name_found_no_biopsy_in_range, p.no_patient_of_that_name,
       p.ambiguous_namesake, p.duplicate_patient,
       (p.matched_own_site + p.matched_a_sibling + p.matched_elsewhere
        + p.name_found_no_biopsy_in_range + p.no_patient_of_that_name
        + p.ambiguous_namesake + p.duplicate_patient)                      AS buckets_sum,
       p.reached_simulation,
       coalesce(m.vob_mentioned_no_vob_found, 0)                           AS vob_mentioned_no_vob_found
  FROM piv p LEFT JOIN ment m ON m.account_name = p.account_name
""")

_d = spark.sql(f"""
SELECT count(*) AS accounts, sum(total_vob) AS vobs, sum(buckets_sum) AS bucketed,
       sum(CASE WHEN buckets_sum <> total_vob THEN 1 ELSE 0 END) AS mismatched_accounts
FROM {C}.staged.vob_disposition
""").first()
print(f"wrote {C}.staged.vob_disposition: {_d['accounts']:,} accounts, {_d['vobs']:,} VOBs")

# THE ASSERTION IS THE POINT. A disposition whose buckets do not sum to the total is
# worse than no disposition -- it looks authoritative while hiding whatever fell
# between the CASE branches. Checked per account, not just in aggregate, because two
# accounts erring in opposite directions cancel in a portfolio total.
assert _d["mismatched_accounts"] == 0, (
    f"{_d['mismatched_accounts']} accounts have buckets that do not sum to their total "
    f"({_d['bucketed']:,} bucketed vs {_d['vobs']:,} VOBs) — a VOB is falling between "
    "the CASE branches, or being counted in two.")
print("  buckets are exclusive and sum to the total on every account")

display(spark.sql(f"""
SELECT bucket, count(*) AS vobs,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)                  AS pct,
       sum(CASE WHEN reached_sim THEN 1 ELSE 0 END)                        AS reached_sim,
       round(100.0 * avg(CASE WHEN reached_sim THEN 1.0 ELSE 0.0 END), 1)  AS sim_rate_pct
FROM {C}.staged.vob_bucket GROUP BY bucket ORDER BY bucket
"""))
print("Read the sim rate down the buckets. 'Matched own site' should be the highest —")
print("if 'No patient of that name' converts just as well, those are real treated")
print("patients our path logs never saw, and that is a collection finding, not a")
print("matching one.")