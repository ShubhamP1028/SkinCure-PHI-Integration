# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — build the VOB source views

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("sf_case", "us_scdb_prod.sc_salesforce_raw.case", "Salesforce case table")
# [10 Sep, Becky] The treatment log carries the SIMULATION field. Joined on
# ptl.VOB_Case__c = case.Id. Pre-aggregated to one row per case before it is
# joined, because the log is one-to-MANY per VOB -- see section 2b.
dbutils.widgets.text("sf_ptl", "us_scdb_prod.sc_salesforce_raw.patient_treatment_log__c",
                     "Patient treatment log table")
dbutils.widgets.text("sf_acct_gold", "us_scdb_prod.comm_gold.rpt_account_dim", "Account dim (gold)")
dbutils.widgets.text("sf_acct_silver", "us_scdb_prod.comm_silver.dim_account_dtl", "Account detail (silver)")
dbutils.widgets.text("vob_from", "2026-01-01", "VOB pull from (inclusive)")
# Over-broad costs nothing; too narrow silently loses matches. The matcher applies its. See wiki.
dbutils.widgets.text("vob_to", "2026-10-02", "VOB pull to (exclusive)")
# [31 Aug] Was hardcoded '2026-07-01' inside the account_dim filter, which silently
# aged as the window rolled: with the window at Apr-Aug, an account terminated in
# July 2026 was live for the whole reporting period and still dropped off the sheet.
# Set it to the window start.
dbutils.widgets.text("terminated_after", "2026-04-01",
                     "Keep accounts terminated on or after (window start)")
# [26 Aug, Becky] The case table holds several record types; only one is a VOB.
# A WIDGET rather than a literal because this filter can only ever REMOVE rows --
# a wrong id here silently empties the pool, and an empty pool looks exactly like
# a practice that stopped verifying. The cell below reports what it removed.
dbutils.widgets.text("vob_record_type", "0123h000000VHSlAAO", "Case RecordTypeId for VOBs")
# [26 Aug, Becky] STATUSES THAT ARE NOT VERIFICATIONS AT ALL. See wiki.
dbutils.widgets.text("exclude_status", "unprocessed|experimental",
                     "Status values to drop entirely (regex, blank = none)")

C = dbutils.widgets.get("catalog")
TERM_AFTER = dbutils.widgets.get("terminated_after")
CASE = dbutils.widgets.get("sf_case")
PTL = dbutils.widgets.get("sf_ptl")
AG = dbutils.widgets.get("sf_acct_gold")
AS_ = dbutils.widgets.get("sf_acct_silver")
V0 = dbutils.widgets.get("vob_from")
V1 = dbutils.widgets.get("vob_to")
RT = dbutils.widgets.get("vob_record_type").strip()
EXCL_STATUS = dbutils.widgets.get("exclude_status").strip()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.raw")
print(f"case table : {CASE}")
print(f"VOB pull   : {V0} .. {V1}")
print("  (deliberately wider than the Mar–Jun biopsy window — it is a MATCHING pool,")
print("   not a counting window. §1 of the counting rules.)")
print(f"record type: {RT or '(no filter)'}")

# What the record-type filter costs, measured before it is applied. A filter that
# removes everything and a filter that removes nothing both look like success in
# a row count; only the split shows which one you have.
if RT:
    display(spark.sql(f"""
    SELECT RecordTypeId,
           count(DISTINCT Id)                                        AS cases,
           sum(CASE WHEN Patient_Name_Text__c IS NOT NULL
                    THEN 1 ELSE 0 END)                               AS with_patient_name,
           CASE WHEN RecordTypeId = '{RT}' THEN '<-- KEPT' ELSE 'dropped' END AS effect
    FROM {CASE}
    WHERE CreatedDate >= '{V0}' AND CreatedDate < '{V1}'
    GROUP BY RecordTypeId ORDER BY cases DESC
    """))
    _rt = spark.sql(f"""
    SELECT count(DISTINCT CASE WHEN RecordTypeId = '{RT}' THEN Id END) AS kept,
           count(DISTINCT Id)                                          AS total
    FROM {CASE}
    WHERE CreatedDate >= '{V0}' AND CreatedDate < '{V1}'
      AND Patient_Name_Text__c IS NOT NULL
    """).first()
    print(f"  named cases kept by the record-type filter: "
          f"{_rt['kept']:,} of {_rt['total']:,}")
    if EXCL_STATUS:
        display(spark.sql(f"""
        SELECT Status__c,
               count(DISTINCT Id)                                          AS cases,
               CASE WHEN lower(trim(regexp_replace(
                      translate(Status__c, '–—', '--'), '  +', ' ')))
                      RLIKE '{EXCL_STATUS}'
                    THEN '<-- DROPPED' ELSE 'kept' END                      AS effect
        FROM {CASE}
        WHERE CreatedDate >= '{V0}' AND CreatedDate < '{V1}'
          AND Patient_Name_Text__c IS NOT NULL
        GROUP BY 1, 3 ORDER BY cases DESC
        """))
        print(f"  dropping statuses matching: {EXCL_STATUS}")
        print("  DENIED and CANCELLED are deliberately NOT here -- those are real")
        print("  verifications that failed, and they belong in the denominator.")

    assert _rt["kept"] > 0, (
        f"RecordTypeId '{RT}' matches NO cases in the pull window. The pool would be "
        "empty and every conversion would read zero. Check the id against the table "
        "above before re-running.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Can we see it, and what is in it?

# COMMAND ----------

print(f"rows in {CASE}: {spark.table(CASE).count():,}")
display(spark.sql(f"""
SELECT count(*) AS rows_in_window,
       count(DISTINCT Id) AS distinct_ids,
       count(DISTINCT AccountId) AS accounts,
       min(CreatedDate) AS earliest, max(CreatedDate) AS latest
FROM {CASE}
WHERE CreatedDate >= '{V0}' AND CreatedDate < '{V1}'
"""))
print("For reference, 01_Counting_Rules 5.2 records 45,341 rows collapsing to 33,740")
print("distinct VOBs on the previous pull. A large move needs an explanation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The three corrections, measured before they are applied

# COMMAND ----------

print("--- issue 3: VOBs the old branch-1 filter silently dropped ---")
display(spark.sql(f"""
SELECT count(*) AS cases_with_null_cell_type_1,
       count(DISTINCT Id) AS distinct_ids_affected
FROM {CASE}
WHERE CreatedDate >= '{V0}' AND CreatedDate < '{V1}'
  AND Patient_Name_Text__c IS NOT NULL
  AND Cell_Type_1__c IS NULL
"""))

print("--- issue 2: what does Cell Type actually contain? ---")
display(spark.sql(f"""
SELECT Cell_Type_1__c, count(*) AS n
FROM {CASE}
WHERE CreatedDate >= '{V0}' AND CreatedDate < '{V1}'
GROUP BY 1 ORDER BY n DESC LIMIT 40
"""))

print("--- §7: Treatment_Status__c was reported as a single value with no information ---")
display(spark.sql(f"""
SELECT Treatment_Status__c, Master_Treatment_Status__c, count(*) AS n
FROM {CASE}
WHERE CreatedDate >= '{V0}' AND CreatedDate < '{V1}'
GROUP BY 1, 2 ORDER BY n DESC LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2b. The treatment-log grain — measure it, then join safely
# MAGIC
# MAGIC [10 Sep, Becky] `Simulation__c` lives on `patient_treatment_log__c`, joined
# MAGIC `ptl.VOB_Case__c = case.Id`, and it is a **DATE** — the day the patient actually
# MAGIC came in, as against `case.Patient_Simulation_Scheduled__c`, the day they were
# MAGIC meant to.
# MAGIC
# MAGIC ### What the first run of this cell found
# MAGIC
# MAGIC ```
# MAGIC 213,277  log rows
# MAGIC 213,211  distinct VOB cases
# MAGIC      37  rows with NO VOB_Case__c   -> never join, harmless
# MAGIC ----------------------------------------------------------
# MAGIC 213,240  rows that DO carry a case id
# MAGIC      29  EXCESS rows                -> a few cases have two log rows
# MAGIC ```
# MAGIC
# MAGIC So the design is 1:1 and Becky is right about the intent — the log is wide, one
# MAGIC record per case carrying many visits as columns. But it is not 1:1 in the data:
# MAGIC **29 rows over the 213,211 cases**. Small, and exactly the size of problem that
# MAGIC never announces itself.
# MAGIC
# MAGIC ### Why the earlier assertion was wrong, not the data
# MAGIC
# MAGIC It read `max(count(*) OVER (PARTITION BY VOB_Case__c)) = 37` and failed. But 37
# MAGIC is the NULL count exactly: `PARTITION BY` puts every NULL `VOB_Case__c` in **one**
# MAGIC partition, so the guard tripped on rows that can never join. NULLs are excluded
# MAGIC below and the check now measures the real duplicates.
# MAGIC
# MAGIC ### The join
# MAGIC
# MAGIC Section 3 joins an inline `GROUP BY VOB_Case__c` rather than the table directly.
# MAGIC That is **provably** one row per case, so the 29 duplicates cannot fan
# MAGIC `raw.vob_sites` — which is already 4× from the lateral `stack` — out to 8 rows and
# MAGIC inflate `count(*) AS n_sites`. No separate view: three lines in the FROM clause.
# MAGIC
# MAGIC `min()` because the column is a date and the earliest simulation is the event —
# MAGIC the same convention `simulation_scheduled` already uses.

# COMMAND ----------

# The column names first. A wrong name fails loudly wherever it is used; a wrong
# CARDINALITY does not, which is the one worth a query.
_ptl_cols = {f.name.lower(): f.dataType.simpleString() for f in spark.table(PTL).schema.fields}
print(f"{PTL}: {len(_ptl_cols)} columns")
for _c in ("simulation__c", "vob_case__c"):
    print(f"  {'ok  ' if _c in _ptl_cols else 'MISSING'} {_c:<16} {_ptl_cols.get(_c, '')}")
_sim_col = "Simulation__c"
if "simulation__c" not in _ptl_cols:
    raise KeyError(f"Simulation__c not on {PTL}. Candidates: "
                   f"{[c for c in _ptl_cols if 'sim' in c]}")
if not _ptl_cols["simulation__c"].startswith(("date", "timestamp")):
    print(f"  *** Simulation__c is {_ptl_cols['simulation__c']}, not a date. §6b's no-show "
          f"test compares the SCHEDULED date to current_date() and needs this to be a "
          f"date to mean anything. ***")

# NULLs excluded -- a NULL VOB_Case__c never joins, and PARTITION BY would lump them
# all into one partition and report their COUNT as though it were one case's fan-out.
_p = spark.sql(f"""
SELECT count(*)                                              AS rows_with_a_case,
       count(DISTINCT VOB_Case__c)                           AS distinct_cases,
       count(*) - count(DISTINCT VOB_Case__c)                AS excess_rows,
       max(_n)                                               AS max_rows_one_case,
       sum(CASE WHEN _n > 1 THEN 1 ELSE 0 END)               AS rows_in_dup_cases,
       count({_sim_col})                                     AS sim_populated
FROM (SELECT *, count(*) OVER (PARTITION BY VOB_Case__c) AS _n
      FROM {PTL} WHERE VOB_Case__c IS NOT NULL)
""").first()
_null = spark.sql(f"SELECT count(*) c FROM {PTL} WHERE VOB_Case__c IS NULL").first()["c"]

print(f"  {_p['rows_with_a_case']:,} rows carry a case id · {_p['distinct_cases']:,} distinct cases")
print(f"  {_null:,} rows have no VOB_Case__c — they never join")
print(f"  {_p['sim_populated']:,} rows have Simulation__c populated")
print(f"  EXCESS rows: {_p['excess_rows']:,} (worst case has {_p['max_rows_one_case']} rows)")
if _p["excess_rows"]:
    print(f"  -> NOT strictly 1:1. Section 3's inline GROUP BY absorbs this; without it "
          f"{_p['rows_in_dup_cases']:,} rows would fan raw.vob_sites out.")
    display(spark.sql(f"""
    SELECT VOB_Case__c                            AS case_id,
           count(*)                               AS log_rows,
           count(DISTINCT {_sim_col})             AS distinct_sim_dates,
           min({_sim_col})                        AS earliest,
           max({_sim_col})                        AS latest,
           datediff(max({_sim_col}), min({_sim_col})) AS days_apart
    FROM {PTL} WHERE VOB_Case__c IS NOT NULL
    GROUP BY VOB_Case__c HAVING count(*) > 1
    ORDER BY log_rows DESC, days_apart DESC NULLS LAST LIMIT 50
    """))
    # distinct_sim_dates > 1 means min() is CHOOSING. Read days_apart: a one-day gap is
    # a duplicated record, a large one is two genuine simulations under one VOB case and
    # is worth raising with Salesforce rather than silently taking the earlier.
else:
    print("  -> strictly 1:1")

# HARD STOP only on a fan-out the GROUP BY cannot absorb -- i.e. never, by construction.
# Kept as a shape check on the subquery itself.
_agg = spark.sql(f"""
SELECT count(*) AS rows, count(DISTINCT case_id) AS cases FROM (
  SELECT VOB_Case__c AS case_id, min({_sim_col}) AS simulation_actual
  FROM {PTL} WHERE VOB_Case__c IS NOT NULL GROUP BY VOB_Case__c)
""").first()
assert _agg["rows"] == _agg["cases"], "the pre-aggregate is not 1:1 — impossible; check the SQL"
print(f"  join source is 1:1 — {_agg['cases']:,} cases")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `raw.vob_sites` — unpivot the four lesion slots

# COMMAND ----------

SIM = _sim_col
spark.sql(f"""
CREATE OR REPLACE VIEW {C}.raw.vob_sites AS
SELECT
  c.AccountId, c.Id, c.Status__c, c.Patient_Name_Text__c, c.CreatedDate,
  c.Master_Treatment_Status__c, c.Account_Source__c,
  c.Patient_Simulation_Scheduled__c, c.Authorization_Needed__c,
  -- [10 Sep, Becky] the SIMULATION field off the treatment log -- the date the patient
  -- ACTUALLY came in. One PTL record per VOB case by design, but 2b measured 29 excess
  -- rows over 213,211 cases, so the join goes through a GROUP BY: provably one row per
  -- case, and the 29 cannot fan these four lesion slots out to eight. LEFT so a case
  -- with no treatment log keeps its slots. min() = the earliest simulation, the same
  -- convention simulation_scheduled uses.
  p.simulation_actual,
  s.site_no, s.anatomical_location, s.cell_type
FROM {CASE} c
LEFT JOIN (
  SELECT VOB_Case__c AS case_id, min({SIM}) AS simulation_actual
  FROM {PTL} WHERE VOB_Case__c IS NOT NULL GROUP BY VOB_Case__c
) p ON p.case_id = c.Id
LATERAL VIEW stack(4,
  1, c.Anatomical_Location_1__c, c.Cell_Type_1__c,
  2, c.Anatomical_Location_2__c, c.Cell_Type_2__c,
  3, c.Anatomical_Location_3__c, c.Cell_Type_3__c,
  4, c.Anatomical_Location_4__c, c.Cell_Type_4__c
) s AS site_no, anatomical_location, cell_type
WHERE c.CreatedDate >= '{V0}' AND c.CreatedDate < '{V1}'
  AND c.Patient_Name_Text__c IS NOT NULL
  AND (s.anatomical_location IS NOT NULL OR s.cell_type IS NOT NULL)
  {"AND c.RecordTypeId = '" + RT + "'" if RT else ""}
  {"AND NOT lower(trim(regexp_replace(translate(c.Status__c, '–—', '--'), '  +', ' '))) RLIKE '" + EXCL_STATUS + "'" if EXCL_STATUS else ""}
""")
print("created", f"{C}.raw.vob_sites")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `raw.vob_sites_nonkeloid` — keloid out at RECORD level

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {C}.raw.vob_sites_nonkeloid AS
SELECT s.*
FROM {C}.raw.vob_sites s
WHERE s.Id NOT IN (
  SELECT DISTINCT Id FROM {C}.raw.vob_sites
  WHERE lower(cell_type) LIKE '%keloid%'
)
""")

display(spark.sql(f"""
SELECT
  (SELECT count(DISTINCT Id) FROM {C}.raw.vob_sites)            AS ids_before_keloid,
  (SELECT count(DISTINCT Id) FROM {C}.raw.vob_sites_nonkeloid)  AS ids_after_keloid,
  (SELECT count(DISTINCT Id) FROM {C}.raw.vob_sites)
    - (SELECT count(DISTINCT Id) FROM {C}.raw.vob_sites_nonkeloid) AS ids_removed
"""))
print("ids_removed is the size of correction 1 + 2. Under the old per-slot filter, any")
print("case whose slot 1 was Keloid but slot 2 was not would have survived.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Is `min()` safe? — prove it before aggregating

# COMMAND ----------

_incon = spark.sql(f"""
SELECT count(*) AS ids_with_inconsistent_header FROM (
  SELECT Id FROM {C}.raw.vob_sites_nonkeloid GROUP BY Id
  HAVING count(DISTINCT Patient_Name_Text__c) > 1
      OR count(DISTINCT CreatedDate) > 1
      OR count(DISTINCT Status__c) > 1
      -- [10 Sep] simulation_actual joins in at case grain, so it must be constant across
      -- the four slots too. >1 here IS the PTL fan-out, caught at the point where it
      -- would start corrupting min().
      OR count(DISTINCT simulation_actual) > 1
)
""").first()["ids_with_inconsistent_header"]

print(f"Ids where the header fields are NOT constant: {_incon}")
if _incon:
    print("  *** min() is picking arbitrarily. The aggregation below needs rethinking ***")
    display(spark.sql(f"""
      SELECT Id, count(DISTINCT Patient_Name_Text__c) AS names,
             count(DISTINCT CreatedDate) AS dates, count(DISTINCT Status__c) AS statuses,
             count(DISTINCT simulation_actual) AS sims
      FROM {C}.raw.vob_sites_nonkeloid GROUP BY Id
      HAVING names > 1 OR dates > 1 OR statuses > 1 OR sims > 1 LIMIT 50
    """))
else:
    print("  §5.2 holds — min() is safe.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `raw.vob` — one row per VOB

# COMMAND ----------

# §7 bands, matched on a NORMALISED status rather than a literal list. See wiki.
_NORM = ("lower(trim(regexp_replace("
         "translate({c}, '\u2013\u2014', '--'), '\\\\s+', ' ')))")

RX_B = r"denied|cancell?ed|unprocessed|experimental"     # terminal negative
RX_C = r"^complete"                                      # completed, anything else
RX_A = r"pending|^new$|working|escalated|returned to vob rep|peer review|^alert"

def band_sql(col):
    n = _NORM.format(c=col)
    return f"""CASE
      WHEN {n} RLIKE '{RX_B}' THEN 'B'
      WHEN {n} RLIKE '{RX_C}' THEN 'C'
      WHEN {n} RLIKE '{RX_A}' THEN 'A'
      ELSE 'UNMAPPED' END"""

spark.sql(f"""
CREATE OR REPLACE VIEW {C}.raw.vob AS
SELECT
  Id                                        AS vob_id,
  min(AccountId)                            AS account_id,
  min(Patient_Name_Text__c)                 AS patient_name_raw,
  min(CreatedDate)                          AS vob_created,
  min(Status__c)                            AS status,
  {band_sql("min(Status__c)")}             AS status_band,
  min(Master_Treatment_Status__c)           AS master_treatment_status,
  min(Patient_Simulation_Scheduled__c)      AS simulation_scheduled,
  -- [10 Sep, Becky] ADDITIVE. `simulation_scheduled` above is untouched, and it is
  -- still what `converted`, `group_has_sim` and `reached_sim` read — see §6b before
  -- changing that. min() here only undoes the 4-slot lateral fan: PTL is 1:1 per case,
  -- so the value is constant across the four slots and min is exact, not a choice.
  -- §5 proves that constancy rather than assuming it.
  min(simulation_actual)                       AS simulation_actual,
  min(Account_Source__c)                    AS account_source,
  min(Authorization_Needed__c)              AS authorization_needed,
  count(*)                                  AS n_sites,
  collect_set(anatomical_location)          AS anatomical_locations,
  collect_set(cell_type)                    AS cell_types
FROM {C}.raw.vob_sites_nonkeloid
GROUP BY Id
""")

print(f"{C}.raw.vob: {spark.table(f'{C}.raw.vob').count():,} distinct VOBs")

display(spark.sql(f"""
SELECT status_band, status,
       count(*) AS vobs,
       sum(CASE WHEN simulation_scheduled IS NOT NULL THEN 1 ELSE 0 END) AS to_sim,
       round(100.0 * avg(CASE WHEN simulation_scheduled IS NOT NULL THEN 1.0 ELSE 0.0 END), 0) AS sim_rate_pct
FROM {C}.raw.vob GROUP BY status_band, status ORDER BY status_band, vobs DESC
"""))
# §7 defines the bands by simulation rate, so check the bands against it rather than
# trusting the rules. If a band's sim rate falls outside its documented range, the
# classification is wrong even when nothing is UNMAPPED.
_bands = spark.sql(f"""
SELECT status_band,
       count(*)                                                           AS vobs,
       sum(CASE WHEN simulation_scheduled IS NOT NULL THEN 1 ELSE 0 END)   AS to_sim,
       round(100.0 * avg(CASE WHEN simulation_scheduled IS NOT NULL
                              THEN 1.0 ELSE 0.0 END), 1)                   AS sim_rate_pct
FROM {C}.raw.vob GROUP BY status_band ORDER BY status_band
""")
display(_bands)

_RANGE = {"A": (0, 20), "B": (2, 30), "C": (45, 64)}
_rows = {r["status_band"]: r for r in _bands.collect()}

_un = _rows.get("UNMAPPED")
if _un:
    print(f"*** {_un['vobs']:,} VOBs UNMAPPED — {100*_un['vobs']/spark.table(f'{C}.raw.vob').count():.1f}% "
          f"of the total, at a {_un['sim_rate_pct']}% sim rate ***")
    display(spark.sql(f"""
      SELECT status, count(*) AS vobs FROM {C}.raw.vob
      WHERE status_band = 'UNMAPPED' GROUP BY 1 ORDER BY vobs DESC
    """))
    print("Each of these needs a rule in RX_A / RX_B / RX_C above. An unbanded VOB is")
    print("not excluded from anything -- it is silently in an undefined category.")
else:
    print("every Status__c value is banded")

for _b, (_lo, _hi) in _RANGE.items():
    _r = _rows.get(_b)
    if not _r:
        print(f"band {_b}: no VOBs")
        continue
    _ok = _lo <= _r["sim_rate_pct"] <= _hi
    print(f"band {_b}: {_r['vobs']:>6,} VOBs, sim {_r['sim_rate_pct']:>5.1f}%  "
          f"(§7 expects {_lo}-{_hi}%)  {'OK' if _ok else '*** OUTSIDE THE DOCUMENTED RANGE'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6b. Scheduled vs actual — the no-show read
# MAGIC
# MAGIC **[10 Sep, Becky] These are two STAGES, not two definitions of one thing.**
# MAGIC
# MAGIC | field | source | means |
# MAGIC |---|---|---|
# MAGIC | `simulation_scheduled` | `case.Patient_Simulation_Scheduled__c` | the date the patient is **supposed to come in** |
# MAGIC | `simulation_actual` | `patient_treatment_log__c.Simulation__c` | the simulation **happened** — they came in |
# MAGIC
# MAGIC So they should normally match, and the gap between them is the useful number:
# MAGIC **patients who were scheduled and never came in.**
# MAGIC
# MAGIC ### [10 Sep, Becky] 07b ALREADY PULLS THE ATTENDED DATE. Read this first.
# MAGIC
# MAGIC `07b_allaccounts_rollup` joins the treatment log itself — `ptl.sim AS sim_date`
# MAGIC off `us_scdb_prod.comm_silver`, filtered `WHERE sim_date IS NOT NULL` — and that
# MAGIC is the ATTENDED simulation. It flows:
# MAGIC
# MAGIC ```
# MAGIC 07b  ptl.sim  ->  raw.account_rollup.sim  ->  gold.sf_rollup.sf_sim_<Mon>
# MAGIC      ->  Pam's Summary SIM band  ->  the regional cards
# MAGIC ```
# MAGIC
# MAGIC **So the SIM numbers on the cards are already attended simulations, not
# MAGIC scheduled ones, and always were.** An earlier note here claimed otherwise. It
# MAGIC was wrong.
# MAGIC
# MAGIC What IS on the scheduled basis is the **ratio**: `_vob_monthly.simm_*` in 08
# MAGIC reads `vob_match.group_has_sim`, which reads `simulation_scheduled`. That feeds
# MAGIC VOB → Sim and `current_sims_per_month`.
# MAGIC
# MAGIC | number | source | basis | cohorted on |
# MAGIC |---|---|---|---|
# MAGIC | SIM band / `TotalVOB Converted` | 07b → `gold.sf_rollup` | **attended** | the sim month |
# MAGIC | VOB → Sim, `current_sims_per_month` | `vob_match.group_has_sim` | **scheduled** | the biopsy month |
# MAGIC
# MAGIC Two bases and two cohort rules, side by side on one sheet. They cannot reconcile
# MAGIC by construction, and that — not a missing field — is the thing to fix.
# MAGIC
# MAGIC `simulation_actual` here is the RAW equivalent of what 07b already aggregates.
# MAGIC It earns its place only because 07b is account × month: it cannot say WHICH
# MAGIC patient failed to come in, and the weekly sheet needs patient grain. Treat the
# MAGIC cells below as a reconciliation against 07b, not as a new source of truth.
# MAGIC
# MAGIC ### The four buckets
# MAGIC
# MAGIC | bucket | reading |
# MAGIC |---|---|
# MAGIC | `showed` | scheduled and simulated. The clean path |
# MAGIC | `scheduled_not_yet` | a scheduled date still in the future — **not a no-show**, just early |
# MAGIC | `no_show` | the scheduled date has passed and no simulation was logged. **This is the number** |
# MAGIC | `sim_no_schedule` | simulated with nothing scheduled — a data-entry gap, not a win |
# MAGIC | `never_scheduled` | no simulation activity at all |
# MAGIC
# MAGIC **`scheduled_not_yet` is why a raw two-way comparison misleads.** Without the
# MAGIC date test every recently-scheduled patient reads as a no-show, and the more
# MAGIC recent the pull, the worse it looks. It is the same maturity trap as
# MAGIC `Window closed` on the weekly sheet.

# COMMAND ----------

_c = spark.sql(f"""
WITH b AS (
  SELECT
    to_date(simulation_scheduled) AS sched,
    to_date(simulation_actual)    AS actual
  FROM {C}.raw.vob
)
SELECT
  count(*)                                                             AS vobs,
  sum(CASE WHEN sched IS NOT NULL AND actual IS NOT NULL
           THEN 1 ELSE 0 END)                                          AS showed,
  -- scheduled, nothing logged, and the date has NOT arrived yet. Early, not lost.
  sum(CASE WHEN sched IS NOT NULL AND actual IS NULL
            AND sched >= current_date() THEN 1 ELSE 0 END)             AS scheduled_not_yet,
  -- scheduled, the date has passed, still no simulation. THE NUMBER.
  sum(CASE WHEN sched IS NOT NULL AND actual IS NULL
            AND sched <  current_date() THEN 1 ELSE 0 END)             AS no_show,
  sum(CASE WHEN sched IS     NULL AND actual IS NOT NULL
           THEN 1 ELSE 0 END)                                          AS sim_no_schedule,
  sum(CASE WHEN sched IS     NULL AND actual IS     NULL
           THEN 1 ELSE 0 END)                                          AS never_scheduled,
  -- how far apart the two dates run, where both exist. A large spread means the
  -- scheduled date is being set once and not maintained.
  round(avg(CASE WHEN sched IS NOT NULL AND actual IS NOT NULL
                 THEN datediff(actual, sched) END), 1)                 AS avg_days_sched_to_sim
FROM b
""").first()

_due = _c["showed"] + _c["no_show"]        # only patients whose date has arrived
print(f"{_c['vobs']:,} VOBs")
print(f"  showed                    {_c['showed']:,}")
print(f"  scheduled, not yet due    {_c['scheduled_not_yet']:,}   <- NOT no-shows, just early")
print(f"  NO SHOW                   {_c['no_show']:,}   <- scheduled, date passed, never came in")
print(f"  sim, no schedule recorded {_c['sim_no_schedule']:,}   <- data-entry gap")
print(f"  never scheduled           {_c['never_scheduled']:,}")
print(f"  avg days scheduled -> sim {_c['avg_days_sched_to_sim']}")
if _due:
    print(f"\n  show rate {100.0 * _c['showed'] / _due:.1f}% of the {_due:,} whose date has arrived")
    print(f"  VOB -> Sim today counts SCHEDULED. On ATTENDED it would be "
          f"{_c['showed'] + _c['sim_no_schedule']:,} instead of "
          f"{_c['showed'] + _c['no_show'] + _c['scheduled_not_yet']:,}.")

# COMMAND ----------

# Per band. §7's _RANGE is calibrated on sim_pct_scheduled; sim_pct_attended is what
# to recalibrate against if the definition is ever changed. A band whose two columns
# diverge sharply is a band where scheduling and attendance are not the same thing.
display(spark.sql(f"""
SELECT status_band,
       count(*)                                                                   AS vobs,
       round(100.0 * avg(CASE WHEN simulation_scheduled IS NOT NULL THEN 1.0 ELSE 0.0 END), 1) AS sim_pct_scheduled,
       round(100.0 * avg(CASE WHEN simulation_actual    IS NOT NULL THEN 1.0 ELSE 0.0 END), 1) AS sim_pct_attended,
       sum(CASE WHEN to_date(simulation_scheduled) <  current_date()
                 AND simulation_actual IS NULL THEN 1 ELSE 0 END)                 AS no_shows
FROM {C}.raw.vob GROUP BY status_band ORDER BY status_band
"""))

# The no-shows by account, which is the version anyone can act on. Practice-level,
# no patient identifiers.
#
# RECONCILE THIS AGAINST 07b. raw.account_rollup.sim is the same fact aggregated from
# comm_silver; this is it from sc_salesforce_raw. If the two disagree, the silver
# layer and the raw layer are not carrying the same simulations and 07b is the one
# the cards trust -- so believe 07b and find out why raw differs, not the reverse.
display(spark.sql(f"""
SELECT v.account_id,
       coalesce(d.account_name, v.account_id)              AS account,
       count(*)                                            AS vobs,
       sum(CASE WHEN v.simulation_scheduled IS NOT NULL
                 AND v.simulation_actual    IS NOT NULL THEN 1 ELSE 0 END) AS showed,
       sum(CASE WHEN to_date(v.simulation_scheduled) <  current_date()
                 AND v.simulation_actual IS NULL THEN 1 ELSE 0 END)        AS no_show,
       sum(CASE WHEN to_date(v.simulation_scheduled) >= current_date()
                 AND v.simulation_actual IS NULL THEN 1 ELSE 0 END)        AS upcoming
FROM {C}.raw.vob v
LEFT JOIN {C}.raw.account_dim d ON d.account_id = v.account_id
GROUP BY v.account_id, d.account_name
HAVING no_show > 0
ORDER BY no_show DESC
LIMIT 40
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `raw.account_dim` — Regional / Senior, and the practice-group source

# COMMAND ----------

# raw.account_map is built by notebook 06, which runs before this one -- but not on a
# first-ever run, and this view must still build then. Absent, the join is a no-op and
# the filter behaves exactly as it did before today.
if spark.catalog.tableExists(f"{C}.raw.account_map"):
    _MAPPED_JOIN = (f"LEFT JOIN (SELECT DISTINCT account_id FROM {C}.raw.account_map) mp "
                    f"ON mp.account_id = g.account_id")
else:
    _MAPPED_JOIN = "LEFT JOIN (SELECT CAST(NULL AS STRING) AS account_id) mp ON false"
    print("  raw.account_map does not exist yet — the mapped-account exemption is off "
          "for this run. Re-run 07 after 06 to pick it up.")

spark.sql(f"""
CREATE OR REPLACE VIEW {C}.raw.account_dim AS
SELECT
  g.account_id,
  g.account_name,
  g.senior_regional_name,
  g.regional_manager_name,
  g.Account_Transition,
  g.go_live_date,
  g.status,
  s.terminated_date,
  -- §5.6: text before the FIRST dash, matching group_alias.py. Named aliases only;
  -- a general normalisation was tried and rejected because it merged
  -- "Dermatology Associates - Savannah" (GA) with "..., Inc - Winchester" (VA).
  trim(replace(lower(regexp_replace(g.account_name,
       '\\\\s*-\\\\s*(?=[A-Z]).*$|\\\\s+-\\\\s+.*$', '')), ' & ', ' and ')) AS practice_group
FROM {AG} g
LEFT JOIN {AS_} s ON s.account_id = g.account_id
{_MAPPED_JOIN}
WHERE g.status IN ('Active', 'Terminated')
  -- [31 Aug] IF WE HOLD A PATH LOG FOR IT, IT BELONGS IN THE DIMENSION.
  --
  -- Both guards below now yield to `mapped`, and Anchorage is why. Advanced
  -- Dermatology of Alaska terminated 2025-07-22, so `terminated_date > '2026-07-01'`
  -- dropped it -- while raw.account_map and raw.account_rollup, which key on
  -- account_id and never consult this view, went on publishing its 15 appropriate
  -- patient-days and 2 verifications. The result was a row on Pam's sheet with counts
  -- and no name, no regional, no senior, and it was the account behind the one gate
  -- that looked like it had failed.
  --
  -- A terminated practice we are still collecting path logs for is exactly the
  -- practice someone is asking about -- Anchorage may be coming back on -- so the
  -- filter must not be the thing that decides. It still excludes accounts with no
  -- folder and no relationship to the analysis, which is what it was for.
  --
  -- The date is a widget now, not the literal '2026-07-01' that sat here. Hardcoded,
  -- it aged with every window roll: at Apr-Aug an account terminated in July 2026 was
  -- live for the whole reporting period and still vanished off the sheet.
  AND (g.senior_regional_name IS NOT NULL OR mp.account_id IS NOT NULL)
  AND (s.terminated_date IS NULL
       OR s.terminated_date >= date('{TERM_AFTER}')
       OR mp.account_id IS NOT NULL)
""")

_ad = spark.sql(f"""
SELECT count(*) AS accounts,
       sum(CASE WHEN status = 'Terminated' THEN 1 ELSE 0 END)          AS terminated,
       sum(CASE WHEN senior_regional_name IS NULL THEN 1 ELSE 0 END)   AS no_senior
FROM {C}.raw.account_dim
""").first()
print(f"  {_ad['accounts']:,} accounts · {_ad['terminated']} terminated · "
      f"{_ad['no_senior']} with no senior regional named")
if _ad["no_senior"]:
    # These are here only because we hold a path log for them. Worth naming rather
    # than admitting silently -- an unexpected one means the account map is wrong.
    display(spark.sql(f"""
      SELECT account_id, account_name, status, go_live_date, terminated_date
      FROM {C}.raw.account_dim WHERE senior_regional_name IS NULL ORDER BY account_name
    """))
    print("  ^ admitted because a path-log folder maps to them, not because Salesforce "
          "assigned a senior. Check each is a practice you mean to report on.")

print(f"{C}.raw.account_dim: {spark.table(f'{C}.raw.account_dim').count():,} accounts")
display(spark.sql(f"""
SELECT practice_group, count(*) AS sites,
       array_sort(collect_list(account_name)) AS members
FROM {C}.raw.account_dim GROUP BY practice_group HAVING sites > 1
ORDER BY sites DESC LIMIT 40
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Ready for Notebook 04?

# COMMAND ----------

ok = True
for t, cols in [(f"{C}.raw.vob", ["vob_id", "account_id", "patient_name_raw",
                                  "vob_created", "status", "simulation_scheduled",
                                  "simulation_actual"]),
                (f"{C}.raw.account_dim", ["account_id", "account_name", "practice_group"])]:
    try:
        have = set(spark.table(t).columns)
        miss = [c for c in cols if c not in have]
        n = spark.table(t).count()
        print(f"  {'ok  ' if not miss and n else 'FAIL'} {t}: {n:,} rows"
              + (f"  MISSING {miss}" if miss else ""))
        ok &= not miss and bool(n)
    except Exception as e:
        print(f"  FAIL {t}: {str(e)[:120]}")
        ok = False

_am = spark.sql(f"SHOW TABLES IN {C}.raw LIKE 'account_map'").count()
print(f"  {'ok  ' if _am else '--  '} {C}.raw.account_map "
      + ("exists" if _am else "NOT BUILT — run 04_account_map first"))

print("\n" + ("Notebook 04 can run." if ok and _am else
              "Notebook 04 is still blocked — see the lines above."))