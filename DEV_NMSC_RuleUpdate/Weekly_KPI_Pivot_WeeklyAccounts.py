# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## **_Weekly KPI — Appropriate · Marked · VOB · SIM, pivoted by week_**
# MAGIC
# MAGIC One row per weekly-pull account, one column per metric per week, in the same shape as
# MAGIC **All Account Trailing 12 Months** — only the bucket is a week (`yyyy-MM-dd`, Monday)
# MAGIC instead of a month.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps
# MAGIC ### - Attach to a cluster that can read `dev_pathology_data` **and** `us_scdb_dev.comm_silver`
# MAGIC ### - Run All
# MAGIC ### - Download the result from the last cell
# MAGIC
# MAGIC ### Prerequisites: notebooks **03** (patient_days) and **06** (vob_match) current. Nothing else.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note
# MAGIC Nothing here re-implements a counting rule. Appropriate and Marked come from
# MAGIC `staged.patient_days`, VOB and SIM from `staged.vob_match` — the same tables 03 and 06
# MAGIC write. `20_weekly_run.py` cell 3c records why that matters: a hand-written patient-day
# MAGIC derivation drifted from 03 and a patient with a real VOB reported "Any VOB = No".
# MAGIC

# COMMAND ----------

# 1. Long format: one row per account per week.
df_long = spark.sql("""
WITH bounds AS (
  -- EDIT HERE ONLY. Inclusive start, exclusive end.
  SELECT date'2026-06-01' AS w0, date'2026-10-9' AS w1
),

-- Scope taken LIVE from Salesforce, not a hardcoded list. account_map collapses
-- pooled AccountIds onto one folder, which is what makes Seraly (#1 + #2) and
-- Schaberg (Maryville + ALTON) SUM rather than split into two rows.
scope AS (
  SELECT m.folder_name                                  AS account_name,
         concat_ws('; ', array_sort(collect_set(s.account_id))) AS account_ids,
         count(DISTINCT s.account_id)                   AS n_ids,
         max(s.senior_director)                         AS senior_director,
         max(s.regional_manager)                   AS regional_manager_name,
         max(s.mohs)                AS mohs_offered_in_practice,
         max(s.status)                                  AS status,
         max(s.go_live_date)                            AS go_live_date
  FROM us_scdb_dev.comm_silver.dim_account_dtl s
  JOIN dev_pathology_data.raw.account_map m ON m.account_id = s.account_id
  WHERE s.access_to_pathology_data = 'Weekly & Monthly Pull'
    AND m.folder_name IS NOT NULL
  GROUP BY m.folder_name
),

-- Monday-start weeks, ONE definition for every metric. Never mix this with the
-- pathology month-week floor((day-1)/7)+1 -- they disagree, and mixing them puts a
-- patient's biopsy and their VOB in different buckets.
pd AS (
  SELECT d.account_name,
         date_trunc('WEEK', d.biopsy_date) AS week_start,
         d.patient_id, d.biopsy_date, d.appropriate, d.marked
  FROM dev_pathology_data.staged.patient_days d
  JOIN scope s ON s.account_name = d.account_name
  CROSS JOIN bounds b
  WHERE d.biopsy_date >= b.w0 AND d.biopsy_date < b.w1
),

-- One row per VOB. vob_folder = the folder owning the AccountId it was raised
-- against ("this practice's VOBs", pooled ids already summed). matched_folder +
-- patient_id + biopsy_date = the patient-day it was bound to, which is what the
-- cohort columns need.
vm AS (
  SELECT vob_id, vob_folder, matched_folder, patient_id, biopsy_date, matched,
         to_date(vob_created)          AS vob_created,
         to_date(simulation_scheduled) AS sim_sched,
         to_date(simulation_actual)    AS sim_actual
  FROM dev_pathology_data.staged.vob_match
),

-- COHORT: every column keyed on the BIOPSY week, so a ratio divides two counts
-- over the SAME patients. The only basis on which a conversion means anything.
cohort AS (
  SELECT p.account_name, p.week_start,
         count(*)                                   AS appropriate_pd,
         sum(CASE WHEN p.marked THEN 1 ELSE 0 END)  AS marked_pd,
         count(DISTINCT CASE WHEN v.vob_id     IS NOT NULL
               THEN concat_ws('|', p.patient_id, p.biopsy_date) END) AS vob_pd,
         count(DISTINCT CASE WHEN v.sim_sched  IS NOT NULL
               THEN concat_ws('|', p.patient_id, p.biopsy_date) END) AS sim_sched_pd,
         count(DISTINCT CASE WHEN v.sim_actual IS NOT NULL
               THEN concat_ws('|', p.patient_id, p.biopsy_date) END) AS sim_att_pd
  FROM pd p
  LEFT JOIN vm v
    ON  v.matched_folder = p.account_name
    AND v.patient_id     = p.patient_id
    AND v.biopsy_date    = p.biopsy_date
    AND v.matched
  WHERE p.appropriate
  GROUP BY 1, 2
),

-- ACTIVITY: each event on its OWN date -- "what did the team do that week".
act AS (
  SELECT account_name, week_start,
         sum(vc) AS vobs_created, sum(ss) AS sims_scheduled, sum(sa) AS sims_attended
  FROM (
    SELECT v.vob_folder AS account_name, date_trunc('WEEK', v.vob_created) AS week_start,
           1 AS vc, 0 AS ss, 0 AS sa
    FROM vm v JOIN scope s ON s.account_name = v.vob_folder CROSS JOIN bounds b
    WHERE v.vob_created >= b.w0 AND v.vob_created < b.w1
    UNION ALL
    SELECT v.vob_folder, date_trunc('WEEK', v.sim_sched), 0, 1, 0
    FROM vm v JOIN scope s ON s.account_name = v.vob_folder CROSS JOIN bounds b
    WHERE v.sim_sched >= b.w0 AND v.sim_sched < b.w1
    UNION ALL
    SELECT v.vob_folder, date_trunc('WEEK', v.sim_actual), 0, 0, 1
    FROM vm v JOIN scope s ON s.account_name = v.vob_folder CROSS JOIN bounds b
    WHERE v.sim_actual >= b.w0 AND v.sim_actual < b.w1
  ) x
  GROUP BY 1, 2
),

spine AS (
  SELECT account_name, week_start FROM cohort
  UNION SELECT account_name, week_start FROM act
)

SELECT
  sc.account_name, sc.account_ids, sc.n_ids, sc.status,
  sc.senior_director, sc.regional_manager_name, sc.mohs_offered_in_practice, sc.go_live_date,
  cast(sp.week_start AS date)        AS week_start,
  coalesce(c.appropriate_pd, 0)      AS Appropriate,
  coalesce(c.marked_pd,      0)      AS Marked,
  coalesce(c.vob_pd,         0)      AS VOB,
  coalesce(c.sim_att_pd,     0)      AS SIM,
  coalesce(c.sim_sched_pd,   0)      AS SIM_Scheduled,
  coalesce(a.vobs_created,   0)      AS VOB_Created,
  coalesce(a.sims_attended,  0)      AS SIM_Attended_Wk,
  datediff(current_date(), date_add(cast(sp.week_start AS date), 6)) AS days_since_week_end
FROM spine sp
JOIN scope  sc ON sc.account_name = sp.account_name
LEFT JOIN cohort c ON c.account_name = sp.account_name AND c.week_start = sp.week_start
LEFT JOIN act    a ON a.account_name = sp.account_name AND a.week_start = sp.week_start
""")

print(f"{df_long.count()} account-week rows")
display(df_long.limit(20))


# COMMAND ----------

# 2. Pivot to wide — the All-Accounts pattern, week label in place of yyyy-MM.
from pyspark.sql import functions as F

META = ["account_name", "account_ids", "n_ids", "status", "senior_director",
        "regional_manager_name", "mohs_offered_in_practice", "go_live_date"]

# Order matters: this is the order the metric BANDS appear in the sheet.
METRICS = ["Appropriate", "Marked", "VOB", "SIM", "SIM_Scheduled", "VOB_Created"]

# The week label is the MONDAY as yyyy-MM-dd, not an ISO week number. Week numbers
# read as "2026-W38" and stop sorting correctly at a year boundary; a date sorts
# lexically, is unambiguous, and can be read back as a date without a lookup.
stacked = df_long.selectExpr(
    *META,
    "date_format(week_start, 'yyyy-MM-dd') AS wk",
    "stack({n}, {pairs}) AS (metric, value)".format(
        n=len(METRICS),
        pairs=", ".join(f"'{m}', CAST({m} AS DOUBLE)" for m in METRICS)),
)

pivot_df = (stacked
            .withColumn("metric_week", F.concat_ws("_", F.col("metric"), F.col("wk")))
            .groupBy(*META)
            .pivot("metric_week")
            .agg(F.sum("value"))
            .fillna(0))

print(f"{pivot_df.count()} accounts x {len(pivot_df.columns) - len(META)} metric-week columns")


# COMMAND ----------

# 3. Column order — metric bands, weeks ascending inside each band, then totals.
weeks = sorted({c.split("_")[-1] for c in pivot_df.columns if c not in META})
print(f"{len(weeks)} weeks: {weeks[0]} -> {weeks[-1]}")

out = pivot_df
ordered, totals = [], []
for m in METRICS:
    cols = [f"{m}_{w}" for w in weeks if f"{m}_{w}" in pivot_df.columns]
    ordered += cols
    # A TOTAL is a sum of the same columns shown beside it, never a separate query.
    # Anyone can add the row up by eye and get the number in the total column.
    out = out.withColumn(f"TOTAL_{m}", sum(F.col(c) for c in cols) if cols else F.lit(0))
    totals.append(f"TOTAL_{m}")

# Conversions are computed on the TOTALS, never averaged across weekly percentages --
# a mean of ratios weights a 2-patient week the same as a 40-patient one.
# All four share the patient-day grain and the biopsy-week cohort, so each divides
# two counts over the same patients.
out = (out
  .withColumn("Marked_pct",            F.round(100 * F.col("TOTAL_Marked") / F.col("TOTAL_Appropriate"), 0))
  .withColumn("Appropriate_to_VOB_pct",F.round(100 * F.col("TOTAL_VOB")    / F.col("TOTAL_Appropriate"), 0))
  .withColumn("Marked_to_VOB_pct",     F.round(100 * F.col("TOTAL_VOB")    / F.col("TOTAL_Marked"), 0))
  .withColumn("VOB_to_SIM_pct",        F.round(100 * F.col("TOTAL_SIM")    / F.col("TOTAL_VOB"), 0)))

RATIOS = ["Marked_pct", "Appropriate_to_VOB_pct", "Marked_to_VOB_pct", "VOB_to_SIM_pct"]
new_df = out.select(*(META + totals + RATIOS + ordered)).orderBy("account_name")


# COMMAND ----------

# 4. Maturity — read this before reading any recent week.
# A VOB trails its biopsy by weeks, so the newest columns are UNFINISHED, not bad.
# They will rise. Without this, the last two weeks of every band read as a collapse.
import datetime as _dt
_today = _dt.date.today()
_imm = [w for w in weeks
        if (_today - (_dt.date.fromisoformat(w) + _dt.timedelta(days=6))).days < 28]
if _imm:
    print("IMMATURE weeks — conversions for these will still rise:")
    for w in _imm:
        print(f"   {w}  ({(_today - (_dt.date.fromisoformat(w) + _dt.timedelta(days=6))).days} days since week end)")
    print("\nDo not compare them against older weeks, and say so on anything published.")
else:
    print("every week in the window is at least 28 days matured")

# Scope check: 6 accounts expected. Short means an AccountId lost its account_map row.
print(f"\naccounts returned: {new_df.count()}")
display(new_df.select("account_name", "account_ids", "n_ids"))


# COMMAND ----------

display(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # DOWNLOAD THE ABOVE RESULT