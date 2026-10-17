# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — Notebook 10: export the gold layer for the workbook build

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("out_volume", "/Volumes/dev_pathology_data/gold/exports", "Output volume path")
dbutils.widgets.text("stamp", "", "Folder name under the volume (blank = window-derived)")
dbutils.widgets.text("window_start", "2026-05-01", "Window start (inclusive)")
dbutils.widgets.text("window_end", "2026-09-01", "Window end (exclusive)")
dbutils.widgets.dropdown("raw_unfiltered_window", "in_window",
                         ["in_window", "all_rows"], "Raw-Unfiltered scope")

C     = dbutils.widgets.get("catalog")
OUT   = dbutils.widgets.get("out_volume").rstrip("/")
W0    = dbutils.widgets.get("window_start")
W1    = dbutils.widgets.get("window_end")
RU    = dbutils.widgets.get("raw_unfiltered_window")
STAMP = dbutils.widgets.get("stamp").strip() or f"{W0}_to_{W1}"

# Volumes are not created implicitly. Fail here by name rather than on the first write
# with a path error that says nothing about which volume is missing.
_vparts = OUT.split("/")           # ['', 'Volumes', catalog, schema, volume, ...]
if len(_vparts) < 5 or _vparts[1] != "Volumes":
    raise ValueError(f"out_volume must look like /Volumes/<catalog>/<schema>/<volume>, got {OUT!r}")
_vcat, _vsch, _vvol = _vparts[2], _vparts[3], _vparts[4]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_vcat}.{_vsch}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {_vcat}.{_vsch}.{_vvol}")

DEST = f"{OUT}/{STAMP}"
dbutils.fs.mkdirs(DEST)
print(f"catalog : {C}")
print(f"window  : {W0} .. {W1}")
print(f"dest    : {DEST}")
print(f"Raw-Unfiltered scope: {RU}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. `gold.vob_scope` — one row per verification, with its biopsy MONTH

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.vob_scope USING DELTA AS
WITH {leaked_to_sister_sql(C)}
SELECT
  coalesce(v.vob_folder, v.matched_folder)              AS account,
  v.vob_id,
  v.account_id,
  -- [31 Aug] From 00_vocab, so notebook 08's disposition buckets and
  -- this table classify a verification identically. They did not before, and the
  -- sheet reported zero sister matches on every account as a result.
  {vob_scope_case_sql("v")}                             AS match_scope,
  -- THE MONTH KEY, populated for leaks as well as own-site matches. This is the
  -- single column the current export is missing.
  v.cohort_biopsy_date,
  date_format(v.cohort_biopsy_date, 'yyyy-MM')          AS cohort_month,
  v.is_duplicate_patient_vob,
  v.group_has_sim,
  v.gap_days,
  v.vob_created,
  v.status,
  v.simulation_scheduled,
  v.leak_folders,
  v.leak_outcome
FROM {C}.staged.vob_match v
LEFT JOIN lk ON lk.vob_id = v.vob_id
""")

_vs = spark.sql(f"""
SELECT match_scope,
       count(*)                                                  AS vobs,
       sum(CASE WHEN is_duplicate_patient_vob THEN 1 ELSE 0 END) AS duplicates,
       sum(CASE WHEN cohort_biopsy_date IS NULL THEN 1 ELSE 0 END) AS no_month
FROM {C}.gold.vob_scope GROUP BY match_scope ORDER BY vobs DESC
""")
display(_vs)

# Every verification with a biopsy must carry a month, or it cannot be cohorted and
# the monthly columns will not sum to the total.
_bad = spark.sql(f"""
SELECT count(*) AS n FROM {C}.gold.vob_scope
WHERE match_scope <> 'no biopsy found' AND cohort_biopsy_date IS NULL
""").first()["n"]
assert _bad == 0, (
    f"{_bad} verifications have a biopsy scope but no cohort_biopsy_date. The month "
    "key comes from coalesce(biopsy_date, leak_biopsy_date) in notebook 04 -- if it is "
    "NULL the leak side of that coalesce did not populate.")
print(f"{C}.gold.vob_scope written — every scoped verification carries a month")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What goes where

# COMMAND ----------

# (table, csv basename, workbook tab, window-filter column or None)
EXPORTS = [
    (f"{C}.gold.account_summary",       "account_summary",       "Account Summary",              None),
    (f"{C}.gold.raw_unfiltered",        "raw_unfiltered",        "Raw-Unfiltered",               "biopsy_date"),
    (f"{C}.gold.vob_path_match",        "vob_path_match",        "VOB Path Match",               None),
    (f"{C}.gold.paths_marked_no_match", "paths_marked_no_match", "Paths Marked w No Match",      None),
    (f"{C}.gold.review",                "review",                "Review",                       None),
    (f"{C}.staged.appropriate_patients","appropriate_patients",  "Appropriate Patients",         "biopsy_date"),
    # [26 Aug] Sheet 5 now carries the TEXT that triggered each flag, so it is worth
    # exporting rather than carrying the prior master's version. *** PHI: free-text
    # clinical notes, same handling as the other files here. ***
    (f"{C}.staged.vob_mentioned_no_vob","vob_mentioned_no_vob",  "VOB mentioned - no VOB found", "biopsy_date"),
    # [27 Aug] The disposition is now a pipeline output, not a side workbook. One row
    # per account, every VOB in exactly one of seven buckets, buckets_sum == total_vob
    # asserted in notebook 08. No patient identifiers -- counts only.
    (f"{C}.staged.vob_disposition",     "vob_disposition",       "VOB BreakDown",                None),
    # [28 Aug] One row per verification with its scope and its BIOPSY MONTH, so the
    # workbook can be built on own-site-only, own+sister, or the full pathology basis
    # without another pipeline change. Counts only -- no patient identifiers.
    (f"{C}.gold.vob_scope",             "vob_scope",             "VOB Scope",                    None),
    # [28 Aug] The Salesforce monthly rollup, summed over EVERY account mapped to a
    # folder rather than the primary alone. Six folders serve two Salesforce accounts
    # (two machines at one address, one path log); joining on role='primary' halved
    # Dunedin and Schaberg. Notebook 09 builds it and gates it. Counts only.
    (f"{C}.gold.sf_rollup",             "sf_rollup",             "AllAccounts",                  None),
    # [28 Aug] The published basis is now Salesforce VOB/SIM against path-log NMSC and
    # Marked, so the business question moved from "how did the window go" to "is it
    # improving". That needs a per-account-per-month table, which the summary's four
    # wide columns cannot answer once there is more than one window. Counts only.
    (f"{C}.gold.trend",                 "trend",                 "Trend",                        None),
]

# Sheets build_v7 wants that this pipeline does not produce. Named so nobody hunts for
# a file that was never meant to exist.
NOT_EXPORTED = {
    "Sheet1":            "account attributes — from AllAccounts / the prior master",
    "AllAccounts":       "the Salesforce rollup CSV, supplied to the build directly",
    # [25 Aug] Timing now REACHES gold: account_summary carries avg_days_to_vob on
    # Becky's anchor chain, the three component columns, and timing_anchor_mix.
    # These two tabs stay carried from the prior master because they are laid out
    # per-account-per-month, which the summary grain cannot supply.
    "Result to Sim AVG": "timing helper — different grain; timing itself is now in account_summary",
    "AvgDays":           "timing source — same",
    "VOB Path Source":   "account_name -> Account_Transition, from the prior master",
    # NOTE: "VOB mentioned - no VOB found" moved OUT of this list on 26 Aug -- it is
    # now produced above. The workbook adapter still CARRIES the prior master's copy
    # of that sheet, so it needs one line changed to prefer the export, or the new
    # evidence columns will sit in the CSV and never reach the workbook.
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write

# COMMAND ----------

import os
from pyspark.sql import functions as F

def _write(table, base, tab, datecol):
    if not spark.catalog.tableExists(table):
        return {"tab": tab, "file": "", "rows": 0, "cols": 0,
                "note": f"MISSING TABLE {table}"}
    df = spark.table(table)
    note = ""
    if datecol and RU == "in_window" and datecol in df.columns:
        before = df.count()
        df = df.filter(f"{datecol} >= '{W0}' AND {datecol} < '{W1}'")
        note = f"filtered {before:,} -> {df.count():,} on {datecol}"
    elif datecol and datecol not in df.columns:
        note = f"no {datecol} column — exported unfiltered"

    # Any DATE outside pandas' representable range (1677..2262) makes toPandas()
    # raise "year N is out of range". 09 bounds its parsed dates, but an upstream
    # table could still carry one, and a crash here is a worse diagnostic than a
    # named column. Neutralise rather than fail.
    import pyspark.sql.types as T
    for f_ in df.schema.fields:
        if isinstance(f_.dataType, (T.DateType, T.TimestampType)):
            df = df.withColumn(f_.name, F.expr(
                f"CASE WHEN `{f_.name}` BETWEEN DATE'1700-01-01' AND DATE'2200-01-01' "
                f"THEN `{f_.name}` END"))
    pdf = df.toPandas()
    path = f"{DEST}/{base}.csv"
    # index=False or the build sees a phantom first column; NA -> '' so openpyxl
    # writes an empty cell rather than the literal string 'nan'.
    pdf.to_csv(path, index=False, na_rep="")
    return {"tab": tab, "file": f"{base}.csv", "rows": len(pdf),
            "cols": len(pdf.columns), "note": note}


results = [_write(*e) for e in EXPORTS]

w = max(len(r["tab"]) for r in results)
print(f"{'TAB':<{w}}  {'FILE':<24} {'ROWS':>9} {'COLS':>5}")
print("-" * (w + 42))
for r in results:
    print(f"{r['tab']:<{w}}  {r['file']:<24} {r['rows']:>9,} {r['cols']:>5}")
    if r["note"]:
        print(f"{'':<{w}}  -> {r['note']}")

print("\nNOT exported — the build supplies these:")
for tab, why in NOT_EXPORTED.items():
    print(f"  {tab:<20} {why}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gate

# COMMAND ----------

_bad = [r for r in results if r["rows"] == 0]
assert not _bad, ("these exports are empty: "
                  + ", ".join(f"{r['tab']} ({r['note'] or 'no rows'})" for r in _bad)
                  + ". Re-run 09 (and 08 for appropriate_patients) before building.")

# Derived from the window rather than fixed at four. A five-month window produces a. See wiki.
_MSUF = [r["label"] for r in spark.sql(
    f"SELECT date_format(d, 'MMM') AS label FROM (SELECT explode(sequence("
    f"date('{W0}'), date_sub(date('{W1}'), 1), interval 1 month)) AS d) ORDER BY d"
).collect()]
_MONTHS_IN_WINDOW = len(_MSUF)
print(f"window spans {_MONTHS_IN_WINDOW} month(s) — expecting "
      f"{', '.join(_MSUF)} as the monthly column suffixes")

_need = (["account_name"]
         + [f"{p}_{lab}" for p in ("nmsc", "marked", "vobm", "simm") for lab in _MSUF]
         + [f"{p}_total" for p in ("nmsc", "marked", "vobm", "simm")]
         # the published Salesforce columns
         + [f"sf_{k}_{lab}" for k in ("vob", "sim") for lab in _MSUF]
         + ["sf_vob_total", "sf_sim_total",
            "vob_pattern_band", "vob_pool_to_marked", "actions_coverage",
            "vob_with_biopsy_in_window",
            # column AY. Added Sep 2026 -- `build_pam_v2.py` prefers it over the
            # hand-pasted All-Accounts values when it is present in this CSV.
            "avg_days_vob_close_to_sim"])
_have = set(spark.table(f"{C}.gold.account_summary").columns)
_missing = [c for c in _need if c not in _have]
assert not _missing, (
    f"gold.account_summary is missing {_missing}. The monthly VOB and SIM columns "
    "(vobm_*, simm_*) were added on 24 Aug and the sf_* pair on 28 Aug, when VOB and "
    "SIM moved to the Salesforce basis. If only the sf_* columns are missing, 08b did "
    "not run. If whole month names are missing, gold.account_summary was written by a "
    "copy of 09 older than 31 Aug -- it still carries m1..m4. Re-run 09 from the "
    "current copy.")

print(f"\nall {len(results)} exports written to {DEST}")
print("\nnext: download the folder and run")
print(f"    python gold_to_v7.py --src <local copy of {STAMP}> "
      f"--prior <prior master.xlsx> --out workbook.xlsx")
print("\nPHI: these CSVs carry patient names, MRNs and dates of birth. Controlled "
      "machines only.")