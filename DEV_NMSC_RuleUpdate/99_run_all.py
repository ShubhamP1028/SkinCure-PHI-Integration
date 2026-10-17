# Databricks notebook source
# MAGIC %md
# MAGIC # Pathology Focus — RUN ALL

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("schema", "raw", "Bronze schema")
dbutils.widgets.dropdown("mode", "binary_table", ["binary_table", "volume_files"], "Source mode")
dbutils.widgets.text("source_table", "pathology_monthly_exports", "Source table (blank = use volume)")
dbutils.widgets.text("source_volume", "/Volumes/dev_pathology_data/raw/practice_pathlogs", "Source volume")
dbutils.widgets.text("expect_min_files", "0", "Fail extract below this file count (0 = off)")
dbutils.widgets.text("batch_size", "200", "Extract: files per batch")
dbutils.widgets.text("max_minutes", "0", "Extract: stop after N min (0 = no limit)")
dbutils.widgets.dropdown("account_map_write", "no", ["yes", "no"], "06: write account_map?")
dbutils.widgets.text("vob_from", "2026-01-01", "07: VOB pull from")
dbutils.widgets.text("vob_to", "2027-01-01", "07: VOB pull to")
dbutils.widgets.text("window_start", "2026-03-01", "Window start (inclusive)")
dbutils.widgets.text("window_end", "2026-07-01", "Window end (exclusive)")
dbutils.widgets.text("months", "4", "09: months in period (Pam's Months_In_Period)")
dbutils.widgets.text("timeout_seconds", "3600", "Per-stage timeout (s)")

dbutils.widgets.dropdown("run_extract", "yes", ["yes", "no"], "1. extract")
dbutils.widgets.dropdown("run_classify", "yes", ["yes", "no"], "2. classify")
dbutils.widgets.dropdown("run_rule_d_gate", "yes", ["yes", "no"], "2b. RULE D gate")
dbutils.widgets.dropdown("run_patient_days", "yes", ["yes", "no"], "3. patient_days")
dbutils.widgets.dropdown("run_account_map", "no",  ["yes", "no"], "6. account_map")
dbutils.widgets.dropdown("run_vob_source", "no",  ["yes", "no"], "7. vob source")
dbutils.widgets.dropdown("run_vob_match", "yes", ["yes", "no"], "4. vob_match")
dbutils.widgets.dropdown("run_review_views", "yes", ["yes", "no"], "8. review views")
# 8b lands the Salesforce monthly numbers that 09 publishes as VOB and SIM.
# Added 28 Aug with the stage itself; the stage was added and this was not,
# so the loop asked for a widget that did not exist and the run died on
# InputWidgetNotDefined before a single notebook started.
dbutils.widgets.dropdown("run_allaccounts", "yes", ["yes", "no"], "8b. AllAccounts rollup")
dbutils.widgets.dropdown("run_gold", "yes", ["yes", "no"], "9. gold (Pam's tabs)")
dbutils.widgets.dropdown("run_preflight", "yes", ["yes", "no"], "4b. new-account preflight")
dbutils.widgets.dropdown("run_qa", "yes", ["yes", "no"], "99. QA")
dbutils.widgets.dropdown("run_deck_feed", "no", ["yes", "no"], "10. deck feed")
dbutils.widgets.text("all_accounts_csv", "", "All-Accounts YTD export (volume path)")
dbutils.widgets.text("patient_access_xlsx", "", "Patient Access file (volume path)")
dbutils.widgets.text("growth_month", "2026-08", "Growth month")
dbutils.widgets.dropdown("stop_on_failure", "yes", ["yes", "no"], "Stop on first failure")

W = dbutils.widgets.get
CATALOG = W("catalog")
TIMEOUT = int(W("timeout_seconds"))
STOP_ON_FAILURE = W("stop_on_failure") == "yes"

_common = {"catalog": CATALOG}
_window = {"window_start": W("window_start"), "window_end": W("window_end")}

# (widget flag, notebook, args, table to count after, label). See wiki.
STAGES = [
    ("run_extract", "./01_ingest",
     {**_common, "schema": W("schema"), "mode": W("mode"),
      "source_table": W("source_table"), "source_volume": W("source_volume"),
      "expect_min_files": W("expect_min_files"),
      "batch_size": W("batch_size"), "max_minutes": W("max_minutes")},
     f"{CATALOG}.raw.path_log_rows", "1. extract -> bronze"),

    ("run_classify", "./02_classify",
     {**_common},
     f"{CATALOG}.staged.lesion_classified", "2. classify -> lesion_classified"),

    # Read-only. Sits between classify and patient_days because it asserts against
    # lesion_classified and must fail before the patient-day grain is rebuilt on top
    # of a rule that did not land.
    ("run_rule_d_gate", "./02b_rule_d_validation",
     {**_common},
     f"{CATALOG}.staged.lesion_classified", "2b. RULE D gate"),

    ("run_patient_days", "./03_patient_days",
     {**_common, **_window},
     f"{CATALOG}.staged.patient_days", "3. patient_days"),

    ("run_account_map", "./04_account_map",
     {**_common, "write": W("account_map_write")},
     f"{CATALOG}.raw.account_map", "6. account_map (folder -> AccountId)"),

    # terminated_after = the window start. account_dim drops accounts that terminated
    # before the period being reported on, and that cutoff was a hardcoded date until
    # 31 Aug -- it has to roll with the window or it quietly starts excluding practices
    # that were live for the whole period.
    ("run_vob_source", "./05_vob_source",
     {**_common, "vob_from": W("vob_from"), "vob_to": W("vob_to"),
      "terminated_after": W("window_start")},
     f"{CATALOG}.raw.vob", "7. vob source from Salesforce"),

    # _window is for section 8's reconciliation only: it must count VOBs on the same
    # four-month basis the prior master used, not over the whole raw.vob pull.
    ("run_vob_match", "./06_match",
     {**_common, **_window},
     f"{CATALOG}.staged.vob_match", "4. vob_match + 8.3 leak check"),

    # 08 takes the window as of 28 Aug: patient_days is additive now, so the review
    # views have to say which month they are reviewing rather than assuming the table
    # holds one.
    ("run_review_views", "./07_review_views",
     {**_common, **_window},
     f"{CATALOG}.staged.appropriate_patients", "8. review views"),

    # 08b lands the Salesforce monthly numbers. It must precede 09, which now reads
    # raw.account_rollup to build gold.sf_rollup and carries those columns onto the
    # summary — VOB and SIM are published on the Salesforce basis. If this step is
    # skipped, 09 still runs and every account publishes zero verifications.
    ("run_allaccounts", "./07b_allaccounts_rollup",
     {**_common, **_window},
     f"{CATALOG}.raw.account_rollup", "8b. AllAccounts rollup"),

    # 09 is LAST of the data stages: it reads patient_days, the scope table, vob_match. See wiki.
    ("run_gold", "./08_gold",
     {**_common, **_window, "months": W("months")},
     f"{CATALOG}.gold.account_summary", "9. gold — Pam's five tabs"),

    ("run_qa", "./99_qa",
     {**_common, **_window, "growth_month": W("growth_month"),
      "all_accounts_csv": W("all_accounts_csv")},
     None, "99. QA (read-only) — BLOCKS on a failed gate"),

    # 10 is off by default: it needs the two business files, and running it without
    # them silently falls back to gold.trend for the growth month, which is the
    # September defect.
    ("run_deck_feed", "./10_deck_feed",
     {**_common, **_window, "growth_month": W("growth_month"),
      "all_accounts_csv": W("all_accounts_csv"),
      "patient_access_xlsx": W("patient_access_xlsx")},
     f"{CATALOG}.gold.deck_feed", "10. deck feed"),
]

# 4b runs between the account map and the VOB source, and only reports.
STAGES.insert(4, ("run_preflight", "./04b_new_account_preflight",
                  {**_common, "patient_access_xlsx": W("patient_access_xlsx")},
                  None, "4b. new-account preflight (read-only)"))

print(f"catalog : {CATALOG}")
print(f"source  : {W('mode')} -> "
      f"{W('source_table') or '(none)' if W('mode') == 'binary_table' else W('source_volume')}")
print(f"window  : {W('window_start')} .. {W('window_end')}")
print(f"timeout : {TIMEOUT}s per stage")
print(f"stages  : {', '.join(lbl for flag, _, _, _, lbl in STAGES if W(flag) == 'yes') or 'NONE SELECTED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

import time
import traceback

results = []


def _rows(table):
    if not table:
        return None
    try:
        return spark.table(table).count()
    except Exception:
        return None


for flag, path, args, out_table, label in STAGES:
    if W(flag) != "yes":
        results.append({"stage": label, "status": "SKIPPED", "secs": 0, "rows": None, "detail": ""})
        print(f"--  SKIPPED   {label}")
        continue

    print(f"==> RUNNING   {label}")
    t0 = time.time()
    try:
        dbutils.notebook.run(path, TIMEOUT, args)
        secs = time.time() - t0
        n = _rows(out_table)
        status = "OK"
        detail = ""
        if out_table and n == 0:
            status = "EMPTY"
            detail = f"{out_table} has 0 rows"
        results.append({"stage": label, "status": status, "secs": secs, "rows": n, "detail": detail})
        print(f"    {status:8s} {secs:6.1f}s" + (f"  {n:,} rows in {out_table}" if n is not None else ""))
    except Exception as e:
        secs = time.time() - t0
        msg = str(e).strip().replace("\n", " ")[:300]
        results.append({"stage": label, "status": "FAILED", "secs": secs, "rows": None, "detail": msg})
        print(f"    FAILED   {secs:6.1f}s")
        print(f"      {msg}")
        if STOP_ON_FAILURE:
            print("\n    stop_on_failure = yes — remaining stages not attempted.")
            for f2, _, _, _, l2 in STAGES[len(results):]:
                results.append({"stage": l2, "status": "NOT RUN", "secs": 0, "rows": None, "detail": ""})
            break

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

w = max(len(r["stage"]) for r in results)
print(f"{'STAGE':<{w}}  {'STATUS':<8} {'SECS':>7}  {'ROWS':>10}")
print("-" * (w + 32))
for r in results:
    rows = f"{r['rows']:,}" if r["rows"] is not None else ""
    print(f"{r['stage']:<{w}}  {r['status']:<8} {r['secs']:>7.1f}  {rows:>10}")
    if r["detail"]:
        print(f"{'':<{w}}  -> {r['detail']}")

bad = [r for r in results if r["status"] in ("FAILED", "EMPTY")]
print()
if bad:
    print(f"{len(bad)} stage(s) need attention:")
    for r in bad:
        print(f"  - {r['stage']}: {r['status']} {r['detail']}")
    print("\nCommon causes:")
    print("  4. vob_match FAILED  -> raw.account_map still holds the seeded Salesforce")
    print("                          names, not your SharePoint folder names; or raw.vob")
    print("                          does not exist yet. Both are asserted deliberately.")
    print("  1. extract EMPTY     -> the manifest already lists every file. Drop")
    print(f"                          {CATALOG}.raw.file_manifest to force a re-extract.")
else:
    print("all selected stages OK")

# fail the run so a Job or a scheduled trigger goes red rather than green-with-errors
if any(r["status"] == "FAILED" for r in results):
    raise Exception(f"{sum(1 for r in results if r['status'] == 'FAILED')} stage(s) failed — see the summary above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-running one stage