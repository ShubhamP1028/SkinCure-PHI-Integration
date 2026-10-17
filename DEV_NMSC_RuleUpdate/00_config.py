# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — 00: config

# COMMAND ----------

# One place for the run's parameters. Every other notebook takes the same widget names,
# so `99_run_all` can pass one set through and nothing has to be edited in two places.

CATALOG_DEV  = "dev_pathology_data"
CATALOG_PROD = "prod_pathology_data"

# The reporting window. Exclusive of its end: 2026-04-01 → 2026-08-01 is Apr, May, Jun, Jul.
WINDOW_START = "2026-06-01"
WINDOW_END   = "2026-11-01"

# The month AFTER the window. Salesforce only — no path log yet. Taken from the
# All-Accounts export, NOT from gold.trend; see the wiki, "The August lesson".
GROWTH_MONTH = "2026-10"

# The VOB query must reach at least 60 days either side of the biopsy window, or a
# late-window biopsy has no chance of finding the verification it produced.
VOB_WINDOW_PAD_DAYS = 60

# Terminated accounts leave the summary and the deck. These stay anyway, by request.
KEEP_TERMINATED = ("anchorage", "wasilla", "petoskey")

# Business files, maintained outside the pipeline. Volume paths.
ALL_ACCOUNTS_CSV    = ""   # All_Account_YTD_Version_*.csv — status, VOB/SIM, VOB→Sim days
PATIENT_ACCESS_XLSX = ""   # Patient Access & Growth Projections — transition, discussion, lever
PRIOR_MASTER_XLSX   = ""   # last month's delivered workbook — Mohs + clinical workflow

OUT_VOLUME = f"/Volumes/{CATALOG_DEV}/gold/exports"

# Conversion goals. The flat set; an account already past one has its own goal raised
# above it, which is why both travel in the deck feed.
FLAT_GOAL = {"mark": 0.75, "m2v": 0.85, "v2s": 0.70, "n2v": 0.40}
HEADROOM  = {"mark": 0.03, "m2v": 0.05, "v2s": 0.03, "n2v": 0.03}

# Below this many appropriate patient-days a month, volume is the diagnosis and no
# conversion step can fix it.
SUPPLY_MAX_APPROPRIATE = 25

ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

# COMMAND ----------

def months_in_window(cols, w0=WINDOW_START, w1=WINDOW_END):
    """The window's month labels, read off the gold columns rather than typed."""
    import re
    found = sorted([m.group(1) for m in (re.match(r"^nmsc_([A-Z][a-z]{2})$", c) for c in cols) if m],
                   key=ORDER.index)
    if not found:
        raise ValueError("gold.account_summary has no nmsc_<Mon> columns — it is emitting "
                         "the positional form (nmsc_m1..m4), which nothing downstream reads")
    return [m for m in found if int(w0[5:7]) <= ORDER.index(m) + 1 < int(w1[5:7])]