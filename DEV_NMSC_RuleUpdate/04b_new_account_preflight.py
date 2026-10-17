# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — Notebook 06b: new-account pre-flight

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("sf_accounts", "us_scdb_prod.comm_gold.rpt_account_dim",
                     "Salesforce account table (id + name)")
dbutils.widgets.text("sf_id_col", "account_id", "…its AccountId column")
dbutils.widgets.text("sf_name_col", "account_name", "…its name column")
dbutils.widgets.text("prior_feed", "", "Last run's deck_feed table (blank = gold.deck_feed)")
dbutils.widgets.text("patient_access_xlsx", "", "Patient Access & Growth Projections (volume path)")
dbutils.widgets.text("prior_master_xlsx", "", "Last month's delivered workbook (volume path)")
dbutils.widgets.dropdown("write", "no", ["no", "yes"], "Write raw.new_account_review")

C      = dbutils.widgets.get("catalog")
SF     = dbutils.widgets.get("sf_accounts")
SF_ID  = dbutils.widgets.get("sf_id_col")
SF_NM  = dbutils.widgets.get("sf_name_col")
PRIOR  = dbutils.widgets.get("prior_feed").strip() or f"{C}.gold.deck_feed"
PA_XLS = dbutils.widgets.get("patient_access_xlsx").strip()
PM_XLS = dbutils.widgets.get("prior_master_xlsx").strip()
WRITE  = dbutils.widgets.get("write") == "no"

import datetime as _dt, os
import pandas as pd
RUN_ID = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
AMAP = f"{C}.raw.account_map"
print(f"catalog {C} · account map {AMAP} · run {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Which folders are new

# COMMAND ----------

folders = spark.sql(f"""
  SELECT DISTINCT account_name AS folder_name FROM {C}.staged.patient_days
""").toPandas()
print(f"staged folders this run: {len(folders)}")

try:
    prior = set(spark.table(PRIOR).select("account_name").toPandas().account_name)
    print(f"previous feed {PRIOR}: {len(prior)} accounts")
except Exception as e:
    prior = set()
    print(f"** no previous feed at {PRIOR} ({type(e).__name__}) — treating every folder "
          f"as new. On the first run this is expected; after that it means the feed was "
          f"never written and there is no baseline to diff against.")

folders["is_new"] = ~folders.folder_name.isin(prior)
NEW = sorted(folders.loc[folders.is_new, "folder_name"])
GONE = sorted(prior - set(folders.folder_name))
print(f"\nNEW folders: {len(NEW)}")
for n in NEW:
    print(f"    + {n}")
print(f"\nGONE since last run: {len(GONE)}")
for n in GONE:
    print(f"    - {n}   (terminated, renamed, or the export missed it — check which)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Did each new folder get an AccountId?

# COMMAND ----------

amap = spark.table(AMAP).toPandas()
sf = spark.sql(f"SELECT {SF_ID} AS account_id, {SF_NM} AS sf_name FROM {SF}").toPandas()

rep = []
for n in NEW:
    rows = amap[amap.folder_name == n]
    ids = list(rows.account_id)
    roles = ",".join(sorted(set(rows.role))) if len(rows) else ""
    # a name that exists in Salesforce but did not map means the join missed something
    # fixable -- a trailing space, a different dash -- rather than a genuinely absent id
    in_sf = sf[sf.sf_name.astype(str).str.strip().str.lower() == n.strip().lower()]
    rep.append(dict(
        folder_name=n,
        mapped_ids=len(ids),
        account_ids=";".join(ids),
        roles=roles,
        name_exists_in_salesforce=len(in_sf) > 0,
        blocking=("NO AccountId — will score zero VOBs" if not ids else ""),
    ))
rep = pd.DataFrame(rep)
if len(rep):
    print(rep.to_string(index=False))
    _nb = rep[rep.mapped_ids == 0]
    if len(_nb):
        print(f"\n*** {len(_nb)} new folder(s) have NO AccountId:")
        for _, r in _nb.iterrows():
            if r.name_exists_in_salesforce:
                print(f"    {r.folder_name}")
                print("        the name IS in Salesforce, so this is a normalisation "
                      "miss — a trailing space, a different dash, a doubled space. "
                      "Fix the folder name or add the row by hand in 06.")
            else:
                print(f"    {r.folder_name}")
                print("        no Salesforce account of this name. Either the practice "
                      "is not in Salesforce yet, or the folder is named differently from "
                      "the account. Confirm with the field before mapping it.")
else:
    print("no new folders — nothing to map")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Could any new folder be POOLED?

# COMMAND ----------

import re as _re

def _key(s):
    s = _re.sub(r"#\s*\d+", " ", str(s).lower())
    s = _re.sub(r"[^a-z0-9 ]", " ", s)
    return _re.sub(r"\s+", " ", s).strip()

sf["k"] = sf.sf_name.map(_key)
mapped_ids = set(amap.account_id)
cand = []
for n in NEW:
    k = _key(n)
    sibs = sf[(sf.k == k)]
    for _, s in sibs.iterrows():
        if s.account_id in set(amap.loc[amap.folder_name == n, "account_id"]):
            continue
        cand.append(dict(folder_name=n, candidate_id=s.account_id, candidate_name=s.sf_name,
                         already_mapped_elsewhere=s.account_id in mapped_ids))
cand = pd.DataFrame(cand)
if len(cand):
    print("POOLED candidates — CONFIRM WITH THE FIELD before seeding any of these:")
    print(cand.to_string(index=False))
    print("\nAn id already mapped elsewhere is NOT a pooling candidate; it belongs to "
          "another folder and adding it would double count.")
else:
    print("no pooling candidates among the new folders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What will render blank

# COMMAND ----------

CARRIED = ["Mohs", "NMSC Result Presentation", "NMSC Results Delivered By",
           "Treatment Option Presentation", "GentleCure Consult Script"]
gaps = []
pm = {}
if PM_XLS and os.path.exists(PM_XLS):
    _w = pd.read_excel(PM_XLS, sheet_name="Account Summary", header=1)
    _c0 = _w.columns[0]
    pm = {str(a).strip() for a in _w[_c0].dropna()}
    print(f"prior master {os.path.basename(PM_XLS)}: {len(pm)} accounts")
else:
    print("** no prior master given — cannot say which accounts lose the carried columns")

pax = set()
if PA_XLS and os.path.exists(PA_XLS):
    _pa = pd.read_excel(PA_XLS, sheet_name="Account Summary", header=1)
    _ca = [c for c in _pa.columns if isinstance(c, str) and c.strip().lower() == "account"][0]
    pax = {str(a).strip() for a in _pa[_ca].dropna()}
    print(f"Patient Access {os.path.basename(PA_XLS)}: {len(pax)} accounts")
else:
    print("** no Patient Access file given")

for n in NEW:
    gaps.append(dict(
        folder_name=n,
        in_prior_master=(n in pm) if pm else None,
        loses_carried_columns=(n not in pm) if pm else None,
        in_patient_access=(n in pax) if pax else None,
        loses_transition_discussion_lever=(n not in pax) if pax else None,
        has_prior_month_comparison=n in prior))
gaps = pd.DataFrame(gaps)
if len(gaps):
    print()
    print(gaps.to_string(index=False))
    print("\nA new account with no prior master row shows blank Mohs and blank clinical "
          "workflow; with no Patient Access row it reads N/A on transition, data "
          "discussion and lever. Neither blocks the build. Both are visible on the card.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The answer, in one place

# COMMAND ----------

blocking = 0 if not len(rep) else int((rep.mapped_ids == 0).sum())
print("=" * 72)
print(f"NEW FOLDERS: {len(NEW)}     GONE: {len(GONE)}")
print(f"  unmapped (BLOCKING, will score zero VOBs) : {blocking}")
print(f"  pooling candidates to confirm             : {0 if not len(cand) else len(cand)}")
if len(gaps) and gaps.loses_carried_columns.notna().any():
    print(f"  will lose Mohs / clinical workflow        : {int(gaps.loses_carried_columns.sum())}")
if len(gaps) and gaps.loses_transition_discussion_lever.notna().any():
    print(f"  will read N/A on the Patient Access three : "
          f"{int(gaps.loses_transition_discussion_lever.sum())}")
print("=" * 72)
if blocking:
    print("\nDO NOT PROCEED until every new folder has an AccountId. Add the row in 06 "
          "(exact-name join first; hand-fill only what the join cannot see) and re-run it.")
else:
    print("\nNo blocking gaps. New folders are mapped; the rest is cosmetic and visible.")

# COMMAND ----------

out = pd.DataFrame()
if len(NEW):
    out = rep.merge(gaps, on="folder_name", how="outer")
    out.insert(0, "run_id", RUN_ID)
if not WRITE:
    print("DRY RUN — raw.new_account_review not written. Set write = yes to keep the record.")
elif len(out):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.raw")
    spark.createDataFrame(out).write.mode("overwrite").option(
        "overwriteSchema", "true").saveAsTable(f"{C}.raw.new_account_review")
    print(f"wrote {C}.raw.new_account_review: {len(out)} rows")
else:
    print("nothing to write — no new folders")