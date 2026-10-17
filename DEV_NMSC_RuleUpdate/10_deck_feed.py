# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "openpyxl",
# ]
# ///
# MAGIC %md
# MAGIC # Pathology Focus — Notebook 11: the deck feed

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# openpyxl is not on the cluster by default and §9 writes an .xlsx. Each notebook gets
# a fresh Python environment, so 01_ingest's install does not carry over.
# restartPython() clears every variable — run this notebook top to bottom after it.
dbutils.library.restartPython()

# COMMAND ----------


dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("out_volume", "/Volumes/dev_pathology_data/gold/exports", "Output volume path")
dbutils.widgets.text("window_start", "2026-04-01", "Reporting window start (inclusive)")
dbutils.widgets.text("window_end", "2026-08-01", "Reporting window end (exclusive)")
dbutils.widgets.text("growth_month", "2026-08", "Growth month (the month AFTER the window)")
dbutils.widgets.text("all_accounts_csv", "", "All-Accounts YTD export (volume path)")
dbutils.widgets.dropdown("metrics_source", "warehouse", ["warehouse", "csv"],
                         "AVG days VOB→Sim and Account Transition from")
dbutils.widgets.text("patient_access_xlsx", "", "Patient Access & Growth Projections (volume path)")
dbutils.widgets.text("keep_terminated", "anchorage,vincennes",
                     "Terminated accounts to keep, comma-separated name fragments")
dbutils.widgets.dropdown("write", "no", ["no", "yes"], "Write gold.deck_feed")

C      = dbutils.widgets.get("catalog")
OUT    = dbutils.widgets.get("out_volume").rstrip("/")
W0     = dbutils.widgets.get("window_start")
W1     = dbutils.widgets.get("window_end")
GM_YM  = dbutils.widgets.get("growth_month").strip()
AA_CSV = dbutils.widgets.get("all_accounts_csv").strip()
MSRC   = dbutils.widgets.get("metrics_source")
PA_XLS = dbutils.widgets.get("patient_access_xlsx").strip()
KEEP   = tuple(x.strip().lower() for x in dbutils.widgets.get("keep_terminated").split(",") if x.strip())
WRITE  = dbutils.widgets.get("write") == "yes"

_vp = OUT.split("/")
if len(_vp) < 5 or _vp[1] != "Volumes":
    raise ValueError(f"out_volume must look like /Volumes/<catalog>/<schema>/<volume>, got {OUT!r}")

import datetime as _dt, os, re
import pandas as pd

ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
RUN_ID = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
FEED = f"{C}.gold.deck_feed"
print(f"catalog {C} · window {W0} → {W1} · growth month {GM_YM} · run {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The window's months, read off the data

# COMMAND ----------

_cols = set(spark.table(f"{C}.gold.account_summary").columns)
MONTHS = sorted([m.group(1) for m in (re.match(r"^nmsc_([A-Z][a-z]{2})$", c) for c in _cols) if m],
                key=ORDER.index)
if not MONTHS:
    raise ValueError(
        f"{C}.gold.account_summary has no nmsc_<Mon> columns. It is emitting the "
        "positional form (nmsc_m1..m4), which nothing downstream can read. Port the "
        "month-name change from dev's 09 into 07_gold before running this.")

# the window can be wider than the reporting window -- March was run to verify the
# recovered months and the deck still starts at April
_w0m, _w1m = int(W0[5:7]), int(W1[5:7])
WIN = [m for m in MONTHS if _w0m <= ORDER.index(m) + 1 < _w1m]
if not WIN:
    raise ValueError(f"no month columns fall inside {W0}..{W1}; found {MONTHS}")
N = len(WIN)
GM = ORDER[int(GM_YM[5:7]) - 1]
print(f"reporting months: {', '.join(WIN)}  ({N})   growth month: {GM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Roster

# COMMAND ----------

acct = spark.table(f"{C}.gold.account_summary").toPandas()
status = pd.DataFrame(columns=["account_id", "status"])
if AA_CSV and os.path.exists(AA_CSV):
    _aa = pd.read_csv(AA_CSV, low_memory=False)
    status = _aa[["account_id", "status"]].copy()
    print(f"status from {os.path.basename(AA_CSV)}: "
          f"{(status.status.astype(str).str.strip() != 'Active').sum()} not Active")
else:
    print("** no All-Accounts export given — every account is treated as Active, and "
          "August will fall back to gold.trend. See §4.")

_dead = set(status.loc[status.status.astype(str).str.strip() != "Active", "account_id"])
acct["is_terminated"] = acct.account_id.isin(_dead)
acct["kept_despite_terminated"] = acct.is_terminated & acct.account_name.str.lower().apply(
    lambda n: any(k in n for k in KEEP))
_drop = acct.is_terminated & ~acct.kept_despite_terminated
if _drop.any():
    print(f"terminated, dropped ({int(_drop.sum())}):")
    for n in acct.loc[_drop, "account_name"]:
        print(f"    {n}")
for n in acct.loc[acct.kept_despite_terminated, "account_name"]:
    print(f"terminated, KEPT by request: {n}")
acct = acct[~_drop].reset_index(drop=True)
print(f"roster: {len(acct)} accounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pooled AccountIds

# COMMAND ----------

_ids = (spark.sql(f"""
        SELECT account, concat_ws(';', sort_array(collect_set(account_id))) AS account_ids,
               count(DISTINCT account_id) AS accounts_pooled
        FROM {C}.gold.vob_scope GROUP BY account""").toPandas()
        .rename(columns={"account": "account_name"}))
acct = acct.merge(_ids, on="account_name", how="left")
acct["account_ids"] = acct.account_ids.fillna(acct.account_id)
acct["accounts_pooled"] = acct.accounts_pooled.fillna(1).astype(int)
print(f"pooled folders (>1 AccountId): {int((acct.accounts_pooled > 1).sum())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The growth month — from the report, not from `trend`

# COMMAND ----------

trend = spark.table(f"{C}.gold.trend").toPandas()
_tg = trend[trend.month.astype(str) == GM_YM].set_index("account_name")
growth_src = "gold.trend"
g_vob, g_sim = {}, {}
for _, a in acct.iterrows():
    n = a.account_name
    g_vob[n] = int(_tg.at[n, "sf_vob"]) if n in _tg.index else 0
    g_sim[n] = int(_tg.at[n, "sf_sim"]) if n in _tg.index else 0

if AA_CSV and os.path.exists(AA_CSV):
    _aa = pd.read_csv(AA_CSV, low_memory=False).set_index("account_id")
    cv, cs = f"VOB_{GM_YM}", f"SIM_{GM_YM}"
    if cv in _aa.columns and cs in _aa.columns:
        moved = 0
        for _, a in acct.iterrows():
            idl = [i for i in str(a.account_ids).split(";") if i in _aa.index]
            if not idl:
                continue
            v, s = int(_aa.loc[idl, cv].sum()), int(_aa.loc[idl, cs].sum())
            if (v, s) != (g_vob[a.account_name], g_sim[a.account_name]):
                moved += 1
            g_vob[a.account_name], g_sim[a.account_name] = v, s
        growth_src = os.path.basename(AA_CSV)
        print(f"{GM} re-sourced from {growth_src}: {moved} of {len(acct)} accounts moved")

        # the cross-check that matters: the LAST WINDOW month should already agree
        _lw = f"2026-{ORDER.index(WIN[-1]) + 1:02d}"
        if f"VOB_{_lw}" in _aa.columns:
            _p = int(acct[f"sf_vob_{WIN[-1]}"].sum())
            _r = sum(int(_aa.loc[[i for i in str(a.account_ids).split(";") if i in _aa.index],
                                f"VOB_{_lw}"].sum()) for _, a in acct.iterrows())
            _gap = _p - _r
            flag = "OK" if abs(_gap) <= max(5, 0.01 * _r) else "*** INVESTIGATE"
            print(f"cross-check {WIN[-1]}: pipeline {_p} vs report {_r}  gap {_gap:+d}  {flag}")
            if flag != "OK":
                print("    A gap here means the Salesforce pull behind gold.trend is "
                      "short, the same way August was in September. Re-pull before "
                      "publishing anything that quotes month-over-month growth.")
    else:
        print(f"** {os.path.basename(AA_CSV)} has no {cv}/{cs} — falling back to gold.trend")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Patient Access — transition, data discussion, lever

# COMMAND ----------

LEVER_CANON = {
    "nmsc to marked": "NMSC → Marked", "nmsc to mark": "NMSC → Marked",
    "marked to vob": "Marked → VOB",   "mark to vob": "Marked → VOB",
    "vob to sim": "VOB → Sim",
    "both top half": "Both top half",
    "flat": "Flat", "flat on nmsc to marked": "Flat",
}

def _lever(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    t = re.sub(r"\s+", " ", str(v)).strip()
    if not t or t.lower() in ("n/a", "na", "none"):
        return None
    return LEVER_CANON.get(t.lower(), t)

PAX = {}
if PA_XLS and os.path.exists(PA_XLS):
    _pa = pd.read_excel(PA_XLS, sheet_name="Account Summary", header=1)
    _c = {c.strip().lower(): c for c in _pa.columns if isinstance(c, str)}
    for _, r in _pa.iterrows():
        a = r.get(_c.get("account"))
        if not isinstance(a, str) or not a.strip():
            continue
        dd = r.get(_c.get("data discussion complete"))
        PAX[a.strip()] = dict(
            transition=(None if pd.isna(r.get(_c.get("account transition")))
                        else str(r[_c["account transition"]]).strip()),
            discussion=(None if dd is None or (isinstance(dd, float) and pd.isna(dd))
                        else ("Complete" if bool(dd) else "Not complete")),
            lever=_lever(r.get(_c.get("projected growth?") or _c.get("lever"))))
    _hit = sum(1 for n in acct.account_name if n in PAX)
    print(f"Patient Access matched {_hit} of {len(acct)} accounts")
    for n in acct.account_name:
        if n not in PAX:
            print(f"    no row, will read N/A: {n}")
else:
    print("** no Patient Access file — transition / discussion / lever read N/A")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. AVG days VOB close → Sim, and Account Transition, from the warehouse

# COMMAND ----------

# Both of these were being maintained in a second place -- the AVG days came off the
# All-Accounts CSV and the transition off the Patient Access workbook. Both already exist
# in the HIG monthly query, so this reads them at source.
#
# Two things about the metric, taken verbatim from that query and worth knowing before
# anyone reads column AY:
#
#   THE MONTH IS THE VOB CASE CLOSED MONTH, not the simulation month. A case closed in
#   July whose sim lands in August is a July row.
#
#   A MONTH WITH NOTHING CLOSED PRODUCES NO ROW, not a zero. The pivoted CSV writes 0.0
#   for those, which reads as "closed same day" -- 168 of the CSV's 483 rows for July were
#   that, and every one had zero simulations. Reading natively removes the ambiguity.
#
# Filters are hers, unchanged: days >= 0, and the case closed on or after the treating
# account's go-live.

AVG_SQL = """
WITH src_dtl AS (
  SELECT
    pat.practice                     AS pat_account_id,
    pat_sta.status                   AS pat_account_status,
    pat_sta.go_live_date             AS pat_go_live_date,
    cases.closed_date                AS case_closed_date,
    ptl.Days_VOB_Closed_to_Sim       AS Days_VOB_Closed_to_Sim
  FROM us_scdb_prod.comm_silver.dim_account_dtl act
  LEFT JOIN us_scdb_prod.comm_silver.fact_case_dtl cases
         ON act.account_id = cases.account_id
  LEFT JOIN us_scdb_prod.comm_silver.fact_ptl_dtl ptl
         ON cases.case_id = ptl.vob_case
  LEFT JOIN us_scdb_prod.comm_silver.fact_patient_dtl pat
         ON pat.patient_treatment_log = ptl.ptl_id
        AND pat.date = coalesce(ptl.treatment_date, ptl.sim)
        AND pat.treatment_type IN ('Treatment','New Start','Simulation','End Of Treatment')
  LEFT JOIN us_scdb_prod.comm_silver.dim_account_dtl pat_sta
         ON pat_sta.account_id = pat.practice
  WHERE act.contract_effective_date IS NOT NULL
),
avg_vob_close_sim AS (
  SELECT pat_account_id AS account_id,
         to_date(case_closed_date) AS date,
         Days_VOB_Closed_to_Sim    AS value
  FROM (SELECT DISTINCT pat_account_id, case_closed_date, pat_account_status,
                        pat_go_live_date, Days_VOB_Closed_to_Sim
        FROM src_dtl
        WHERE case_closed_date IS NOT NULL
          AND Days_VOB_Closed_to_Sim >= 0
          AND case_closed_date >= pat_go_live_date)
)
SELECT account_id,
       date_format(date_trunc('month', date), 'yyyy-MM') AS ym,
       avg(value) AS avg_days_vob_close_to_sim,
       count(*)   AS cases_closed
FROM avg_vob_close_sim
WHERE account_id IS NOT NULL
GROUP BY 1, 2
"""

TRANSITION_SQL = """
SELECT account_id,
       CASE WHEN Account_Transition = 'Hold' THEN 'Hold - Amended Program'
            WHEN Account_Transition = 'Normal Operations, Accelerator' THEN 'Normal Operations, CS'
            ELSE Account_Transition END AS account_transition,
       status
FROM us_scdb_prod.comm_silver.dim_account_dtl
"""

V2S, TRANS = {}, {}
V2S_MONTH = f"{W1[:4]}-{int(W1[5:7]) - 1:02d}" if W1[5:7] != "01" else f"{int(W1[:4]) - 1}-12"

# [3 Sep] If 07b now lands the metric in raw.account_rollup, take it from there: it is
# the same figure, already asserted against the metric name, and it means this notebook
# is not a second definition of a number the pipeline already holds. The direct query
# below stays as the fallback for a catalog whose 07b has not been updated yet.
#
# The two are NOT identical and the difference is worth knowing: 07b keys on
# `case_account_id` with no go-live filter (the shape of the monthly HIG query), the
# fallback on `pat_account_id` with `case_closed_date >= pat_go_live_date`. They differ
# at accounts whose case and patient sit on different ids, and in an account's first
# month live. `avg_days_v2s_source` on the feed records which one produced the number.
try:
    _gr = spark.sql(f"""
        SELECT account_id, avg_days_vob_close_to_sim
        FROM {C}.raw.account_rollup
        WHERE month = '{V2S_MONTH}' AND avg_days_vob_close_to_sim IS NOT NULL
    """).toPandas().set_index("account_id")
    if len(_gr):
        for _, a in acct.iterrows():
            vals = [float(_gr.at[i, "avg_days_vob_close_to_sim"])
                    for i in str(a.account_ids).split(";") if i in _gr.index]
            if vals:
                V2S[a.account_name] = round(sum(vals) / len(vals), 1)
        MSRC = "raw.account_rollup"
        print(f"AVG days VOB→Sim from raw.account_rollup for {V2S_MONTH}: "
              f"{len(V2S)} of {len(acct)} accounts")
except Exception:
    pass

# [25 Sep] The transition is read UNCONDITIONALLY. It used to live inside the block
# below, which only runs when the AVG-days figure did NOT come from
# raw.account_rollup. On a run where the rollup supplied AVG days — the normal case
# now — TRANS stayed empty and all 190 accounts printed "N/A" for Account Transition.
# They are independent facts that happened to share a code path.
try:
    _trx = (spark.sql(TRANSITION_SQL).drop_duplicates(["account_id"]).toPandas()
                 .set_index("account_id"))
    for _, a in acct.iterrows():
        t = [str(_trx.at[i, "account_transition"]) for i in str(a.account_ids).split(";")
             if i in _trx.index and pd.notna(_trx.at[i, "account_transition"])]
        if t:
            TRANS[a.account_name] = t[0]
    print(f"account transition from the warehouse: {len(TRANS)} of {len(acct)} accounts")
except Exception as e:
    print(f"** transition read failed ({type(e).__name__}: {e}) — "
          f"Patient Access will be the only source")

if MSRC == "warehouse" and not V2S:
    try:
        _av = spark.sql(AVG_SQL).toPandas()
        _av = _av[_av.ym == V2S_MONTH].set_index("account_id")
        _tr = spark.sql(TRANSITION_SQL).drop_duplicates(["account_id"]).toPandas() \
                   .set_index("account_id")
        # Pooled folders take the STRAIGHT MEAN of their ids, not a weighted one: the
        # two ids are one site doing one body of work, which is why the folder is pooled.
        # Single-id accounts take the value as it stands.
        for _, a in acct.iterrows():
            idl = [i for i in str(a.account_ids).split(";") if i in _av.index]
            vals = [float(_av.at[i, "avg_days_vob_close_to_sim"]) for i in idl
                    if pd.notna(_av.at[i, "avg_days_vob_close_to_sim"])]
            if vals:
                V2S[a.account_name] = round(sum(vals) / len(vals), 1)
            t = [str(_tr.at[i, "account_transition"]) for i in str(a.account_ids).split(";")
                 if i in _tr.index and pd.notna(_tr.at[i, "account_transition"])]
            if t:
                TRANS[a.account_name] = t[0]
        print(f"warehouse: AVG days VOB→Sim for {V2S_MONTH} on {len(V2S)} of {len(acct)} "
              f"accounts; transition on {len(TRANS)}")
        _pool = [a.account_name for _, a in acct.iterrows() if a.accounts_pooled > 1]
        for n in _pool:
            print(f"    pooled, straight mean of {int(acct.loc[acct.account_name == n, 'accounts_pooled'].iloc[0])}"
                  f" ids: {n} = {V2S.get(n)}")
    except Exception as e:
        print(f"** warehouse read failed ({type(e).__name__}: {e}) — falling back to the CSV")
        MSRC = "csv"

if MSRC == "csv" and AA_CSV and os.path.exists(AA_CSV):
    _aa = pd.read_csv(AA_CSV, low_memory=False).set_index("account_id")
    _c, _s = f"AVG_Days_VOB_Closed-Sim_{V2S_MONTH}", f"SIM_{V2S_MONTH}"
    if _c in _aa.columns:
        for _, a in acct.iterrows():
            idl = [i for i in str(a.account_ids).split(";") if i in _aa.index]
            # a zero in the pivoted file means nothing closed, not "closed same day"
            vals = [float(_aa.at[i, _c]) for i in idl
                    if float(_aa.at[i, _c]) > 0 and float(_aa.at[i, _s]) > 0]
            if vals:
                V2S[a.account_name] = round(sum(vals) / len(vals), 1)
        if "Account_Transition" in _aa.columns:
            for _, a in acct.iterrows():
                idl = [i for i in str(a.account_ids).split(";") if i in _aa.index]
                if idl:
                    TRANS[a.account_name] = str(_aa.at[idl[0], "Account_Transition"])
    print(f"csv: AVG days VOB→Sim for {V2S_MONTH} on {len(V2S)} accounts")

_missing = [n for n in acct.account_name if n not in V2S]
print(f"no AVG days figure ({len(_missing)}): " + ("; ".join(_missing[:6]) or "none"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build the row

# COMMAND ----------

FLAT = {"mark": 0.75, "m2v": 0.85, "v2s": 0.70, "n2v": 0.40}
HEADROOM = {"mark": 0.03, "m2v": 0.05, "v2s": 0.03, "n2v": 0.03}

def _goal(kind, actual):
    g = FLAT[kind]
    return actual + HEADROOM[kind] if (actual is not None and actual > g) else g

rows = []
for _, a in acct.iterrows():
    n = a.account_name
    nm = [int(a[f"nmsc_{m}"]) for m in WIN]
    mk = [int(a[f"marked_{m}"]) for m in WIN]
    vb = [int(a[f"sf_vob_{m}"]) for m in WIN]
    sm = [int(a[f"sf_sim_{m}"]) for m in WIN]
    NM, MK, VB, SM = sum(nm), sum(mk), sum(vb), sum(sm)
    is_ipm = (MK == 0 and NM > 0)
    mark = None if is_ipm or not NM else MK / NM
    m2v  = None if is_ipm or not MK else VB / MK
    v2s  = None if not VB else SM / VB
    n2v  = None if not NM else VB / NM
    px   = PAX.get(n, {})
    r = dict(
        run_id=RUN_ID, account_name=n, account_id=a.account_id,
        account_ids=a.account_ids, accounts_pooled=int(a.accounts_pooled),
        regional=a.get("regional"), senior=a.get("senior"),
        practice_group=a.get("practice_group"),
        is_terminated=bool(a.is_terminated),
        kept_despite_terminated=bool(a.kept_despite_terminated),
        window_months=",".join(WIN), growth_month=GM, growth_source=growth_src,
        appropriate_total=NM, marked_total=MK, sf_vob_total=VB, sf_sim_total=SM,
        appropriate_per_month=round(NM / N), marked_per_month=None if is_ipm else round(MK / N),
        vob_per_month=round(VB / N), sim_per_month=round(SM / N),
        growth_vob=g_vob.get(n, 0), growth_sim=g_sim.get(n, 0),
        growth_vob_delta=g_vob.get(n, 0) - vb[-1], growth_sim_delta=g_sim.get(n, 0) - sm[-1],
        rate_mark=mark, rate_m2v=m2v, rate_v2s=v2s, rate_n2v=n2v,
        rate_n2s=None if None in (mark, m2v, v2s) else mark * m2v * v2s,
        goal_mark=_goal("mark", mark), goal_m2v=_goal("m2v", m2v), goal_v2s=_goal("v2s", v2s),
        flat_goal_mark=FLAT["mark"], flat_goal_m2v=FLAT["m2v"], flat_goal_v2s=FLAT["v2s"],
        flat_goal_n2v=FLAT["n2v"], flat_goal_n2s=FLAT["mark"] * FLAT["m2v"] * FLAT["v2s"],
        is_ipm=is_ipm, ipm_log=(a.get("vob_pattern_band") == "IPM"),
        vob_pattern_band=a.get("vob_pattern_band"),
        actions_coverage=a.get("actions_coverage"),
        avg_days_result_to_vob=a.get("avg_days_result_to_vob"),
        avg_days_biopsy_to_vob=a.get("avg_days_biopsy_to_vob"),
        avg_days_vob_close_to_sim=V2S.get(n),
        avg_days_v2s_month=V2S_MONTH,
        # [3 Sep, Becky] THE WAREHOUSE IS AUTHORITATIVE for the transition. It carries
        # the live value from dim_account_dtl through the normalising CASE ('Hold' ->
        # 'Hold - Amended Program', 'Normal Operations, Accelerator' -> 'Normal
        # Operations, CS'), so it is current by construction; the Patient Access
        # workbook is a snapshot someone maintains by hand. Patient Access is left as a
        # fallback for an account the warehouse has no row for, and the source is
        # recorded so a mixed column is never a guess.
        #
        # The two use different words for the same states -- warehouse "Normal
        # Operations, CS" against Patient Access "Normal Operations - PCA Program" --
        # so switching the precedence CHANGES WHAT PRINTS ON THE CARDS.
        account_transition=TRANS.get(n) or px.get("transition") or "N/A",
        account_transition_source=("warehouse" if TRANS.get(n)
                                   else ("patient_access" if px.get("transition") else "none")),
        data_discussion=px.get("discussion") or "N/A",
        projected_lever=px.get("lever"))
    for i, m in enumerate(WIN):
        r[f"nmsc_{m}"], r[f"marked_{m}"] = nm[i], mk[i]
        r[f"sf_vob_{m}"], r[f"sf_sim_{m}"] = vb[i], sm[i]
    rows.append(r)

feed = pd.DataFrame(rows)
print(f"deck_feed: {len(feed)} rows × {len(feed.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Regional rollup

# COMMAND ----------

reg = (feed.groupby(["senior", "regional"], dropna=False)
       .agg(accounts=("account_name", "count"),
            ipm_log_accounts=("ipm_log", "sum"),
            appropriate_per_month=("appropriate_total", lambda s: round(s.sum() / N)),
            vob_per_month=("sf_vob_total", lambda s: round(s.sum() / N)),
            sim_per_month=("sf_sim_total", lambda s: round(s.sum() / N)))
       .reset_index())
reg["vob_pct_of_appropriate"] = reg.vob_per_month / reg.appropriate_per_month
reg["sim_pct_of_vob"] = reg.sim_per_month / reg.vob_per_month
reg.insert(0, "run_id", RUN_ID)
print(reg.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Provenance

# COMMAND ----------

def _asof(path):
    if path and os.path.exists(path):
        return _dt.datetime.utcfromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M UTC")
    return "(not supplied)"

prov = pd.DataFrame([
    dict(item="run_id", value=RUN_ID, note="UTC, when this notebook ran"),
    dict(item="catalog", value=C, note="dev or prod"),
    dict(item="reporting window", value=f"{W0} → {W1} ({', '.join(WIN)})", note="exclusive of end"),
    dict(item="growth month", value=GM_YM, note=f"taken from {growth_src}"),
    dict(item="all_accounts_csv", value=os.path.basename(AA_CSV) or "(none)", note=_asof(AA_CSV)),
    dict(item="patient_access_xlsx", value=os.path.basename(PA_XLS) or "(none)", note=_asof(PA_XLS)),
    dict(item="gold.account_summary", value=f"{len(acct)} accounts", note=f"months {', '.join(MONTHS)}"),
    dict(item="roster", value=f"{len(feed)} accounts",
         note=f"kept terminated: {', '.join(KEEP)}"),
    dict(item="AVG days VOB→Sim", value=f"{V2S_MONTH}, {len(V2S)} accounts",
         note=f"source: {MSRC}. Month is the VOB CASE CLOSED month, not the sim month."),
    dict(item="account transition",
         value=f"{int((feed.account_transition_source == 'warehouse').sum())} warehouse / "
               f"{int((feed.account_transition_source == 'patient_access').sum())} Patient Access / "
               f"{int((feed.account_transition_source == 'none').sum())} none",
         note="warehouse is authoritative; Patient Access fills only what it has no row for"),
])
print(prov.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write

# COMMAND ----------

# [25 Sep] openpyxl writes through the local filesystem, and the UC Volumes FUSE mount
# does not reliably accept a create from zipfile. Build on local disk, then copy across
# with dbutils.fs, which speaks to Volumes properly. tempfile picks a directory that is
# writable on whatever runtime this is — /local_disk0 and /tmp are not both available.
import tempfile

STAMP = f"{W0}_to_{W1}"
_dir  = f"{OUT}/{STAMP}"
_name = f"deck_feed_{RUN_ID}.xlsx"
_xlsx = f"{_dir}/{_name}"

_scratch = tempfile.mkdtemp(prefix="deck_")
_tmp     = os.path.join(_scratch, _name)
with pd.ExcelWriter(_tmp, engine="openpyxl") as xw:
    feed.to_excel(xw, sheet_name="accounts", index=False)
    reg.to_excel(xw, sheet_name="regional", index=False)
    prov.to_excel(xw, sheet_name="sources", index=False)
print(f"built locally: {_tmp} ({os.path.getsize(_tmp):,} bytes)")

try:
    dbutils.fs.mkdirs(_dir)
except Exception as e:
    print(f"  note: could not mkdirs {_dir} — {e}")
try:
    dbutils.fs.cp(f"file:{_tmp}", _xlsx)
    print(f"wrote {_xlsx}")
except Exception as e:
    import shutil
    try:
        os.makedirs(_dir, exist_ok=True)
        shutil.copyfile(_tmp, _xlsx)
        print(f"wrote {_xlsx} (shutil)")
    except Exception as e2:
        raise RuntimeError(
            f"Built the feed at {_tmp} — it is complete, only the copy failed.\n"
            f"Could not place it at {_xlsx}.\n"
            f"  dbutils.fs.cp said: {e}\n  shutil said       : {e2}\n"
            f"out_volume must reach INTO a volume: /Volumes/<catalog>/<schema>/<volume>")

if not WRITE:
    print("DRY RUN — gold.deck_feed not written. Review above, then set write = yes.")
else:
    spark.createDataFrame(feed).write.mode("overwrite").option(
        "overwriteSchema", "true").saveAsTable(FEED)
    spark.createDataFrame(reg).write.mode("overwrite").option(
        "overwriteSchema", "true").saveAsTable(f"{C}.gold.deck_feed_regional")
    print(f"wrote {FEED}: {spark.table(FEED).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Gates

# COMMAND ----------

/Volumes/dev_pathology_data/gold/exports/deck_data_2026_08.xlsx_fail = []
if feed.account_name.duplicated().any():
    _fail.append("an account appears twice — the pooled id sets are overlapping")
if (feed.accounts_pooled > 1).sum() != int((acct.accounts_pooled > 1).sum()):
    _fail.append("pooled count changed between the roster and the feed")
_ipm_bad = feed[(~feed.is_ipm) & (feed.rate_mark.isna()) & (feed.appropriate_total > 0)]
if len(_ipm_bad):
    _fail.append(f"{len(_ipm_bad)} account(s) have appropriate days but no mark rate "
                 "and are not flagged IPM")
if feed.growth_month.nunique() != 1:
    _fail.append("more than one growth month in the feed")
_v2s = feed.avg_days_vob_close_to_sim.dropna()
if len(_v2s) and (_v2s < 0).any():
    _fail.append("a negative AVG days VOB→Sim reached the feed — the source filters "
                 "days >= 0, so this means the join changed")
if len(_v2s) < 0.5 * len(feed):
    _fail.append(f"only {len(_v2s)} of {len(feed)} accounts have an AVG days figure for "
                 f"{V2S_MONTH} — check the month and the metrics_source widget")

if _fail:
    for f in _fail:
        print(f"*** {f}")
    raise AssertionError(f"{len(_fail)} gate(s) failed — do not build the deck from this feed")
print("all gates passed")
print(f"\nNEXT: download {os.path.basename(_xlsx)}, then locally:")
print("  python3 tpl/feed_to_export.py <feed.xlsx> -o export/")
print("      → writes the three CSVs the deck scripts already read, and prints the")
print("        as-of date of every source. It does NOT write the PHI exports; those")
print("        stay with notebook 10.")
print("  then the three steps feed_to_export.py prints, then build_deck.js")