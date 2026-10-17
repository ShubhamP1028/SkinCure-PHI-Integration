# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — 99: QA

# COMMAND ----------

# [22 Sep] Loaded so the extract stage can parse a bronze date with the SAME
# date_parse_sql the pipeline uses. Re-implementing the format list here is how the
# check and the thing it checks drift apart.

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("window_start", "2026-05-01", "Window start (inclusive)")
dbutils.widgets.text("window_end", "2026-09-01", "Window end (exclusive)")
dbutils.widgets.text("growth_month", "2026-09", "Growth month")
dbutils.widgets.text("all_accounts_csv", "", "All-Accounts YTD export (volume path)")
dbutils.widgets.text("watchlist_count", "0", "Expected account count (0 = skip)")
dbutils.widgets.dropdown("stage", "all",
                         ["all", "preflight", "extract", "patient_days", "match", "gold", "growth"],
                         "Stage")
dbutils.widgets.dropdown("write", "no", ["no", "yes"], "Write gold.qa_results")

C      = dbutils.widgets.get("catalog")
W0     = dbutils.widgets.get("window_start")
W1     = dbutils.widgets.get("window_end")
GM_YM  = dbutils.widgets.get("growth_month").strip()
AA_CSV = dbutils.widgets.get("all_accounts_csv").strip()
WATCH  = int(dbutils.widgets.get("watchlist_count") or 0)
STAGE  = dbutils.widgets.get("stage")
WRITE  = dbutils.widgets.get("write") == "yes"

import datetime as _dt, os, re
import pandas as pd

RUN_ID = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
print(f"{C} · {W0} → {W1} · growth {GM_YM} · stage {STAGE} · run {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harness

# COMMAND ----------

RESULTS = []

def check(stage, name, severity, ok, detail="", fix=""):
    """severity: BLOCK stops the run, WARN is a look, INFO is a number worth seeing."""
    RESULTS.append(dict(run_id=RUN_ID, stage=stage, check=name, severity=severity,
                        passed=bool(ok), detail=str(detail)[:900], fix=fix))
    mark = "ok  " if ok else ("FAIL" if severity == "BLOCK" else "warn")
    print(f"[{mark}] {stage:12} {name:38} {detail}")
    if not ok and fix:
        print(f"         → {fix}")

def q(sql):
    return spark.sql(sql).toPandas()

def table_exists(t):
    """[22 Sep] spark.table() is LAZY under Spark Connect (DBR 19.x): it returns an
    unresolved plan instead of raising, so the try/except never fired and this
    returned True for a table that does not exist. The caller then died on the next
    line's .toPandas() with TABLE_OR_VIEW_NOT_FOUND -- a guard that reported the
    opposite of the truth. tableExists resolves against the catalog.
    """
    try:
        return spark.catalog.tableExists(t)
    except Exception:
        return False


def first_table(*cands):
    """The first of these that exists, or None. The extract stage was pinned to
    staged.lesions, which does not exist in this catalog -- so with the guard broken
    it errored, and with the guard fixed it would simply never run. Five checks were
    dead either way, including the biopsy_date parse gate written after the August
    defect that silently failed 156,217 of 157,000 rows.
    """
    for t in cands:
        if table_exists(t):
            return t
    return None


def col_of(tbl, *cands):
    """First of these column names present on tbl, else None. Named rather than
    positional so a schema change degrades to a skipped check with a reason, not a
    wrong number or a crash."""
    have = {c.lower() for c in spark.table(tbl).columns}
    for c in cands:
        if c.lower() in have:
            return c
    return None

RUN = lambda s: STAGE in ("all", s)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pre-flight — the ingest manifest

# COMMAND ----------

if RUN("preflight") and table_exists(f"{C}.raw.file_manifest"):
    m = q(f"SELECT * FROM {C}.raw.file_manifest")

    # An OK or EMPTY row is terminal, so 01 never revisits it. A file that was staged,
    # marked, and then never produced rows is invisible without this. Sep 2026: 6
    # accounts, ~4,000 rows, Boone recovered 2,802 of 3,116.
    if {"status", "rows"} <= set(m.columns):
        orph = m[(m.status.isin(["OK", "EMPTY"])) & (m.rows.fillna(0) == 0)]
        check("preflight", "orphaned manifest rows", "BLOCK", len(orph) == 0,
              f"{len(orph)} file(s) terminal with zero rows",
              "DELETE those rows from raw.file_manifest and re-run 01")

    if "sha256" in m.columns:
        dup = m.groupby("sha256").size()
        dup = dup[dup > 1]
        check("preflight", "byte-identical files staged twice", "WARN", len(dup) == 0,
              f"{len(dup)} hash(es) appear more than once",
              "cross-part re-send or a byte-identical sibling — keep one")

    if "bytes" in m.columns:
        check("preflight", "zero-byte files", "BLOCK", int((m.bytes == 0).sum()) == 0,
              f"{int((m.bytes == 0).sum())} file(s)")

    if WATCH:
        n = m.account.nunique() if "account" in m.columns else 0
        check("preflight", "account count vs watchlist", "BLOCK", n == WATCH,
              f"{n} staged against {WATCH} expected")
else:
    print("(skipped — no raw.file_manifest or stage filtered)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract

# COMMAND ----------

LES = first_table(f"{C}.staged.lesions", f"{C}.staged.lesion_classified")

if RUN("extract") and LES:
    # Column names differ between the two candidate tables, so resolve them rather
    # than assuming. A missing one skips ITS check with a reason printed.
    c_acct = col_of(LES, "account", "account_name")
    c_file = col_of(LES, "source_file", "file_name")
    c_pat  = col_of(LES, "patient", "patient_name")
    c_fac  = col_of(LES, "facility")
    c_date = col_of(LES, "biopsy_date")
    c_raw  = col_of(LES, "collected_date")
    # biopsy_date is already a DATE on staged.lesions; on lesion_classified the date
    # is the raw bronze string, so it has to go through the pipeline's own parser.
    date_expr = c_date if c_date else (date_parse_sql(c_raw) if c_raw else None)
    print(f"extract stage reading {LES}")
    print(f"  account={c_acct} file={c_file} patient={c_pat} facility={c_fac} "
          f"date={c_date or ('date_parse_sql(' + str(c_raw) + ')')}")

    if not (c_acct and date_expr):
        check("extract", "extract checks runnable", "WARN", False,
              f"{LES} has no usable account/date column — extract checks skipped")
    else:
        L = q(f"""SELECT {c_acct} AS account, count(*) rows,
                         {f'count(DISTINCT {c_file})' if c_file else 'CAST(NULL AS BIGINT)'} files,
                         min({date_expr}) lo, max({date_expr}) hi,
                         sum(CASE WHEN {date_expr} IS NULL THEN 1 ELSE 0 END) no_date
                  FROM {LES} GROUP BY {c_acct}""")

        check("extract", "every account produced rows", "BLOCK", (L.rows > 0).all(),
              f"{int((L.rows == 0).sum())} account(s) with none")

        # A 91-92 day span against a 121-day window is a trailing-window export: the
        # practice sent the last three months, not the window we asked for.
        L["span"] = (pd.to_datetime(L.hi) - pd.to_datetime(L.lo)).dt.days
        exp = (pd.to_datetime(W1) - pd.to_datetime(W0)).days
        short = L[L.span < exp - 20]
        check("extract", "in-window span per account", "WARN", len(short) == 0,
              f"{len(short)} account(s) span < {exp - 20}d against a {exp}d window",
              "trailing-window export — ask for the window, not the last N months")

        # The date-parse defect of Aug 2026: dropping the bare try_to_date arm silently
        # failed 156,217 of 157,000 rows. A high null rate here is that class of bug.
        #
        # NOTE this counts EVERY row in the table, not just the window -- an unparsed
        # date has no month, so it cannot be windowed out. That is the point.
        tot, nul = int(L.rows.sum()), int(L.no_date.sum())
        check("extract", "biopsy_date parsed", "BLOCK", nul / max(tot, 1) < 0.02,
              f"{nul} of {tot} null ({nul/max(tot,1):.2%})",
              "check date_parse_sql coalesces a BARE try_to_date(c) first, then the formats")

        if c_pat:
            shortnm = q(f"""SELECT count(*) n FROM {LES}
                            WHERE length(regexp_replace({c_pat}, '[^A-Za-z]', '')) < 4""")
            check("extract", "names under 4 letters", "WARN", int(shortnm.n[0]) == 0,
                  f"{int(shortnm.n[0])} row(s)", "OCR name loss — re-OCR at 300 DPI")

        if c_fac:
            fac = q(f"""SELECT {c_acct} AS account, count(DISTINCT {c_fac}) f FROM {LES}
                        WHERE {c_fac} IS NOT NULL GROUP BY {c_acct} HAVING f > 1""")
            check("extract", "single facility per account", "WARN", len(fac) == 0,
                  f"{len(fac)} account(s) with more than one facility",
                  "each needs a FULL / MARKED ONLY / DISREGARD decision in the rule sheet")
elif RUN("extract"):
    check("extract", "lesion table found", "WARN", False,
          f"neither {C}.staged.lesions nor {C}.staged.lesion_classified exists — "
          "every extract check skipped")
else:
    print("(skipped — stage filtered)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Patient days

# COMMAND ----------

PDT = f"{C}.staged.patient_days"

if RUN("patient_days") and table_exists(PDT):
    # [22 Sep] Column names resolved, not assumed. This block was written against
    # `account` / `is_appropriate` / `is_marked`; 03 writes `account_name` /
    # `appropriate` / `marked`. Every query here raised on the first column it hit.
    p_acct = col_of(PDT, "account_name", "account")
    p_pid  = col_of(PDT, "patient_id")
    p_date = col_of(PDT, "biopsy_date")
    p_appr = col_of(PDT, "appropriate", "is_appropriate")
    p_mark = col_of(PDT, "marked", "is_marked")
    print(f"patient_days: account={p_acct} patient={p_pid} date={p_date} "
          f"appropriate={p_appr} marked={p_mark}")

    # The grain. Everything published is a patient-day; a duplicate here multiplies
    # every downstream count.
    #
    # NOTE the bracket access. `g.keys` is pandas' DataFrame.keys METHOD, so g.keys[0]
    # subscripts a bound method and raises TypeError. Any alias that collides with a
    # DataFrame attribute has to be read as g["keys"] -- attribute access silently
    # picks the method instead of the column.
    g = q(f"""SELECT count(*) AS rows,
                     count(DISTINCT concat_ws('|', {p_acct}, {p_pid},
                           cast({p_date} AS string))) AS keys
              FROM {PDT}""")
    check("patient_days", "grain is (account, patient, biopsy_date)", "BLOCK",
          int(g["rows"][0]) == int(g["keys"][0]),
          f'{int(g["rows"][0])} rows against {int(g["keys"][0])} distinct keys')

    check("patient_days", "no null account", "BLOCK",
          int(q(f"SELECT count(*) AS n FROM {PDT} WHERE {p_acct} IS NULL")["n"][0]) == 0,
          "rows with no account reach gold and render as a blank row")

    mk = q(f"""SELECT {p_acct} AS account,
                      sum(CASE WHEN {p_appr} THEN 1 ELSE 0 END) AS appr,
                      sum(CASE WHEN {p_mark} THEN 1 ELSE 0 END) AS mk
               FROM {PDT}
               WHERE {p_date} >= '{W0}' AND {p_date} < '{W1}'
               GROUP BY {p_acct}""")
    bad = mk[mk["mk"] > mk["appr"]]
    check("patient_days", "marked never exceeds appropriate", "BLOCK", len(bad) == 0,
          f"{len(bad)} account(s)" + ("" if len(bad) == 0 else
              ": " + ", ".join(bad["account"].head(5))),
          "a marked day that is not appropriate is a classifier bug")

    blanket = mk[(mk["appr"] > 0) & (mk["mk"] == mk["appr"])]
    check("patient_days", "Blanket accounts (marked == appropriate)", "INFO", True,
          f"{len(blanket)}",
          "v6.2 §5.1's Marked → VOB inversion keys on this set — it was 10 in Sep, "
          "not the 12 the instructions assume")

    # [22 Sep] FOLDER_CANON should leave exactly ONE account per practice. Two names
    # that differ only by punctuation or spacing is the Iconic Dermatology shape --
    # split patient-days here, and a double-counted AccountId in 06.
    nk = q(f"""SELECT lower(regexp_replace({p_acct}, '[^A-Za-z0-9]', '')) AS k,
                      count(DISTINCT {p_acct}) AS n,
                      concat_ws(' | ', array_sort(collect_set({p_acct}))) AS names
               FROM {PDT} GROUP BY 1 HAVING count(DISTINCT {p_acct}) > 1""")
    check("patient_days", "one folder name per practice", "BLOCK", len(nk) == 0,
          f"{len(nk)} practice(s) under two names" +
          ("" if len(nk) == 0 else ": " + "; ".join(nk["names"].head(5))),
          "add the stray spelling to FOLDER_CANON in 00_vocab, then re-run 03")
else:
    print("(skipped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Match

# COMMAND ----------

if RUN("match") and table_exists(f"{C}.gold.vob_scope"):
    vs = q(f"SELECT * FROM {C}.gold.vob_scope")

    # The one that must never fail: a sister match moves the BIOPSY attribution, never
    # the VOB. Sep 2026: 17,510 distinct vob_id across exactly 17,510 pairs.
    pairs = vs[["account", "vob_id"]].drop_duplicates()
    check("match", "no VOB credited at two accounts", "BLOCK",
          vs.vob_id.nunique() == len(pairs),
          f"{vs.vob_id.nunique()} distinct vob_id across {len(pairs)} account-VOB pairs")

    print(vs.match_scope.value_counts().to_string())

    # Reported, never gated. Some practices raise the VOB before the biopsy -- 66 of 182
    # accounts show it in own-site data -- so a date test would delete real matches.
    v = vs.copy()
    v["lag"] = (pd.to_datetime(v.vob_created, errors="coerce").dt.tz_localize(None)
                - pd.to_datetime(v.cohort_biopsy_date, errors="coerce")).dt.days
    for sc in ("own site", "sister site", "enterprise"):
        x = v[(v.match_scope == sc) & v.lag.notna()]
        if len(x):
            check("match", f"biopsy-after-VOB, {sc}", "INFO", True,
                  f"{int((x.lag < 0).sum())} of {len(x)} ({(x.lag < 0).mean():.1%}), "
                  f"median lag {x.lag.median():.0f}d")
    ent = v[(v.match_scope == "enterprise") & v.lag.notna()]
    own = v[(v.match_scope == "own site") & v.lag.notna()]
    if len(ent) and len(own):
        check("match", "enterprise vs own-site chronology", "WARN",
              (ent.lag < 0).mean() < 3 * max((own.lag < 0).mean(), 0.001),
              f"enterprise {(ent.lag < 0).mean():.1%} against own-site {(own.lag < 0).mean():.1%}",
              "a large gap means the enterprise branch is matching loosely; investigate "
              "the branch, but do NOT gate on the biopsy date")

    # One biopsy date carrying a disproportionate share of enterprise matches is the
    # 2026-04-02 shape: 22 of 27 pointing at one Central Missouri clinic day.
    if len(ent):
        top = ent.cohort_biopsy_date.value_counts()
        if len(top):
            share = top.iloc[0] / len(ent)
            check("match", "enterprise matches concentrated on one date", "WARN", share < 0.04,
                  f"{top.index[0]} carries {top.iloc[0]} of {len(ent)} ({share:.1%})")

    pool = q(f"""SELECT account, count(DISTINCT account_id) ids FROM {C}.gold.vob_scope
                 GROUP BY account HAVING ids > 1""")
    check("match", "pooled folders", "INFO", True, f"{len(pool)} folder(s) with 2+ AccountIds",
          "each must SUM its ids downstream — joining on the primary halves it")
else:
    print("(skipped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold

# COMMAND ----------

if RUN("gold") and table_exists(f"{C}.gold.account_summary"):
    cols = set(spark.table(f"{C}.gold.account_summary").columns)
    months = sorted([m.group(1) for m in (re.match(r"^nmsc_([A-Z][a-z]{2})$", c) for c in cols) if m],
                    key=ORDER.index)
    check("gold", "month columns are NAMED not positional", "BLOCK", bool(months),
          f"found {months or 'nmsc_m1..m4'}",
          "prod/07_gold still emits the positional form; nothing downstream reads it")

    s = q(f"SELECT * FROM {C}.gold.account_summary")
    check("gold", "account count", "INFO", True, f"{len(s)}")
    if WATCH:
        check("gold", "reconciles to watchlist", "BLOCK", len(s) == WATCH,
              f"{len(s)} against {WATCH}")

    check("gold", "no null account_name", "BLOCK", int(s.account_name.isna().sum()) == 0,
          f"{int(s.account_name.isna().sum())} row(s)")
    check("gold", "no duplicate account_name", "BLOCK", not s.account_name.duplicated().any(),
          f"{int(s.account_name.duplicated().sum())} duplicate(s)")

    if months:
        _ap = s[[f"nmsc_{m}" for m in months]].sum(axis=1)
        _mk = s[[f"marked_{m}" for m in months]].sum(axis=1)
        over = s[(_ap > 0) & (_mk > _ap)]
        check("gold", "Marked / Appropriate ≤ 100%", "BLOCK", len(over) == 0,
              f"{len(over)} account(s) over")

        _vb = s[[f"sf_vob_{m}" for m in months]].sum(axis=1)
        hi = s[(_mk > 0) & (_vb / _mk > 1.0)]
        check("gold", "Marked → VOB over 100%", "INFO", True,
              f"{len(hi)} account(s) — expected by design at treatment sites")

    if "vob_pattern_band" in s.columns:
        print(s.vob_pattern_band.value_counts().to_string())
else:
    print("(skipped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. AVG days VOB close → Sim

# COMMAND ----------

if RUN("gold") and table_exists(f"{C}.raw.account_rollup"):
    _c = set(spark.table(f"{C}.raw.account_rollup").columns)
    check("gold", "raw.account_rollup carries avg_days_vob_close_to_sim", "WARN",
          "avg_days_vob_close_to_sim" in _c,
          "present" if "avg_days_vob_close_to_sim" in _c else "absent",
          "07b builds it but the SELECT in its section 6 lists columns by name — add it "
          "there or it stops at the temp view")
    if "avg_days_vob_close_to_sim" in _c:
        v = q(f"""SELECT count(*) rows,
                         sum(CASE WHEN avg_days_vob_close_to_sim < 0 THEN 1 ELSE 0 END) neg,
                         sum(CASE WHEN avg_days_vob_close_to_sim = 0 THEN 1 ELSE 0 END) zero,
                         sum(CASE WHEN avg_days_vob_close_to_sim IS NULL THEN 1 ELSE 0 END) nul
                  FROM {C}.raw.account_rollup""")
        check("gold", "no negative VOB→Sim average", "BLOCK", int(v.neg[0]) == 0,
              f"{int(v.neg[0])} row(s)",
              "the source filters days >= 0, so a negative means the join changed")
        # a zero is the tell that a coalesce crept back in: nothing closed should be NULL
        check("gold", "nothing-closed months are NULL, not 0", "WARN", int(v.zero[0]) == 0,
              f"{int(v.zero[0])} row(s) read exactly 0 of {int(v.rows[0])}",
              "a 0 reads as 'closed same day'. Drop the coalesce(..., 0) in 07b")
        check("gold", "VOB→Sim coverage", "INFO", True,
              f"{int(v.rows[0]) - int(v.nul[0])} of {int(v.rows[0])} account-months valued")

    # and that it survived the two joins between raw and the CSV the workbook reads
    if table_exists(f"{C}.gold.account_summary"):
        _a = set(spark.table(f"{C}.gold.account_summary").columns)
        check("gold", "account_summary carries avg_days_vob_close_to_sim", "WARN",
              "avg_days_vob_close_to_sim" in _a,
              "present" if "avg_days_vob_close_to_sim" in _a else "absent",
              "08 selects sf_rollup columns by name -- if this is absent the workbook "
              "falls back to the hand-pasted All-Accounts values")
        if "avg_days_vob_close_to_sim" in _a:
            w = q(f"""SELECT count(*) n, count(avg_days_vob_close_to_sim) have,
                             round(avg(avg_days_vob_close_to_sim), 1) mean,
                             max(avg_days_vob_close_to_sim) hi
                      FROM {C}.gold.account_summary""")
            check("gold", "column AY populated", "INFO", True,
                  f'{int(w["have"][0])} of {int(w["n"][0])} accounts · mean '
                  f'{w["mean"][0]} days · max {w["hi"][0]}')   # w["mean"]: DataFrame.mean is a METHOD
else:
    print("(skipped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Growth month — the September lesson

# COMMAND ----------

# gold.trend is only as complete as the Salesforce pull behind it. In September the
# growth month was 2,511 verifications against the report's 3,284 -- 773 short across
# 166 accounts -- and it moved 172 of 182 published growth figures. The tell is that the
# LAST WINDOW MONTH should already agree; if it does not, the pull is short.

if RUN("growth") and AA_CSV and os.path.exists(AA_CSV) and table_exists(f"{C}.gold.account_summary"):
    aa = pd.read_csv(AA_CSV, low_memory=False).set_index("account_id")
    ids = q(f"""SELECT account, concat_ws(';', collect_set(account_id)) ids
                FROM {C}.gold.vob_scope GROUP BY account""").set_index("account")
    s = q(f"SELECT * FROM {C}.gold.account_summary")
    cols = set(s.columns)
    months = sorted([m.group(1) for m in (re.match(r"^nmsc_([A-Z][a-z]{2})$", c) for c in cols) if m],
                    key=ORDER.index)
    win = [m for m in months if int(W0[5:7]) <= ORDER.index(m) + 1 < int(W1[5:7])]

    def report_sum(col):
        t = 0
        for _, a in s.iterrows():
            idl = [i for i in str(ids.ids.get(a.account_name, a.account_id)).split(";")
                   if i in aa.index]
            if idl:
                t += int(aa.loc[idl, col].sum())
        return t

    for m in win[-2:]:
        ym = f"2026-{ORDER.index(m)+1:02d}"
        if f"VOB_{ym}" not in aa.columns:
            continue
        p, r = int(s[f"sf_vob_{m}"].sum()), report_sum(f"VOB_{ym}")
        gap = p - r
        check("growth", f"{m} VOB pipeline vs report", "BLOCK",
              abs(gap) <= max(5, 0.01 * max(r, 1)),
              f"pipeline {p} vs report {r}, gap {gap:+d}",
              "the Salesforce pull behind gold.trend is short for this month — re-pull "
              "before publishing anything that quotes month-over-month growth")

    if f"VOB_{GM_YM}" in aa.columns and table_exists(f"{C}.gold.trend"):
        t = q(f"SELECT sum(sf_vob) v, sum(sf_sim) s FROM {C}.gold.trend WHERE month = '{GM_YM}'")
        pv, rv = int(t.v[0] or 0), report_sum(f"VOB_{GM_YM}")
        check("growth", f"{GM_YM} growth month completeness", "WARN",
              abs(pv - rv) <= max(10, 0.02 * max(rv, 1)),
              f"trend {pv} vs report {rv}, gap {pv - rv:+d}",
              "take the growth month from the report (notebook 10 does this), not trend")
else:
    print("(skipped — supply all_accounts_csv to run the growth checks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verdict

# COMMAND ----------

res = pd.DataFrame(RESULTS)
if len(res):
    blocks = res[(~res.passed) & (res.severity == "BLOCK")]
    warns  = res[(~res.passed) & (res.severity == "WARN")]
    print("=" * 76)
    print(f"{len(res)} checks · {int(res.passed.sum())} passed · "
          f"{len(blocks)} BLOCKING · {len(warns)} warnings")
    print("=" * 76)
    for _, r in blocks.iterrows():
        print(f"BLOCK  {r.stage:12} {r.check}: {r.detail}")
    for _, r in warns.iterrows():
        print(f"warn   {r.stage:12} {r.check}: {r.detail}")

    if WRITE:
        spark.createDataFrame(res).write.mode("append").saveAsTable(f"{C}.gold.qa_results")
        print(f"\nappended {len(res)} rows to {C}.gold.qa_results")
    else:
        print("\nDRY RUN — gold.qa_results not written.")

    if len(blocks):
        raise AssertionError(f"{len(blocks)} blocking check(s) failed — do not publish")
    print("\nNo blocking failures.")
    print("Still to do OUTSIDE Databricks: build the workbook, then recalc.py must "
          "report ZERO formula errors (v6.2 §7.1). That gate cannot run here — "
          "serverless has no LibreOffice.")
else:
    print("no checks ran — widen the stage widget")

# COMMAND ----------

# %run ./00_vocab   (already run at the top of 99_qa)

_LC = f"{CATALOG}.staged.lesion_classified"

_guard = spark.sql(f"""
WITH lesion AS (
  SELECT
    account_name,
    patient_id,
    biopsy_date,
    lesion_appropriate,
    srt_marked,
    -- a suppression only COUNTS if the lesion would otherwise have been
    -- appropriate: path_class acceptable, not already killed by an older rule,
    -- and not rescued by the SRT backstop
    (path_class = 'acceptable' AND NOT srt_marked
       AND NOT is_plan_removed AND NOT is_never_appropriate) AS _eligible,
    is_cleared_plan, is_cleared_comment, is_removed_at_biopsy, is_deceased
  FROM {_LC}
),
tagged AS (
  SELECT *,
    (_eligible AND (is_cleared_plan OR is_cleared_comment
                    OR is_removed_at_biopsy OR is_deceased)) AS _suppressed
  FROM lesion
),
day AS (
  SELECT account_name, patient_id, biopsy_date,
         bool_or(lesion_appropriate)       AS still_appropriate,
         bool_or(_suppressed)              AS had_suppression
  FROM tagged GROUP BY 1,2,3
)
SELECT
  account_name,
  count(*)                                                        AS days_seen,
  sum(CASE WHEN still_appropriate THEN 1 ELSE 0 END)              AS appropriate_now,
  sum(CASE WHEN had_suppression AND NOT still_appropriate
           THEN 1 ELSE 0 END)                                     AS days_lost_to_rules
FROM day
GROUP BY 1
""").toPandas()

_guard["denominator_before"] = _guard.appropriate_now + _guard.days_lost_to_rules
_guard["pct_lost"] = (_guard.days_lost_to_rules
                      / _guard.denominator_before.replace(0, 1) * 100).round(1)
_guard = _guard.sort_values("pct_lost", ascending=False)

_touched = _guard[_guard.days_lost_to_rules > 0]
_global = _guard.days_lost_to_rules.sum() / max(_guard.denominator_before.sum(), 1) * 100

print(f"rule suppression: {_guard.days_lost_to_rules.sum():,} patient-days "
      f"({_global:.2f}% global) across {len(_touched)} of {len(_guard)} accounts")

# WARN, not BLOCK. A big single-account move is usually CORRECT -- Rixis is one
# canned template fired 42 times -- but §7 of the project instructions requires
# every large movement versus the prior master to carry an explanation, and this
# is where you find out which accounts need one.
_OVER = _touched[_touched.pct_lost > 5]
if len(_OVER):
    print(f"\nWARN · {len(_OVER)} account(s) lose more than 5% of their "
          f"appropriate patient-days to rules. Each needs an explanation line "
          f"in the master before Pam's Summary ships:")
    print(_OVER[["account_name", "denominator_before", "appropriate_now",
                 "days_lost_to_rules", "pct_lost"]].to_string(index=False))
else:
    print("\nOK · no account loses more than 5% of its denominator to rules")

# BLOCK. Every rule sits inside `OR srt_marked`, so a marked day can never be
# suppressed. If this fires, a rule has been wired OUTSIDE the backstop and the
# marked counts -- the numbers the therapists are measured on -- have moved.
_mk = spark.sql(f"""
SELECT count(*) AS n FROM (
  SELECT patient_id, biopsy_date, account_name
  FROM {_LC}
  WHERE srt_marked AND NOT is_never_appropriate AND NOT lesion_appropriate
)""").first()["n"]
assert _mk == 0, (
    f"{_mk} SRT-marked lesions are no longer appropriate. Every rule belongs "
    f"INSIDE `OR srt_marked`; one has been wired outside it and marked "
    f"patient-days have moved. Check lesion_appropriate in 00_vocab.")
print("OK · no SRT-marked lesion was suppressed by any rule")


# COMMAND ----------

# MAGIC %md
# MAGIC ## [24 Sep] One patient, one identity per day

# COMMAND ----------

# The patient key falls back to the NAME when a lesion row has no usable accession.
# Before 24 Sep a patient whose accession was filled on one row and blank on the next
# became two patients and the day was counted twice -- 139 days across 14 accounts,
# 92 of them marked. 03 now consolidates onto the single accession in the group.
# This gate makes a regression loud.
_dupe = spark.sql(f"""
SELECT account_name,
       regexp_replace(lower(trim(coalesce(patient_name, ''))), '\\s+', ' ') AS name_norm,
       biopsy_date,
       count(DISTINCT patient_id) AS identities,
       sum(CASE WHEN marked THEN 1 ELSE 0 END) AS marked_days
FROM {C}.staged.appropriate_patients
GROUP BY 1, 2, 3
HAVING count(DISTINCT patient_id) > 1
""")
_nd = _dupe.count()
if _nd:
    display(_dupe.orderBy(F.desc("identities")))
    print(f"  *** {_nd} patient-day(s) carry MORE THAN ONE patient_id for the same name "
          f"and date. Each one is counted twice in NMSC and, where marked, in MARKED.")
    print(f"  *** Expected 0 after the 24 Sep MRN consolidation in 03. A non-zero count "
          f"means the consolidation did not run, or two real people share a name and a date "
          f"(03 prints those separately and leaves them unmerged by design).")
else:
    print("  patient identity: one patient_id per name per day — no double counting")
