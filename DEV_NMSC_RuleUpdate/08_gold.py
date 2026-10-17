# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — Notebook 9: the gold layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## The vocabulary notebook

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("window_start", "2026-05-01", "Window start (inclusive)")
dbutils.widgets.text("window_end", "2026-09-01", "Window end (exclusive)")
dbutils.widgets.text("months", "4", "Months in period (Pam's Months_In_Period)")
dbutils.widgets.dropdown("namesake_in_enterprise", "exclude", ["exclude", "include"],
                         "Ambiguous namesakes in the enterprise scope")
# [26 Aug] BOTH OF THESE ARE UNDECIDED AND BOTH ARE WIDGETS ON PURPOSE. See wiki.
dbutils.widgets.text("band_ipm", "1.35", "IPM cutoff (x marked days)")
dbutils.widgets.text("band_lead", "1.00", "Lead cutoff (x marked days)")
# NOTE: there is no band-A switch here. The only statuses excluded are. See wiki.

C  = dbutils.widgets.get("catalog")
W0 = dbutils.widgets.get("window_start")
W1 = dbutils.widgets.get("window_end")
MONTHS = int(dbutils.widgets.get("months"))
NAMESAKE = dbutils.widgets.get("namesake_in_enterprise")
BAND_IPM  = float(dbutils.widgets.get("band_ipm"))
BAND_LEAD = float(dbutils.widgets.get("band_lead"))

DAYS  = f"{C}.staged.patient_days"

# ---- WINDOW SCOPE --------------------------------------------------------------. See wiki.
_DAYS_TABLE = DAYS
# [23 Sep] raw.account_exclusion is now ENFORCED here, not merely recorded.
# Before this it was read only for a severity downgrade in the review view, so
# seeding the table changed no number anywhere.
#
# _days_window is the ONE scope view in this notebook -- everything downstream
# reads DAYS, which points at it -- so filtering here drops an excluded account
# from account_summary, the conversions, the disposition and the export
# together, with no chance of two sheets disagreeing.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _days_window AS
SELECT d.* FROM {_DAYS_TABLE} d
LEFT JOIN {C}.raw.account_map      m ON m.folder_name = d.account_name AND m.role = 'primary'
LEFT JOIN {C}.raw.account_exclusion e ON e.account_id = m.account_id
-- [23 Sep] VOB_ONLY facilities. 03 cannot decide these -- the verification does
-- not exist until 06 -- so it labels the day on facility_kinds and the decision
-- lands here, where staged.vob_match is available.
-- vob_match names the owning folder `matched_folder`, not `account_name`.
LEFT JOIN (SELECT DISTINCT matched_folder, patient_id, biopsy_date
           FROM {C}.staged.vob_match WHERE matched) vm
       ON vm.matched_folder = d.account_name
      AND vm.patient_id     = d.patient_id
      AND vm.biopsy_date    = d.biopsy_date
WHERE d.biopsy_date >= date('{W0}') AND d.biopsy_date < date('{W1}')
  AND (e.account_id IS NULL
       OR (e.effective_from IS NOT NULL AND d.biopsy_date < e.effective_from))
  -- a day is dropped only when EVERY facility on it is VOB_ONLY and it matched
  -- no verification. A day that also touches a FULL facility stays, which is
  -- the same precedence the 03 gate uses.
  AND NOT (array_contains(d.facility_kinds, 'VOB_ONLY')
           AND NOT array_contains(d.facility_kinds, 'FULL')
           AND vm.patient_id IS NULL)
""")
DAYS = "_days_window"

_vo = spark.sql(f"""
SELECT count(*) AS dropped, count(DISTINCT account_name) AS accounts
FROM {_DAYS_TABLE} d
LEFT JOIN (SELECT DISTINCT matched_folder, patient_id, biopsy_date
           FROM {C}.staged.vob_match WHERE matched) vm
       ON vm.matched_folder = d.account_name AND vm.patient_id = d.patient_id
      AND vm.biopsy_date    = d.biopsy_date
WHERE d.biopsy_date >= date('{W0}') AND d.biopsy_date < date('{W1}')
  AND array_contains(d.facility_kinds, 'VOB_ONLY')
  AND NOT array_contains(d.facility_kinds, 'FULL')
  AND vm.patient_id IS NULL
""").first()
print(f"VOB_ONLY facilities: {_vo['dropped']:,} patient-day(s) dropped for having no "
      f"matched verification, across {_vo['accounts']} account(s)")

# Print the cost every run. A filter whose effect is never shown is one nobody
# notices has stopped working.
_excl_cost = spark.sql(f"""
SELECT count(*) AS patient_days, count(DISTINCT d.account_name) AS accounts
FROM {_DAYS_TABLE} d
JOIN {C}.raw.account_map      m ON m.folder_name = d.account_name AND m.role = 'primary'
JOIN {C}.raw.account_exclusion e ON e.account_id = m.account_id
WHERE d.biopsy_date >= date('{W0}') AND d.biopsy_date < date('{W1}')
  AND (e.effective_from IS NULL OR d.biopsy_date >= e.effective_from)
""").first()
print(f"excluded by raw.account_exclusion: {_excl_cost['patient_days']:,} patient-days "
      f"across {_excl_cost['accounts']} account(s)")
_held = spark.table(_DAYS_TABLE).count()
_dw = spark.sql(
    "SELECT count(*) AS n, count(DISTINCT date_format(biopsy_date,'yyyy-MM')) AS m "
    "FROM _days_window").first()
print(f"patient-days: {_dw['n']:,} in window across {_dw['m']} month(s), "
      f"{_held:,} held in total")

SCOPE = f"{C}.staged.patient_vob_scope"
VOBM  = f"{C}.staged.vob_match"
LEAK  = f"{C}.staged.vob_leak_detail"
LES   = f"{C}.staged.lesion_classified"
ADIM  = f"{C}.raw.account_dim"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.gold")
print(f"window {W0} .. {W1} ({MONTHS} months) | namesakes in enterprise scope: {NAMESAKE}")
print(f"band cutoffs: IPM > {BAND_IPM}x marked, Lead >= {BAND_LEAD}x marked")
if (BAND_IPM, BAND_LEAD) == (1.35, 1.00):
    print("  (original ratios, on the biopsy-linked pool -> IPM 15 / Lead 6 /")
    print("   Standard 149. 0.91/0.74 would reproduce the pre-basis-change mix.")
    print("   Band decision deferred by Becky, 26 Aug.)")

# Fail early and by name rather than three cells later with a column error.
for t in [DAYS, SCOPE, VOBM, LEAK, LES, ADIM,
          f"{C}.raw.account_map", f"{C}.raw.account_exclusion", f"{C}.raw.vob"]:
    if not spark.catalog.tableExists(t):
        raise ValueError(
            f"{t} does not exist. Run the notebook that builds it first — "
            "03 for patient_days, 04 for the scope / match / leak tables, "
            "02 for lesion_classified, 06 for account_map and "
            "account_exclusion, 07 for account_dim and vob.")
if "sim_own_site" not in spark.table(SCOPE).columns:
    raise ValueError(
        f"{SCOPE} has no sim_own_site column, so SIM cannot follow the VOB funnel. "
        "That column was added to notebook 04 on 21 Aug — re-run 04 before this.")
print("all inputs present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The month spine

# COMMAND ----------

_m = spark.sql(f"""
SELECT date_format(d, 'yyyy-MM') AS ym, date_format(d, 'MMM') AS label
FROM (SELECT explode(sequence(to_date('{W0}'), add_months(to_date('{W1}'), -1),
                              interval 1 month)) AS d)
ORDER BY ym
""").collect()
YM = [(r["ym"], r["label"]) for r in _m]
print("months:", YM)
if len(YM) != MONTHS:
    raise ValueError(
        f"the window spans {len(YM)} months ({[y for y,_ in YM]}) but months={MONTHS}. "
        "Pam's Avg Sims/mo divides by that widget, so a mismatch silently scales every "
        "yield column. Fix one of the two.")

# ---- MONTH SUFFIXES ------------------------------------------------------------. See wiki.
MSUF = [lab for _, lab in YM]
print("monthly column suffixes:", MSUF)
assert len(set(MSUF)) == len(MSUF), (
    f"two months in this window share the label {MSUF} -- the window must be under "
    f"twelve months for 'MMM' to be unique, or the suffix needs a year")


def monthly(expr, alias_prefix, src_month_col="ym"):
    """Four pivoted monthly columns plus their total, as SQL text.

    Keys on `ym` (yyyy-MM), derived in `_pd` from biopsy_date — NOT on
    `patient_days.month_label`, which is `date_format(biopsy_date, 'MMM')` and
    therefore reads 'Mar'. Comparing that to '2026-03' matches nothing: the monthly
    columns would all be zero while the totals stayed correct, which is exactly the
    shape of error that ships. It is also year-ambiguous, and this window rolls.
    """
    parts = []
    for i, (ym, lab) in enumerate(YM, start=1):
        parts.append(f"sum(CASE WHEN {src_month_col} = '{ym}' THEN {expr} ELSE 0 END)"
                     f" AS {alias_prefix}_{lab}")
    parts.append(f"sum({expr}) AS {alias_prefix}_total")
    return ",\n  ".join(parts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Patient-day side — NMSC, MARKED, and the three VOB scopes

# COMMAND ----------

# The enterprise scope's namesake handling. `vob_enterprise` on the scope table counts
# any folder; excluding namesakes means requiring the VOB to have resolved to a single
# folder. Exposed as a widget because it is a judgement call, and the two answers differ
# by roughly threefold on cross-clinic matching (§6).
_ns = ("" if NAMESAKE == "include" else
       "AND coalesce(s.n_enterprise_folders, 0) <= 1")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _pd AS
SELECT
  d.account_name,
  date_format(d.biopsy_date, 'yyyy-MM')                     AS ym,
  d.appropriate,
  d.marked,
  d.has_actions,
  coalesce(s.vob_own_site,     false)                       AS vob_own,
  coalesce(s.vob_sharing_site, false)                       AS vob_sharing,
  -- THE NAMESAKE GUARD APPLIES TO THE WIDENING ONLY, NEVER TO THE SCOPES BENEATH IT.
  -- `_ns` used to sit on `vob_enterprise` alone, which inverted the funnel: a patient
  -- with a VOB at their OWN site who also namesake-matched two other folders came back
  -- vob_own = true, vob_ent = false. Since vob_total counts vob_ent, that patient
  -- landed in vob_here and not in vob_total. Measured 24 Aug: 61 accounts whose
  -- buckets did not sum, 42 on the sim side, and 177 of 183 where
  -- marked_to_vob_sibling exceeded marked_to_vob_enterprise.
  -- own OR sharing keeps the nesting true BY CONSTRUCTION; the guard only ever
  -- withholds the extra credit a namesake match would otherwise buy.
  (coalesce(s.vob_own_site,     false)
    OR coalesce(s.vob_sharing_site, false)
    OR (coalesce(s.vob_enterprise, false) {_ns}))           AS vob_ent,
  coalesce(s.sim_own_site,     false)                       AS sim_own,
  coalesce(s.sim_sharing_site, false)                       AS sim_sharing,
  (coalesce(s.sim_own_site,     false)
    OR coalesce(s.sim_sharing_site, false)
    OR (coalesce(s.sim_enterprise, false) {_ns}))           AS sim_ent
FROM {DAYS} d
LEFT JOIN {SCOPE} s
       ON s.account_name = d.account_name
      AND s.patient_id   = d.patient_id
      AND s.biopsy_date  = d.biopsy_date
WHERE d.biopsy_date >= '{W0}' AND d.biopsy_date < '{W1}'
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _pd_summary AS
SELECT
  account_name,
  {monthly("CASE WHEN appropriate THEN 1 ELSE 0 END", "nmsc")},
  {monthly("CASE WHEN marked THEN 1 ELSE 0 END", "marked")},

  -- EXCLUSIVE buckets: these sum to vob_total, which is what the sheet displays
  sum(CASE WHEN marked AND vob_own                        THEN 1 ELSE 0 END) AS vob_here,
  sum(CASE WHEN marked AND vob_sharing AND NOT vob_own    THEN 1 ELSE 0 END) AS vob_sibling,
  sum(CASE WHEN marked AND vob_ent AND NOT vob_sharing    THEN 1 ELSE 0 END) AS vob_enterprise,
  sum(CASE WHEN marked AND vob_ent                        THEN 1 ELSE 0 END) AS vob_total,

  -- SIM following the same funnel, same exclusivity
  sum(CASE WHEN marked AND sim_own                        THEN 1 ELSE 0 END) AS sim_here,
  sum(CASE WHEN marked AND sim_sharing AND NOT sim_own    THEN 1 ELSE 0 END) AS sim_sibling,
  sum(CASE WHEN marked AND sim_ent AND NOT sim_sharing    THEN 1 ELSE 0 END) AS sim_enterprise,
  sum(CASE WHEN marked AND sim_ent                        THEN 1 ELSE 0 END) AS sim_total,

  -- the same three scopes on the APPROPRIATE denominator, for the NMSC→VOB column
  sum(CASE WHEN appropriate AND vob_own THEN 1 ELSE 0 END)  AS appr_vob_here,
  sum(CASE WHEN appropriate AND vob_ent THEN 1 ELSE 0 END)  AS appr_vob_total,

  round(avg(CASE WHEN appropriate AND has_actions THEN 1.0
                 WHEN appropriate THEN 0.0 END), 4)          AS actions_coverage
FROM _pd
GROUP BY account_name
""")

print(f"_pd_summary: {spark.table('_pd_summary').count()} accounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. VOB side — the `VOB → Sim` denominator

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3a. Timing — days to VOB, on Becky's anchor chain

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _timing AS
WITH pair AS (
  SELECT
    v.matched_folder                              AS account_name,
    v.gap_days                                    AS d_biopsy,
    -- gap_days is measured from the biopsy, so moving the anchor forward by
    -- (anchor - biopsy) days shortens the interval by the same amount. Doing it
    -- by subtraction keeps all three on ONE definition of the VOB date.
    -- BOUND BOTH ANCHORS. A result date more than 180 days after the biopsy is
    -- not that biopsy's result, and one BEFORE the biopsy is impossible. Two
    -- accounts (Branson, Heights Portland) carried a single malformed result
    -- date each -- a scanned log whose year lost a space -- and because this is
    -- computed by subtraction their averages came out at MINUS THREE MILLION
    -- DAYS, dragging the portfolio result-anchor mean to -41,093. One bad row
    -- per account was enough, because an average has no defence against it.
    CASE WHEN d.result_date IS NOT NULL
          AND datediff(d.result_date, v.biopsy_date) BETWEEN 0 AND 180
         THEN v.gap_days - datediff(d.result_date, v.biopsy_date) END AS d_result,
    CASE WHEN d.phone_date IS NOT NULL
          AND datediff(d.phone_date, v.biopsy_date) BETWEEN 0 AND 180
         THEN v.gap_days - datediff(d.phone_date, v.biopsy_date) END  AS d_phone
  FROM {VOBM} v
  JOIN {DAYS} d
    ON  d.account_name = v.matched_folder
    AND d.patient_id   = v.patient_id
    AND d.biopsy_date  = v.biopsy_date
  WHERE v.matched
    AND v.gap_days IS NOT NULL
    AND v.vob_created >= '{W0}' AND v.vob_created < '{W1}'
),
-- [28 Aug] THE OFFSETS, measured on this run's own rows.
--
-- The chain picks whichever anchor a row HAS, and the three anchors do not sit at the
-- same point in time: the results phone call comes AFTER the pathology result, the
-- biopsy BEFORE it. So an account with no result date reads systematically fast (it
-- is measuring from the phone call) or slow (from the biopsy) -- not because the
-- practice behaves differently but because of what its EMR exports. 40 accounts have
-- no result date at all.
--
-- Each offset is the gap measured ONLY on rows carrying BOTH dates, which is the only
-- place it is observable. MEDIAN, not mean: a handful of malformed dates give the
-- mean a 62-day standard deviation against an inter-quartile range of 3 to 6 days.
--
-- Global, not per-account, and that is forced: an account with no result date has no
-- paired rows, so it cannot supply its own offset. It is exactly those accounts that
-- need one.
offsets AS (
  SELECT percentile_approx(d_result - d_phone,  0.5) AS off_phone,
         percentile_approx(d_biopsy - d_result, 0.5) AS off_biopsy
  FROM pair
),
chained AS (
  SELECT p.account_name, p.d_biopsy, p.d_result, p.d_phone,
         coalesce(p.d_result, p.d_phone, p.d_biopsy)    AS d_chain,
         -- Same priority, put on a RESULT-DATE scale so accounts are comparable.
         -- An ESTIMATE for any row that is not result-anchored; `timing_anchor_mix`
         -- says how much of an account rests on one.
         CASE WHEN p.d_result IS NOT NULL THEN p.d_result
              WHEN p.d_phone  IS NOT NULL THEN p.d_phone  + o.off_phone
              ELSE                              p.d_biopsy - o.off_biopsy
         END                                            AS d_chain_norm,
         CASE WHEN p.d_result IS NOT NULL THEN 'result'
              WHEN p.d_phone  IS NOT NULL THEN 'phone'
              ELSE                              'biopsy' END AS anchor
  FROM pair p CROSS JOIN offsets o
)
SELECT
  account_name,
  round(avg(d_result), 1)                                   AS avg_days_result_to_vob,
  round(avg(d_phone),  1)                                   AS avg_days_phone_to_vob,
  round(avg(d_biopsy), 1)                                   AS avg_days_biopsy_to_vob,
  round(avg(d_chain),  1)                                   AS avg_days_to_vob,
  round(avg(d_chain_norm), 1)                               AS avg_days_to_vob_norm,
  count(*)                                                  AS timing_n,
  round(100.0 * sum(CASE WHEN anchor='result' THEN 1 ELSE 0 END) / count(*), 0) AS pct_anchor_result,
  round(100.0 * sum(CASE WHEN anchor='phone'  THEN 1 ELSE 0 END) / count(*), 0) AS pct_anchor_phone,
  round(100.0 * sum(CASE WHEN anchor='biopsy' THEN 1 ELSE 0 END) / count(*), 0) AS pct_anchor_biopsy,
  concat(
    'result ', cast(round(100.0*sum(CASE WHEN anchor='result' THEN 1 ELSE 0 END)/count(*),0) AS INT), '%',
    ' / phone ', cast(round(100.0*sum(CASE WHEN anchor='phone' THEN 1 ELSE 0 END)/count(*),0) AS INT), '%',
    ' / biopsy ', cast(round(100.0*sum(CASE WHEN anchor='biopsy' THEN 1 ELSE 0 END)/count(*),0) AS INT), '%'
  )                                                         AS timing_anchor_mix
FROM chained
GROUP BY account_name
""")

_t = spark.sql("""
SELECT count(*) AS accounts,
       sum(CASE WHEN avg_days_to_vob IS NULL THEN 1 ELSE 0 END)      AS MUST_BE_ZERO,
       sum(CASE WHEN pct_anchor_biopsy = 100 THEN 1 ELSE 0 END)      AS biopsy_only_accts,
       sum(CASE WHEN pct_anchor_result = 100 THEN 1 ELSE 0 END)      AS result_only_accts,
       round(avg(avg_days_result_to_vob), 1)                         AS port_result,
       round(avg(avg_days_phone_to_vob), 1)                          AS port_phone,
       round(avg(avg_days_biopsy_to_vob), 1)                         AS port_biopsy
FROM _timing
""").first()

# The chain's own guarantee: biopsy is always available, so a matched pair can
# never fail to produce a chained value. A NULL here means the join lost rows.
assert _t["MUST_BE_ZERO"] == 0, (
    f"{_t['MUST_BE_ZERO']} accounts have a NULL chained timing. Biopsy date anchors "
    "every patient-day, so the coalesce cannot fall through -- the join to "
    "patient_days is dropping matched rows.")

print(f"_timing: {_t['accounts']} accounts")
print(f"  portfolio avg days to VOB by anchor -- "
      f"result {_t['port_result']}, phone {_t['port_phone']}, biopsy {_t['port_biopsy']}")
print(f"  {_t['result_only_accts']} accounts are pure result anchor, "
      f"{_t['biopsy_only_accts']} are pure biopsy anchor")

# HARD GATE, but on the ARITHMETIC BOUND -- not on the sign. See wiki.
_b = spark.sql(f"""
SELECT min(gap_days) AS lo, max(gap_days) AS hi
FROM {C}.staged.vob_match WHERE matched AND gap_days IS NOT NULL
""").first()
_LO, _HI = _b["lo"] - 180, _b["hi"]

_bad = spark.sql(f"""
SELECT count(*) AS accounts
FROM _timing
WHERE avg_days_to_vob        NOT BETWEEN {_LO} AND {_HI}
   OR avg_days_biopsy_to_vob NOT BETWEEN {_LO} AND {_HI}
   OR (avg_days_result_to_vob IS NOT NULL
       AND avg_days_result_to_vob NOT BETWEEN {_LO} AND {_HI})
   OR (avg_days_phone_to_vob IS NOT NULL
       AND avg_days_phone_to_vob NOT BETWEEN {_LO} AND {_HI})
""").first()["accounts"]
assert _bad == 0, (
    f"{_bad} accounts have an average days-to-VOB outside [{_LO}, {_HI}], which the "
    f"inputs cannot produce: gap_days runs {_b['lo']}..{_b['hi']} and the anchor "
    "offset is bounded to 0..180. A date is parsed wrong -- check "
    "patient_days.result_date and phone_date for years outside 1990-2100.")

# Negative averages are legitimate but worth seeing, because a whole account
# reading negative usually means very few matches rather than fast verification.
_negs = spark.sql("""
SELECT count(*) AS accounts, sum(timing_n) AS matches
FROM _timing WHERE avg_days_to_vob < 0
""").first()
if _negs["accounts"]:
    print(f"  {_negs['accounts']} accounts average BELOW zero days to VOB across "
          f"{_negs['matches']} matches -- verification opened at the biopsy, with the "
          f"result dated later. Legitimate; read them with timing_n in view.")

assert _t["port_phone"] <= _t["port_result"] <= _t["port_biopsy"], (
    f"the timing anchors do not nest: phone {_t['port_phone']} <= result "
    f"{_t['port_result']} <= biopsy {_t['port_biopsy']} is false. The physical "
    "order is biopsy -> result -> phone -> VOB, so a later anchor must yield a "
    "smaller interval. This inverting means an anchor date is wrong.")
print("  anchors nest correctly (later anchor -> shorter interval)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. Salesforce monthly rollup — sum over every account mapped to the folder

# COMMAND ----------

# The month columns are built from YM, so rolling the window forward stays a widget
# change. The SOURCE is now raw.account_rollup (long) rather than the wide AllAccounts
# columns on account_dim — adding July is a load, not an edit in three places.
_vob_cols = ", ".join(
    f"sum(CASE WHEN r.month = '{ym}' THEN r.vob ELSE 0 END) AS sf_vob_{lab}"
    for ym, lab in YM)
_sim_cols = ", ".join(
    f"sum(CASE WHEN r.month = '{ym}' THEN r.sim ELSE 0 END) AS sf_sim_{lab}"
    for ym, lab in YM)
# AVG days VOB close -> Sim. NOT summed: the value is already an average per
# account-month, and a pooled folder is two AccountIds for ONE site doing ONE body of
# work, so summing two averages is meaningless. A straight avg() across the pooled ids,
# per the 2 Sep decision -- "for those 6 just average the 2 values".
#   Case-weighted alternative, if that decision is ever revisited:
#   round(sum(days * cases) / nullif(sum(cases), 0), 1)  -- v2s_cases is carried for it.
# Absent stays absent: no coalesce, so a month with nothing closed reads NULL, not 0.
_v2s_cols = ", ".join(
    f"round(avg(CASE WHEN r.month = '{ym}' THEN r.avg_days_vob_close_to_sim END), 1) "
    f"AS sf_v2s_days_{lab}"
    for ym, lab in YM)
_v2s_case_cols = ", ".join(
    f"sum(CASE WHEN r.month = '{ym}' THEN r.v2s_cases ELSE 0 END) AS sf_v2s_cases_{lab}"
    for ym, lab in YM)

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.sf_rollup USING DELTA AS
WITH mapped AS (
  -- DISTINCT because a folder can appear twice in the map (once as its own primary,
  -- once carried in the pooled seed). Counting it twice would double the practice
  -- rather than pool it.
  SELECT DISTINCT folder_name, account_id FROM {C}.raw.account_map
),
prim AS (
  SELECT folder_name, min(account_id) AS account_id
  FROM {C}.raw.account_map WHERE role = 'primary' GROUP BY folder_name
)
SELECT
  m.folder_name                     AS account_name,
  p.account_id                      AS account_id,     -- the attribute-bearing account
  count(DISTINCT m.account_id)      AS accounts_pooled,
  {_vob_cols},
  {_sim_cols},
  {_v2s_cols},
  {_v2s_case_cols}
FROM mapped m
LEFT JOIN {C}.raw.account_rollup r ON r.account_id = m.account_id
LEFT JOIN prim p ON p.folder_name = m.folder_name
GROUP BY m.folder_name, p.account_id
""")

_n = spark.table(f"{C}.gold.sf_rollup").count()
_vt = ' + '.join(f'sf_vob_{lab}' for lab in MSUF)
_st = ' + '.join(f'sf_sim_{lab}' for lab in MSUF)
_tot = spark.sql(f"SELECT sum({_vt}) v, sum({_st}) s FROM {C}.gold.sf_rollup").first()
print(f"wrote {C}.gold.sf_rollup: {_n:,} folders · {_tot['v']:,} VOB · {_tot['s']:,} SIM")
_v2sn = spark.sql(
    f"SELECT count(sf_v2s_days_{MSUF[-1]}) c FROM {C}.gold.sf_rollup").first()["c"]
print(f"  AVG days VOB→Sim ({MSUF[-1]}) on {_v2sn:,} of {_n:,} folders — the rest closed "
      f"nothing that month and read NULL, not 0")
display(spark.sql(f"""
SELECT account_name, accounts_pooled, {_vt} AS sf_vob, {_st} AS sf_sim
FROM {C}.gold.sf_rollup WHERE accounts_pooled > 1 ORDER BY account_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gates on the published basis

# COMMAND ----------

# ---- POOLED-FOLDER GATE -----------------------------------------------------
# Downgraded from assert to warning: a failure here no longer stops the run.
# The finding is unchanged, only the consequence. A gate that has been softened
# has to shout and has to be carried forward, or it quietly becomes a gate
# nobody knows has stopped holding.
try:
    DEGRADED
except NameError:
    DEGRADED = []          # (gate, n, detail) for every check downgraded this run

_bad = spark.sql(f"""
WITH pooled AS (
  SELECT folder_name FROM {C}.raw.account_map
  GROUP BY folder_name HAVING count(DISTINCT account_id) > 1
),
prim AS (
  SELECT m.folder_name, sum(r.vob) AS primary_vob, sum(r.sim) AS primary_sim
  FROM {C}.raw.account_map m
  JOIN {C}.raw.account_rollup r ON r.account_id = m.account_id
  WHERE m.role = 'primary' AND r.month IN ({','.join(repr(ym) for ym, _ in YM)})
  GROUP BY m.folder_name
)
SELECT r.account_name, r.accounts_pooled,
       {_vt} AS rollup_vob, p.primary_vob,
       {_st} AS rollup_sim, p.primary_sim
FROM {C}.gold.sf_rollup r
JOIN pooled x ON x.folder_name = r.account_name
JOIN prim   p ON p.folder_name = r.account_name
WHERE {_vt} <= p.primary_vob
""")
_nbad = _bad.count()
_bad_folders = []
if _nbad:
    display(_bad)
    _bad_folders = [r["account_name"] for r in _bad.select("account_name").collect()]
    DEGRADED.append(("sf_rollup pooled-folder gate", _nbad, _bad_folders))
    print("!" * 78)
    print(f"WARNING — POOLED-FOLDER GATE FAILED ON {_nbad} FOLDER(S). RUN CONTINUING.")
    print("A pooled folder whose rollup is no larger than its primary account alone")
    print("means the second machine is not being counted, and that practice's")
    print("published verifications are roughly half of what they should be.")
    for _f in _bad_folders:
        print(f"    - {_f}")
    print("Column Y is UNDERSTATED for these folders. Confirm raw.account_rollup")
    print("carries a row for BOTH account_ids before anything here is published.")
    print("!" * 78)

_empty = spark.sql(f"""
SELECT account_name FROM {C}.gold.sf_rollup WHERE {_vt} = 0 AND {_st} = 0
""")
_ne = _empty.count()
if _ne:
    display(_empty)
    print(f"  *** {_ne} folder(s) publish zero verifications AND zero simulations for "
          f"the whole window. Confirm each is genuinely quiet rather than missing from "
          f"raw.account_rollup — notebook 05 section 5 lists the gaps.")

# [29 Aug] THE "SIM MAY NOT EXCEED VOB" GATE IS GONE FROM THIS BASIS, DELIBERATELY. See wiki.

_np = spark.sql(f"SELECT count(*) c FROM {C}.gold.sf_rollup "
                f"WHERE accounts_pooled > 1").first()["c"]
# The summary line has to reflect what actually happened. "all exceeding their
# primary. OK" printed under a failing gate is how a known defect gets shipped.
if _nbad:
    print(f"{_n} folders · {_np} pooled, {_nbad} NOT exceeding their primary — "
          f"WARNED, not blocked. See the banner above.")
else:
    print(f"{_n} folders · {_np} pooled, all exceeding their primary. OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN dev_pathology_data.staged LIKE 'vob_match';
# MAGIC DESCRIBE HISTORY dev_pathology_data.staged.vob_match;   -- when was it last written?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `gold.account_summary`

# COMMAND ----------

# ---- PREREQUISITES FOR SECTION 4, ALL OF THEM -------------------------------
# Section 4 (gold.account_summary) reads six temp views. They are built across
# four earlier cells; if any one of those has not run in THIS session, section 4
# dies naming whichever view it reaches first, and fixing that one just moves the
# error to the next.
#
#   _days_window -> _pd -> _pd_summary -> _vob_side -> _vob_monthly -> _timing
#
# Every statement below is CREATE OR REPLACE, so running this when the earlier
# cells HAVE run changes nothing. Paste it immediately above section 4.
#
# Needs C, VOBM, W0, W1 and the other config names. A NameError here means the
# config cell has not run, and that is the real problem rather than the views.

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("window_start", "2026-05-01", "Window start (inclusive)")
dbutils.widgets.text("window_end", "2026-09-01", "Window end (exclusive)")
dbutils.widgets.text("months", "4", "Months in period (Pam's Months_In_Period)")
dbutils.widgets.dropdown("namesake_in_enterprise", "exclude", ["exclude", "include"],
                         "Ambiguous namesakes in the enterprise scope")
# [26 Aug] BOTH OF THESE ARE UNDECIDED AND BOTH ARE WIDGETS ON PURPOSE. See wiki.
dbutils.widgets.text("band_ipm", "1.35", "IPM cutoff (x marked days)")
dbutils.widgets.text("band_lead", "1.00", "Lead cutoff (x marked days)")
# NOTE: there is no band-A switch here. The only statuses excluded are. See wiki.

C  = dbutils.widgets.get("catalog")
W0 = dbutils.widgets.get("window_start")
W1 = dbutils.widgets.get("window_end")
MONTHS = int(dbutils.widgets.get("months"))
NAMESAKE = dbutils.widgets.get("namesake_in_enterprise")
BAND_IPM  = float(dbutils.widgets.get("band_ipm"))
BAND_LEAD = float(dbutils.widgets.get("band_lead"))

DAYS  = f"{C}.staged.patient_days"

# ---- WINDOW SCOPE --------------------------------------------------------------. See wiki.
_DAYS_TABLE = DAYS
# [23 Sep] raw.account_exclusion is now ENFORCED here, not merely recorded.
# Before this it was read only for a severity downgrade in the review view, so
# seeding the table changed no number anywhere.
#
# _days_window is the ONE scope view in this notebook -- everything downstream
# reads DAYS, which points at it -- so filtering here drops an excluded account
# from account_summary, the conversions, the disposition and the export
# together, with no chance of two sheets disagreeing.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _days_window AS
SELECT d.* FROM {_DAYS_TABLE} d
LEFT JOIN {C}.raw.account_map      m ON m.folder_name = d.account_name AND m.role = 'primary'
LEFT JOIN {C}.raw.account_exclusion e ON e.account_id = m.account_id
-- [23 Sep] VOB_ONLY facilities. 03 cannot decide these -- the verification does
-- not exist until 06 -- so it labels the day on facility_kinds and the decision
-- lands here, where staged.vob_match is available.
-- vob_match names the owning folder `matched_folder`, not `account_name`.
LEFT JOIN (SELECT DISTINCT matched_folder, patient_id, biopsy_date
           FROM {C}.staged.vob_match WHERE matched) vm
       ON vm.matched_folder = d.account_name
      AND vm.patient_id     = d.patient_id
      AND vm.biopsy_date    = d.biopsy_date
WHERE d.biopsy_date >= date('{W0}') AND d.biopsy_date < date('{W1}')
  AND (e.account_id IS NULL
       OR (e.effective_from IS NOT NULL AND d.biopsy_date < e.effective_from))
  -- a day is dropped only when EVERY facility on it is VOB_ONLY and it matched
  -- no verification. A day that also touches a FULL facility stays, which is
  -- the same precedence the 03 gate uses.
  AND NOT (array_contains(d.facility_kinds, 'VOB_ONLY')
           AND NOT array_contains(d.facility_kinds, 'FULL')
           AND vm.patient_id IS NULL)
""")
DAYS = "_days_window"

_vo = spark.sql(f"""
SELECT count(*) AS dropped, count(DISTINCT account_name) AS accounts
FROM {_DAYS_TABLE} d
LEFT JOIN (SELECT DISTINCT matched_folder, patient_id, biopsy_date
           FROM {C}.staged.vob_match WHERE matched) vm
       ON vm.matched_folder = d.account_name AND vm.patient_id = d.patient_id
      AND vm.biopsy_date    = d.biopsy_date
WHERE d.biopsy_date >= date('{W0}') AND d.biopsy_date < date('{W1}')
  AND array_contains(d.facility_kinds, 'VOB_ONLY')
  AND NOT array_contains(d.facility_kinds, 'FULL')
  AND vm.patient_id IS NULL
""").first()
print(f"VOB_ONLY facilities: {_vo['dropped']:,} patient-day(s) dropped for having no "
      f"matched verification, across {_vo['accounts']} account(s)")

# Print the cost every run. A filter whose effect is never shown is one nobody
# notices has stopped working.
_excl_cost = spark.sql(f"""
SELECT count(*) AS patient_days, count(DISTINCT d.account_name) AS accounts
FROM {_DAYS_TABLE} d
JOIN {C}.raw.account_map      m ON m.folder_name = d.account_name AND m.role = 'primary'
JOIN {C}.raw.account_exclusion e ON e.account_id = m.account_id
WHERE d.biopsy_date >= date('{W0}') AND d.biopsy_date < date('{W1}')
  AND (e.effective_from IS NULL OR d.biopsy_date >= e.effective_from)
""").first()
print(f"excluded by raw.account_exclusion: {_excl_cost['patient_days']:,} patient-days "
      f"across {_excl_cost['accounts']} account(s)")
_held = spark.table(_DAYS_TABLE).count()
_dw = spark.sql(
    "SELECT count(*) AS n, count(DISTINCT date_format(biopsy_date,'yyyy-MM')) AS m "
    "FROM _days_window").first()
print(f"patient-days: {_dw['n']:,} in window across {_dw['m']} month(s), "
      f"{_held:,} held in total")

SCOPE = f"{C}.staged.patient_vob_scope"
VOBM  = f"{C}.staged.vob_match"
LEAK  = f"{C}.staged.vob_leak_detail"
LES   = f"{C}.staged.lesion_classified"
ADIM  = f"{C}.raw.account_dim"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.gold")
print(f"window {W0} .. {W1} ({MONTHS} months) | namesakes in enterprise scope: {NAMESAKE}")
print(f"band cutoffs: IPM > {BAND_IPM}x marked, Lead >= {BAND_LEAD}x marked")
if (BAND_IPM, BAND_LEAD) == (1.35, 1.00):
    print("  (original ratios, on the biopsy-linked pool -> IPM 15 / Lead 6 /")
    print("   Standard 149. 0.91/0.74 would reproduce the pre-basis-change mix.")
    print("   Band decision deferred by Becky, 26 Aug.)")

# Fail early and by name rather than three cells later with a column error.
for t in [DAYS, SCOPE, VOBM, LEAK, LES, ADIM,
          f"{C}.raw.account_map", f"{C}.raw.account_exclusion", f"{C}.raw.vob"]:
    if not spark.catalog.tableExists(t):
        raise ValueError(
            f"{t} does not exist. Run the notebook that builds it first — "
            "03 for patient_days, 04 for the scope / match / leak tables, "
            "02 for lesion_classified, 06 for account_map and "
            "account_exclusion, 07 for account_dim and vob.")
if "sim_own_site" not in spark.table(SCOPE).columns:
    raise ValueError(
        f"{SCOPE} has no sim_own_site column, so SIM cannot follow the VOB funnel. "
        "That column was added to notebook 04 on 21 Aug — re-run 04 before this.")
print("all inputs present")

# ----------------------------------------------------------------------------

# The enterprise scope's namesake handling. `vob_enterprise` on the scope table counts
# any folder; excluding namesakes means requiring the VOB to have resolved to a single
# folder. Exposed as a widget because it is a judgement call, and the two answers differ
# by roughly threefold on cross-clinic matching (§6).
_ns = ("" if NAMESAKE == "include" else
       "AND coalesce(s.n_enterprise_folders, 0) <= 1")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _pd AS
SELECT
  d.account_name,
  date_format(d.biopsy_date, 'yyyy-MM')                     AS ym,
  d.appropriate,
  d.marked,
  d.has_actions,
  coalesce(s.vob_own_site,     false)                       AS vob_own,
  coalesce(s.vob_sharing_site, false)                       AS vob_sharing,
  -- THE NAMESAKE GUARD APPLIES TO THE WIDENING ONLY, NEVER TO THE SCOPES BENEATH IT.
  -- `_ns` used to sit on `vob_enterprise` alone, which inverted the funnel: a patient
  -- with a VOB at their OWN site who also namesake-matched two other folders came back
  -- vob_own = true, vob_ent = false. Since vob_total counts vob_ent, that patient
  -- landed in vob_here and not in vob_total. Measured 24 Aug: 61 accounts whose
  -- buckets did not sum, 42 on the sim side, and 177 of 183 where
  -- marked_to_vob_sibling exceeded marked_to_vob_enterprise.
  -- own OR sharing keeps the nesting true BY CONSTRUCTION; the guard only ever
  -- withholds the extra credit a namesake match would otherwise buy.
  (coalesce(s.vob_own_site,     false)
    OR coalesce(s.vob_sharing_site, false)
    OR (coalesce(s.vob_enterprise, false) {_ns}))           AS vob_ent,
  coalesce(s.sim_own_site,     false)                       AS sim_own,
  coalesce(s.sim_sharing_site, false)                       AS sim_sharing,
  (coalesce(s.sim_own_site,     false)
    OR coalesce(s.sim_sharing_site, false)
    OR (coalesce(s.sim_enterprise, false) {_ns}))           AS sim_ent
FROM {DAYS} d
LEFT JOIN {SCOPE} s
       ON s.account_name = d.account_name
      AND s.patient_id   = d.patient_id
      AND s.biopsy_date  = d.biopsy_date
WHERE d.biopsy_date >= '{W0}' AND d.biopsy_date < '{W1}'
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _pd_summary AS
SELECT
  account_name,
  {monthly("CASE WHEN appropriate THEN 1 ELSE 0 END", "nmsc")},
  {monthly("CASE WHEN marked THEN 1 ELSE 0 END", "marked")},

  -- EXCLUSIVE buckets: these sum to vob_total, which is what the sheet displays
  sum(CASE WHEN marked AND vob_own                        THEN 1 ELSE 0 END) AS vob_here,
  sum(CASE WHEN marked AND vob_sharing AND NOT vob_own    THEN 1 ELSE 0 END) AS vob_sibling,
  sum(CASE WHEN marked AND vob_ent AND NOT vob_sharing    THEN 1 ELSE 0 END) AS vob_enterprise,
  sum(CASE WHEN marked AND vob_ent                        THEN 1 ELSE 0 END) AS vob_total,

  -- SIM following the same funnel, same exclusivity
  sum(CASE WHEN marked AND sim_own                        THEN 1 ELSE 0 END) AS sim_here,
  sum(CASE WHEN marked AND sim_sharing AND NOT sim_own    THEN 1 ELSE 0 END) AS sim_sibling,
  sum(CASE WHEN marked AND sim_ent AND NOT sim_sharing    THEN 1 ELSE 0 END) AS sim_enterprise,
  sum(CASE WHEN marked AND sim_ent                        THEN 1 ELSE 0 END) AS sim_total,

  -- the same three scopes on the APPROPRIATE denominator, for the NMSC→VOB column
  sum(CASE WHEN appropriate AND vob_own THEN 1 ELSE 0 END)  AS appr_vob_here,
  sum(CASE WHEN appropriate AND vob_ent THEN 1 ELSE 0 END)  AS appr_vob_total,

  round(avg(CASE WHEN appropriate AND has_actions THEN 1.0
                 WHEN appropriate THEN 0.0 END), 4)          AS actions_coverage
FROM _pd
GROUP BY account_name
""")

print(f"_pd_summary: {spark.table('_pd_summary').count()} accounts")

# ----------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _vob_side AS
SELECT
  m.vob_folder                                              AS account_name,
  count(DISTINCT m.vob_id)                                  AS vob_total_all_on_account,
  -- THE BANDING BASIS, and deliberately NOT the column above it. This view keeps a
  -- five-month VOB tail (add_months(W1, 5)) because a June biopsy can produce an
  -- August VOB and the funnel would otherwise undercount conversion. The master's
  -- `TOTAL VOB` had no such tail -- it counted the same four months as the biopsy
  -- window, which is the basis 04's reconciliation compares on. Banding on the
  -- tailed column divides nine months of VOBs by four months of marked days:
  -- measured 24 Aug, 80 accounts >=100% and 42 IPM against the master's 49, falling
  -- to 40 and 22 on the window-matched count. Keep both; band on this one.
  count(DISTINCT CASE WHEN m.vob_created < '{W1}' THEN m.vob_id END)
                                                            AS vob_total_on_account_in_window,
  -- The FUNNEL denominator on the workbook's window: VOBs this site owns that matched
  -- a biopsy ANYWHERE (own site or a sibling). This is what X..AA sum to, and what
  -- VOB -> Sim divides by. A VOB with no biopsy behind it cannot have come through
  -- this funnel, so counting it would charge the site for a conversion it never had
  -- the chance to make.
  -- [25 Aug] The cohort denominator NO LONGER LIVES HERE. The monthly columns are
  -- now cohorted on the biopsy month with no vob_created bound, and this view is
  -- bounded at `vob_created >= W0` for the continuity columns above -- so a
  -- February VOB matching a 3 March biopsy (gap -30, inside the window) would be
  -- in the monthly columns and NOT in a denominator computed here. Widening this
  -- view's bound instead would pull those VOBs into
  -- vob_total_on_account_in_window and break its exact tie to Pam's 10,456.
  -- `vob_with_biopsy_in_window` therefore comes from _vob_monthly.vobm_total,
  -- which is the same rows by construction rather than by coincidence.
  -- [25 Aug] FUNNEL columns take the §6 de-duplication; the CONTINUITY columns
  -- above deliberately do not. Pam's TOTAL VOB counts every distinct VOB on the
  -- account, so de-duplicating it would break the tie to the master that the
  -- "ship both" decision exists to preserve. These three are conversion measures,
  -- where one patient verified twice for one biopsy is one verification.
  count(DISTINCT CASE WHEN (m.matched OR m.leak_folders IS NOT NULL)
                       AND NOT m.is_duplicate_patient_vob
                      THEN m.vob_id END)                    AS vob_with_a_biopsy_anywhere,
  count(DISTINCT CASE WHEN m.matched AND NOT m.is_duplicate_patient_vob
                      THEN m.vob_id END)                     AS vob_matched_own_biopsy,
  -- group_has_sim, not simulation_scheduled: the survivor of a duplicate group
  -- carries the OR-ed conversion, so collapsing never deletes a real sim.
  count(DISTINCT CASE WHEN (m.matched OR m.leak_folders IS NOT NULL)
                       AND NOT m.is_duplicate_patient_vob
                       AND m.group_has_sim
                      THEN m.vob_id END)                     AS sim_from_a_matched_vob,
  count(DISTINCT CASE WHEN m.simulation_scheduled IS NOT NULL
                      THEN m.vob_id END)                     AS sim_all_on_account
FROM {VOBM} m
WHERE m.vob_folder IS NOT NULL
  AND m.vob_created >= '{W0}' AND m.vob_created < add_months('{W1}', 5)
GROUP BY m.vob_folder
""")
print(f"_vob_side: {spark.table('_vob_side').count()} accounts")

# MONTHLY VOB AND SIM, ON THE VOB GRAIN. The workbook's X..AA and AC..AF need these. See wiki.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _vob_monthly AS
SELECT account_name,
  {monthly("1", "vobm")},
  {monthly("CASE WHEN has_sim THEN 1 ELSE 0 END", "simm")}
FROM (
  SELECT vob_folder                                    AS account_name,
         -- [25 Aug, Becky] COHORTED ON THE BIOPSY MONTH, not the VOB month.
         --
         -- The column now answers "of THIS month's biopsies, how many converted",
         -- which is a conversion rate. It used to answer "how many VOBs were
         -- opened this month", which is an activity count, and the two were being
         -- divided by each other.
         --
         -- This also DISSOLVES the left-edge artefact. March read structurally low
         -- (1,273 vs 2,067 in June) because a March VOB from a February biopsy
         -- could not be counted; now the March biopsy carries its own VOB whenever
         -- that VOB was opened.
         --
         -- THE MIRROR RISK IS THE RIGHT EDGE, and it is why the vob_created bound
         -- is gone: a June biopsy's VOB may be opened in August. Bounding on
         -- vob_created < W1 would silently truncate the newest cohort and make
         -- June look bad. `_vob_side` already carries a five-month tail for
         -- exactly this reason; the monthly columns now use it too. June is still
         -- the least complete cohort -- say so on the sheet rather than filtering.
         date_format(cohort_biopsy_date, 'yyyy-MM')    AS ym,
         group_has_sim                                 AS has_sim
  FROM {VOBM}
  WHERE vob_folder IS NOT NULL
    -- BIOPSY-LINKED ONLY. Without this the monthly columns are the site's whole VOB
    -- book, and the sheet's VOB -> Sim (=AG/AB) becomes total-sims over total-VOBs --
    -- the overall metric, not the funnel. Requested explicitly: the VOB -> Sim
    -- denominator is VOBs for that site with a biopsy anywhere, and SIM follows the
    -- same funnel. `vob_total_all_on_account` and `vob_total_on_account_in_window`
    -- keep the unrestricted counts for the band and for continuity with the master.
    AND (matched OR leak_folders IS NOT NULL)
    -- §6: one verification per (folder, patient, biopsy date). `group_has_sim`
    -- above carries the OR-ed conversion, so collapsing loses no sim.
    AND NOT is_duplicate_patient_vob
    AND cohort_biopsy_date IS NOT NULL
)
GROUP BY account_name
""")

# The monthly columns must reconcile to the banding basis, or the sheet's =SUM(X:AA). See wiki.
_vm = spark.sql(f"""
SELECT count(*) AS accounts,
       sum(CASE WHEN m.vobm_total <> ({' + '.join(f'm.vobm_{lab}' for lab in MSUF)})
                THEN 1 ELSE 0 END) AS disagree,
       sum(CASE WHEN m.simm_total <> ({' + '.join(f'm.simm_{lab}' for lab in MSUF)})
                THEN 1 ELSE 0 END) AS sim_disagree
FROM _vob_monthly m
""").first()
assert _vm["sim_disagree"] == 0, (
    f"{_vm['sim_disagree']} accounts where the monthly SIM columns do not sum to "
    "simm_total -- same cause as below, on the SIM side.")
assert _vm["disagree"] == 0, (
    f"{_vm['disagree']} accounts where the monthly VOB columns do not sum to "
    "vobm_total. Every counted VOB is cohorted on cohort_biopsy_date, and every "
    "biopsy date comes from patient_days which is filtered to W0..W1 -- so a "
    "cohort date outside the month spine means the spine and the patient-day "
    "window disagree. The sheet's TOTAL VOB is =SUM(X:AA), so this must hold.")
print(f"_vob_monthly: {_vm['accounts']} accounts, monthly VOB sums to the banding basis")

# THE LAST COHORT IS INCOMPLETE, AND THE SHEET HAS TO SAY SO. See wiki.
_obs = spark.sql(f"""
SELECT date_format(cohort_biopsy_date, 'yyyy-MM')                        AS month,
       max(datediff((SELECT max(to_date(vob_created)) FROM {VOBM}),
                    cohort_biopsy_date))                                  AS days_observed
FROM {VOBM}
WHERE cohort_biopsy_date IS NOT NULL
  AND cohort_biopsy_date >= date('{W0}') AND cohort_biopsy_date < date('{W1}')
GROUP BY 1 ORDER BY 1
""").collect()
print("  days of VOB opportunity observed per cohort:")
for r in _obs:
    print(f"    {r['month']}  {r['days_observed']:>4} days"
          + ("   <-- INCOMPLETE, footnote this column" if r["days_observed"] < 180 else ""))
print("  FOOTNOTE FOR THE SHEET: 'A biopsy has 180 days to produce a")
print("  verification. The later months have not had that long yet, so their")
print("  columns are still filling. They are not a decline.'")

# Soft check, deliberately not an assert. `vobm_total` has NO vob_created bound. See wiki.
_edge = spark.sql("""
SELECT sum(CASE WHEN m.vobm_total > v.vob_with_a_biopsy_anywhere THEN 1 ELSE 0 END) AS accts,
       sum(greatest(m.vobm_total - v.vob_with_a_biopsy_anywhere, 0))                AS vobs
FROM _vob_monthly m JOIN _vob_side v USING (account_name)
""").first()
if _edge["vobs"]:
    print(f"  note: {_edge['vobs']} VOBs across {_edge['accts']} accounts are cohorted "
          f"into the window but created before it (pre-W0, gap >= -30). Expected and "
          f"correct -- the biopsy is in the window even though the VOB predates it.")

# ----------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _timing AS
WITH pair AS (
  SELECT
    v.matched_folder                              AS account_name,
    v.gap_days                                    AS d_biopsy,
    -- gap_days is measured from the biopsy, so moving the anchor forward by
    -- (anchor - biopsy) days shortens the interval by the same amount. Doing it
    -- by subtraction keeps all three on ONE definition of the VOB date.
    -- BOUND BOTH ANCHORS. A result date more than 180 days after the biopsy is
    -- not that biopsy's result, and one BEFORE the biopsy is impossible. Two
    -- accounts (Branson, Heights Portland) carried a single malformed result
    -- date each -- a scanned log whose year lost a space -- and because this is
    -- computed by subtraction their averages came out at MINUS THREE MILLION
    -- DAYS, dragging the portfolio result-anchor mean to -41,093. One bad row
    -- per account was enough, because an average has no defence against it.
    CASE WHEN d.result_date IS NOT NULL
          AND datediff(d.result_date, v.biopsy_date) BETWEEN 0 AND 180
         THEN v.gap_days - datediff(d.result_date, v.biopsy_date) END AS d_result,
    CASE WHEN d.phone_date IS NOT NULL
          AND datediff(d.phone_date, v.biopsy_date) BETWEEN 0 AND 180
         THEN v.gap_days - datediff(d.phone_date, v.biopsy_date) END  AS d_phone
  FROM {VOBM} v
  JOIN {DAYS} d
    ON  d.account_name = v.matched_folder
    AND d.patient_id   = v.patient_id
    AND d.biopsy_date  = v.biopsy_date
  WHERE v.matched
    AND v.gap_days IS NOT NULL
    AND v.vob_created >= '{W0}' AND v.vob_created < '{W1}'
),
-- [28 Aug] THE OFFSETS, measured on this run's own rows.
--
-- The chain picks whichever anchor a row HAS, and the three anchors do not sit at the
-- same point in time: the results phone call comes AFTER the pathology result, the
-- biopsy BEFORE it. So an account with no result date reads systematically fast (it
-- is measuring from the phone call) or slow (from the biopsy) -- not because the
-- practice behaves differently but because of what its EMR exports. 40 accounts have
-- no result date at all.
--
-- Each offset is the gap measured ONLY on rows carrying BOTH dates, which is the only
-- place it is observable. MEDIAN, not mean: a handful of malformed dates give the
-- mean a 62-day standard deviation against an inter-quartile range of 3 to 6 days.
--
-- Global, not per-account, and that is forced: an account with no result date has no
-- paired rows, so it cannot supply its own offset. It is exactly those accounts that
-- need one.
offsets AS (
  SELECT percentile_approx(d_result - d_phone,  0.5) AS off_phone,
         percentile_approx(d_biopsy - d_result, 0.5) AS off_biopsy
  FROM pair
),
chained AS (
  SELECT p.account_name, p.d_biopsy, p.d_result, p.d_phone,
         coalesce(p.d_result, p.d_phone, p.d_biopsy)    AS d_chain,
         -- Same priority, put on a RESULT-DATE scale so accounts are comparable.
         -- An ESTIMATE for any row that is not result-anchored; `timing_anchor_mix`
         -- says how much of an account rests on one.
         CASE WHEN p.d_result IS NOT NULL THEN p.d_result
              WHEN p.d_phone  IS NOT NULL THEN p.d_phone  + o.off_phone
              ELSE                              p.d_biopsy - o.off_biopsy
         END                                            AS d_chain_norm,
         CASE WHEN p.d_result IS NOT NULL THEN 'result'
              WHEN p.d_phone  IS NOT NULL THEN 'phone'
              ELSE                              'biopsy' END AS anchor
  FROM pair p CROSS JOIN offsets o
)
SELECT
  account_name,
  round(avg(d_result), 1)                                   AS avg_days_result_to_vob,
  round(avg(d_phone),  1)                                   AS avg_days_phone_to_vob,
  round(avg(d_biopsy), 1)                                   AS avg_days_biopsy_to_vob,
  round(avg(d_chain),  1)                                   AS avg_days_to_vob,
  round(avg(d_chain_norm), 1)                               AS avg_days_to_vob_norm,
  count(*)                                                  AS timing_n,
  round(100.0 * sum(CASE WHEN anchor='result' THEN 1 ELSE 0 END) / count(*), 0) AS pct_anchor_result,
  round(100.0 * sum(CASE WHEN anchor='phone'  THEN 1 ELSE 0 END) / count(*), 0) AS pct_anchor_phone,
  round(100.0 * sum(CASE WHEN anchor='biopsy' THEN 1 ELSE 0 END) / count(*), 0) AS pct_anchor_biopsy,
  concat(
    'result ', cast(round(100.0*sum(CASE WHEN anchor='result' THEN 1 ELSE 0 END)/count(*),0) AS INT), '%',
    ' / phone ', cast(round(100.0*sum(CASE WHEN anchor='phone' THEN 1 ELSE 0 END)/count(*),0) AS INT), '%',
    ' / biopsy ', cast(round(100.0*sum(CASE WHEN anchor='biopsy' THEN 1 ELSE 0 END)/count(*),0) AS INT), '%'
  )                                                         AS timing_anchor_mix
FROM chained
GROUP BY account_name
""")

_t = spark.sql("""
SELECT count(*) AS accounts,
       sum(CASE WHEN avg_days_to_vob IS NULL THEN 1 ELSE 0 END)      AS MUST_BE_ZERO,
       sum(CASE WHEN pct_anchor_biopsy = 100 THEN 1 ELSE 0 END)      AS biopsy_only_accts,
       sum(CASE WHEN pct_anchor_result = 100 THEN 1 ELSE 0 END)      AS result_only_accts,
       round(avg(avg_days_result_to_vob), 1)                         AS port_result,
       round(avg(avg_days_phone_to_vob), 1)                          AS port_phone,
       round(avg(avg_days_biopsy_to_vob), 1)                         AS port_biopsy
FROM _timing
""").first()

# The chain's own guarantee: biopsy is always available, so a matched pair can
# never fail to produce a chained value. A NULL here means the join lost rows.
assert _t["MUST_BE_ZERO"] == 0, (
    f"{_t['MUST_BE_ZERO']} accounts have a NULL chained timing. Biopsy date anchors "
    "every patient-day, so the coalesce cannot fall through -- the join to "
    "patient_days is dropping matched rows.")

print(f"_timing: {_t['accounts']} accounts")
print(f"  portfolio avg days to VOB by anchor -- "
      f"result {_t['port_result']}, phone {_t['port_phone']}, biopsy {_t['port_biopsy']}")
print(f"  {_t['result_only_accts']} accounts are pure result anchor, "
      f"{_t['biopsy_only_accts']} are pure biopsy anchor")

# HARD GATE, but on the ARITHMETIC BOUND -- not on the sign. See wiki.
_b = spark.sql(f"""
SELECT min(gap_days) AS lo, max(gap_days) AS hi
FROM {C}.staged.vob_match WHERE matched AND gap_days IS NOT NULL
""").first()
_LO, _HI = _b["lo"] - 180, _b["hi"]

_bad = spark.sql(f"""
SELECT count(*) AS accounts
FROM _timing
WHERE avg_days_to_vob        NOT BETWEEN {_LO} AND {_HI}
   OR avg_days_biopsy_to_vob NOT BETWEEN {_LO} AND {_HI}
   OR (avg_days_result_to_vob IS NOT NULL
       AND avg_days_result_to_vob NOT BETWEEN {_LO} AND {_HI})
   OR (avg_days_phone_to_vob IS NOT NULL
       AND avg_days_phone_to_vob NOT BETWEEN {_LO} AND {_HI})
""").first()["accounts"]
assert _bad == 0, (
    f"{_bad} accounts have an average days-to-VOB outside [{_LO}, {_HI}], which the "
    f"inputs cannot produce: gap_days runs {_b['lo']}..{_b['hi']} and the anchor "
    "offset is bounded to 0..180. A date is parsed wrong -- check "
    "patient_days.result_date and phone_date for years outside 1990-2100.")

# Negative averages are legitimate but worth seeing, because a whole account
# reading negative usually means very few matches rather than fast verification.
_negs = spark.sql("""
SELECT count(*) AS accounts, sum(timing_n) AS matches
FROM _timing WHERE avg_days_to_vob < 0
""").first()
if _negs["accounts"]:
    print(f"  {_negs['accounts']} accounts average BELOW zero days to VOB across "
          f"{_negs['matches']} matches -- verification opened at the biopsy, with the "
          f"result dated later. Legitimate; read them with timing_n in view.")

assert _t["port_phone"] <= _t["port_result"] <= _t["port_biopsy"], (
    f"the timing anchors do not nest: phone {_t['port_phone']} <= result "
    f"{_t['port_result']} <= biopsy {_t['port_biopsy']} is false. The physical "
    "order is biopsy -> result -> phone -> VOB, so a later anchor must yield a "
    "smaller interval. This inverting means an anchor date is wrong.")
print("  anchors nest correctly (later anchor -> shorter interval)")

# ---- prove the whole chain is up before section 4 runs ----------------------
_need = ["_days_window", "_pd", "_pd_summary", "_vob_side", "_vob_monthly", "_timing"]
_gone = [v for v in _need if not spark.catalog.tableExists(v)]
assert not _gone, f"still missing after rebuild: {_gone}"
print("all six prerequisite views present — section 4 can run:")
for v in _need:
    print(f"    {v:<15} {spark.table(v).count():>7,} rows")

# COMMAND ----------

# THE PARENTHESES AROUND {n} ARE LOAD-BEARING. Without them a compound numerator is. See wiki.
def _DT(c):
    """Bronze's date forms, parsed, with implausible years rejected.

    [31 Aug] Now `date_parse_bounded_sql` from 00_vocab rather than a
    fourth hand-written copy of the chain. The copies had already drifted: this one
    knew five formats, notebook 03 knew five in two places and two in a third, and
    none of them knew `MMMM d, yyyy` -- which is how three practices lost their July.
    """
    return date_parse_bounded_sql(c)


_pct = lambda n, d: f"round(({n}) / nullif({d}, 0), 4)"

# The monthly Salesforce columns, named the way the workbook's headers expect. Built. See wiki.
def _mcols(alias, prefix, zero=False):
    """The window's monthly columns for one band, plus its total.

    `zero=True` coalesces to 0. The vobm/simm band comes off a LEFT JOIN to
    `_vob_monthly`, so an account with no matched verification got NULL across the
    whole band and shipped BLANK cells to Pam rather than zeros -- Alaska did exactly
    that on the 31 Aug run. The sf_* band has always been coalesced; this band was
    the one that was not.
    """
    if zero:
        cols = ", ".join(
            f"coalesce({alias}.{prefix}_{lab}, 0) AS {prefix}_{lab}" for lab in MSUF)
        return f"{cols}, coalesce({alias}.{prefix}_total, 0) AS {prefix}_total"
    cols = ", ".join(f"{alias}.{prefix}_{lab}" for lab in MSUF)
    return f"{cols}, {alias}.{prefix}_total"

_sf_sel = ",\n  ".join(
    f"coalesce(sr.sf_vob_{lab}, 0) AS sf_vob_{lab}" for lab in MSUF) + ",\n  " + \
          ",\n  ".join(
    f"coalesce(sr.sf_sim_{lab}, 0) AS sf_sim_{lab}" for lab in MSUF)
# Pam's column AY is a single number, and it is the LAST window month -- July for the
# Apr-Jul window. Named without a month suffix because `09_export`, `feed_to_export.py`
# and `build_pam_v2.py` all read it by this name.
_v2s_sel = (f"sr.sf_v2s_days_{MSUF[-1]}  AS avg_days_vob_close_to_sim,\n  "
            f"sr.sf_v2s_cases_{MSUF[-1]} AS avg_days_v2s_cases,\n  "
            f"'{YM[-1][0]}'              AS avg_days_v2s_month")
_sf_vt = " + ".join(f"coalesce(sr.sf_vob_{lab}, 0)" for lab in MSUF)
_sf_st = " + ".join(f"coalesce(sr.sf_sim_{lab}, 0)" for lab in MSUF)

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.account_summary USING DELTA AS
SELECT
  p.account_name,
  a.account_id, a.go_live_date, a.Account_Transition,
  a.regional_manager_name  AS regional,
  a.senior_regional_name   AS senior,
  a.status,

  {_mcols('p', 'nmsc')},
  {_mcols('p', 'marked')},

  -- exclusive display buckets
  p.vob_here, p.vob_sibling, p.vob_enterprise, p.vob_total,
  p.sim_here, p.sim_sibling, p.sim_enterprise, p.sim_total,

  -- the three cumulative conversions the business asked for
  {_pct("p.marked_total", "p.nmsc_total")}    AS appropriate_to_marked,
  {_pct("p.vob_here", "p.marked_total")}      AS marked_to_vob_self,
  {_pct("p.vob_here + p.vob_sibling", "p.marked_total")}
                                              AS marked_to_vob_sibling,
  {_pct("p.vob_total", "p.marked_total")}     AS marked_to_vob_enterprise,

  -- NMSC → VOB, own and enterprise
  {_pct("p.appr_vob_here", "p.nmsc_total")}   AS nmsc_to_vob_self,
  {_pct("p.appr_vob_total", "p.nmsc_total")}  AS nmsc_to_vob_enterprise,

  -- VOB → Sim, on the VOB grain. Denominator = VOBs this site owns with a biopsy
  -- anywhere; see section 3 for why, and why it is not vob_total_all_on_account.
  {_mcols('mv', 'vobm', zero=True)},
  {_mcols('mv', 'simm', zero=True)},
  -- [26 Aug, Becky] THE SALESFORCE-TOTAL COLUMNS ARE NO LONGER PUBLISHED.
  -- `vob_total_all_on_account`, `vob_total_on_account_in_window` and
  -- `sim_all_on_account` counted every verification on the AccountId whether or
  -- not a biopsy in our path logs produced it. Only the pathology-linked funnel
  -- ships now. They are still computed inside _vob_side because the QA gate below
  -- uses one as an upper bound, and they remain derivable from staged.vob_match
  -- at any time -- nothing is lost, it is just not on the sheet.
  coalesce(mv.vobm_total, 0)                     AS vob_with_biopsy_in_window,
  v.vob_with_a_biopsy_anywhere,
  -- VOB pattern band. MIXED GRAIN ON PURPOSE, and the only place in this notebook
  -- that is true: VOBs (Salesforce) over marked patient-days (path log). It can and
  -- should exceed 100% -- that is the signal, meaning a site verifies more people
  -- than its own log marked, i.e. outside referral. It is a CLASSIFICATION input,
  -- never a conversion rate; the four marked_to_vob_* columns above are the rates
  -- and none of them can pass 100% by construction.
  --
  -- The <10 floor is not in the master. A band computed on a denominator of 1 --
  -- Siouxland is literally 1 marked day with 1 VOB -- reads as a 100% referral
  -- practice on the strength of one patient. Say "insufficient data" instead of
  -- printing a number nobody should act on.
  CASE
    WHEN p.marked_total < 10                                     THEN 'Insufficient data'
    -- [26 Aug] REBASED onto the biopsy-linked pool, because that is now the only
    -- VOB count on the sheet. It also fixes a real inconsistency: the band was
    -- computed on the Salesforce pool while the Marked -> VOB column the band is
    -- meant to describe displayed the biopsy-linked one -- two different answers
    -- to one question, and the classification used the one nobody could see.
    --
    -- THE THRESHOLDS HAVE NOT BEEN RECALIBRATED. 1.35 and 1.00 were set against a
    -- pool 17% larger, so accounts shift down: measured on the 26 Aug run the mix
    -- moves from Standard 129 / IPM 23 / Lead 18 to Standard 149 / IPM 15 /
    -- Lead 6 -- 26 accounts change band. That is arithmetic, not a change in
    -- behaviour at those practices. Decide the thresholds against the new basis
    -- before the band drives anything.
    WHEN coalesce(mv.vobm_total, 0) >  {BAND_IPM} * p.marked_total THEN 'IPM'
    WHEN coalesce(mv.vobm_total, 0) >= {BAND_LEAD} * p.marked_total THEN 'Lead / outside referral'
    ELSE                                                              'Standard'
  END                                         AS vob_pattern_band,
  {_pct("coalesce(mv.vobm_total, 0)", "p.marked_total")}
                                              AS vob_pool_to_marked,
  v.vob_matched_own_biopsy,
  v.sim_from_a_matched_vob,

  -- [25 Aug] ON THE COHORT BASIS -- simm_total / vobm_total -- which is exactly
  -- the sheet's =AG3/AB3. Three different VOB -> Sim figures used to ship: this
  -- one over the nine-month tail, `vob_to_sim_reported_basis` over the whole VOB
  -- book, and the workbook's own =AG/AB over four months. None equalled another.
  -- Now there is ONE, and the SQL and the spreadsheet compute it from the same
  -- rows: simulations from biopsy-linked verifications, over those verifications.
  {_pct("coalesce(mv.simm_total, 0)", "coalesce(mv.vobm_total, 0)")}
                                              AS vob_to_sim,

  -- [25 Aug] WAS `v.sim_from_a_matched_vob / MONTHS`, which divided a NINE-month
  -- numerator by four. `_vob_side` runs to add_months(W1, 5) so the funnel can
  -- see an August VOB from a June biopsy; `sim_from_a_matched_vob` therefore
  -- carries the whole tail, while MONTHS is the biopsy window.
  --
  -- Measured on the Mar-Jun export: 5,118 tailed sims vs 4,235 in-window, so
  -- every account read 1.21x high on average and 179 of 183 were affected --
  -- Kerrville at 2.56x. This is v7's AR, the baseline every +Sims/mo yield is
  -- measured against, so the inflation propagated into every opportunity figure.
  --
  -- v7 section 3.2 defines AR as AG/Months_In_Period where AG = SUM(AC:AF), the
  -- four in-window monthly SIM columns. `mv.simm_total` IS that sum, so this now
  -- matches the spec and the workbook agree by construction rather than by luck.
  round(coalesce(mv.simm_total, 0) / {MONTHS}.0, 2) AS current_sims_per_month,

  -- TIMING. Two headline columns, and the difference between them matters.
  --
  -- `avg_days_to_vob`      raw chain: result, else phone, else biopsy, unadjusted.
  -- `avg_days_to_vob_norm` the same chain put on a RESULT-DATE scale, by adding the
  --                        measured offset back when a row falls through. THIS is the
  --                        comparable one, and it is an ESTIMATE wherever an account
  --                        is not fully result-anchored.
  --
  -- The three components stay so a like-for-like comparison is still possible, and
  -- `timing_anchor_mix` is NOT decoration -- it says how much of an account's figure
  -- rests on an estimate.
  t.avg_days_to_vob, t.avg_days_to_vob_norm,
  t.timing_anchor_mix,
  t.avg_days_result_to_vob,
  t.avg_days_phone_to_vob,
  t.avg_days_biopsy_to_vob,
  t.timing_n,
  t.pct_anchor_result, t.pct_anchor_phone, t.pct_anchor_biopsy,

  p.actions_coverage,
  a.practice_group,

  -- THE PUBLISHED VOB AND SIM COLUMNS.
  --
  -- Salesforce basis: every verification the account raised, whether or not we hold
  -- the biopsy behind it. Chosen over the pathology bases because it reconciles to a
  -- Salesforce report, which is what makes it safe to track month over month — the
  -- pathology figure for a month keeps rising for ~180 days as late verifications
  -- match back, so an improvement and a maturing cohort look identical on it.
  --
  -- NMSC appropriate and SRT-marked above are unchanged, from the path log. A biopsy
  -- belongs to the folder whose log recorded it, so those two have no scope choice.
  --
  -- The consequence to keep in view: `marked_to_vob_sf` divides a Salesforce
  -- numerator by a path-log denominator, so it is not a closed conversion. It is the
  -- right column for tracking movement and the wrong one for asking what share of
  -- marked patients converted. `marked_to_vob_self` beside it is the closed version.
  {_sf_sel},
  {_sf_vt}                                                    AS sf_vob_total,
  {_sf_st}                                                    AS sf_sim_total,
  round({_sf_vt} / nullif(p.marked_total, 0), 4)              AS marked_to_vob_sf,
  round({_sf_st} / nullif({_sf_vt}, 0), 4)                    AS sf_vob_to_sim,
  round({_sf_vt} / nullif(p.nmsc_total, 0), 4)                AS nmsc_to_vob_sf,
  {_v2s_sel}
FROM _pd_summary p
LEFT JOIN _vob_side v ON v.account_name = p.account_name
LEFT JOIN _vob_monthly mv ON mv.account_name = p.account_name
LEFT JOIN _timing t ON t.account_name = p.account_name
LEFT JOIN {C}.gold.sf_rollup sr ON sr.account_name = p.account_name
LEFT JOIN (SELECT m.folder_name, d.* FROM {C}.raw.account_map m
           JOIN {ADIM} d ON d.account_id = m.account_id
           WHERE m.role = 'primary') a ON a.folder_name = p.account_name
""")

_n = spark.table(f"{C}.gold.account_summary").count()
print(f"wrote {C}.gold.account_summary: {_n:,} accounts")
display(spark.sql(f"""
SELECT account_name, nmsc_total, marked_total,
       vob_here, vob_sibling, vob_enterprise, vob_total,
       appropriate_to_marked, marked_to_vob_self, marked_to_vob_sibling,
       marked_to_vob_enterprise, vob_with_a_biopsy_anywhere, sim_from_a_matched_vob,
       vob_to_sim
FROM {C}.gold.account_summary ORDER BY nmsc_total DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The arithmetic gates

# COMMAND ----------

_g = spark.sql(f"""
SELECT
  -- exclusive buckets must sum to the total
  sum(CASE WHEN a.vob_here + a.vob_sibling + a.vob_enterprise <> a.vob_total
           THEN 1 ELSE 0 END)                                        AS vob_buckets_dont_sum,
  sum(CASE WHEN a.sim_here + a.sim_sibling + a.sim_enterprise <> a.sim_total
           THEN 1 ELSE 0 END)                                        AS sim_buckets_dont_sum,
  -- the cumulative conversions must be monotone
  sum(CASE WHEN a.marked_to_vob_self > a.marked_to_vob_sibling
           THEN 1 ELSE 0 END)                                        AS self_above_sibling,
  sum(CASE WHEN a.marked_to_vob_sibling > a.marked_to_vob_enterprise
           THEN 1 ELSE 0 END)                                        AS sibling_above_ent,
  -- §7.2: a mark makes a day appropriate, so marked can never exceed appropriate
  sum(CASE WHEN a.marked_total > a.nmsc_total THEN 1 ELSE 0 END)          AS marked_over_nmsc,
  -- a VOB scope cannot exceed the marked days it is counted against
  sum(CASE WHEN a.vob_total > a.marked_total THEN 1 ELSE 0 END)           AS vob_over_marked,
  -- sim follows vob at every scope
  sum(CASE WHEN a.sim_total > a.vob_total THEN 1 ELSE 0 END)              AS sim_over_vob,
  -- the funnel denominator cannot exceed the whole pool. `vob_total_all_on_account`
  -- is no longer published, so this reads it from _vob_side -- the check is still
  -- worth having: more biopsy-linked verifications than verifications would mean
  -- the matcher invented one.
  sum(CASE WHEN a.vob_with_a_biopsy_anywhere > v.vob_total_all_on_account
           THEN 1 ELSE 0 END)                                        AS matched_over_pool
FROM {C}.gold.account_summary a
LEFT JOIN _vob_side v ON v.account_name = a.account_name
""").first()

_GATE_WHERE = {
    "vob_buckets_dont_sum":  "vob_here + vob_sibling + vob_enterprise <> vob_total",
    "sim_buckets_dont_sum":  "sim_here + sim_sibling + sim_enterprise <> sim_total",
    "self_above_sibling":    "marked_to_vob_self > marked_to_vob_sibling",
    "sibling_above_ent":     "marked_to_vob_sibling > marked_to_vob_enterprise",
    "marked_over_nmsc":      "marked_total > nmsc_total",
    "vob_over_marked":       "vob_total > marked_total",
    "sim_over_vob":          "sim_total > vob_total",
    # `vob_total_all_on_account` is no longer a published column, so the drill-down
    # for this gate recomputes it from staged.vob_match. It has to be VALID SQL:
    # this dict is interpolated straight into a SELECT when a gate fires, and a
    # prose placeholder here would turn a real failure into a syntax error.
    "matched_over_pool":     (
        "vob_with_a_biopsy_anywhere > ("
        f"SELECT count(DISTINCT m.vob_id) FROM {C}.staged.vob_match m "
        "WHERE m.vob_folder = account_name)"),
}
_fail = {k: _g[k] for k in _g.asDict() if (_g[k] or 0) > 0}
if _fail:
    for k, v in _fail.items():
        print(f"  {k}: {v} account(s)")
        display(spark.sql(f"SELECT * FROM {C}.gold.account_summary "
                          f"WHERE {_GATE_WHERE[k]}"))
    raise AssertionError(
        f"{sum(_fail.values())} arithmetic violation(s) across {len(_fail)} gate(s): "
        f"{_fail}. These are properties of how the columns are built, so a failure is a "
        "bug in notebook 09, not a finding about a practice. Do not ship the workbook.")
print("all arithmetic gates pass")

# marked_to_vob_* MAY exceed 100% and that is not a defect: v7 §9.2's basis change puts
# unmatched VOBs in the reported numerator, and 49 of 181 accounts exceeded 100% in the
# 8.5.26 master by construction. Printed, never raised, so nobody reads it as broken.
display(spark.sql(f"""
SELECT account_name, marked_total, vob_total, marked_to_vob_enterprise,
       CASE WHEN marked_to_vob_enterprise > 1.35 THEN 'IPM'
            WHEN marked_to_vob_enterprise >= 1.00 THEN 'Lead / outside referral'
            ELSE 'normal range' END AS band
FROM {C}.gold.account_summary
WHERE marked_to_vob_enterprise >= 1.00
ORDER BY marked_to_vob_enterprise DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `gold.raw_unfiltered` — the audit trail

# COMMAND ----------

# [31 Aug] THE LESION DEDUP, WHICH THIS TABLE HAD NEVER APPLIED. See wiki.
_LES_PID = ("coalesce(nullif(regexp_replace(trim(accession), '\\\\.0$', ''), ''), "
            "regexp_replace(lower(trim(patient_name)), '\\\\s+', ' '))")
_LES_DT  = _DT("collected_date")

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.raw_unfiltered USING DELTA AS
WITH pat AS (
  SELECT DISTINCT lower(trim(patient_name_raw)) AS nm FROM {C}.raw.vob
  WHERE trim(coalesce(patient_name_raw, '')) <> ''
),
scoped AS (
  SELECT account_name, patient_id, biopsy_date,
         vob_own_site, vob_sharing_site, vob_enterprise
  FROM {SCOPE}
),
les AS (
  SELECT * FROM (
    SELECT *,
           {lesion_dedup_rank_sql(_LES_PID, _LES_DT)} AS _rn,
           {lesion_copies_sql(_LES_PID, _LES_DT)}     AS _copies
    FROM {LES}
  ) WHERE _rn = 1
)
SELECT
  l.account_name                             AS account,
  l.file_name                                AS source_file,
  l.page_no                                  AS page,
  date_format({_DT("l.collected_date")}, 'MMMM')      AS month,
  l.collected_date                           AS date,
  -- Parsed alongside the verbatim `date`, because bronze holds more than one format:
  -- the PDF parsers run collected_date through norm_date and emit yyyy-MM-dd, but §7f
  -- shipped without that call, so the 8 EzDerm CSV practices carry '3/10/2026' and
  -- '1-Apr-26' as typed. A bare try_to_date() parses only the ISO form and returns
  -- NULL for the rest -- which blanked `month`, made `date` unsortable, and (worse)
  -- broke the scope join below so every sheet lesion read 'path only'.
  {_DT("l.collected_date")}                  AS biopsy_date,
  l.patient_name                             AS patient,
  l.accession                                AS mrn,
  l.facility,
  l.body_site                                AS location,
  l.pathology                                AS result_pathology,
  l.actions                                  AS actions_and_plans,
  l.comments                                 AS comments_assigned_to,
  l.flags,
  l.notifications,
  l.notes,
  l.provider                                 AS extra,
  CASE WHEN l.lesion_appropriate THEN 'Yes' ELSE 'No' END  AS appropriate_lesion,
  CASE WHEN l.srt_marked         THEN 'Yes' ELSE 'No' END  AS srt_signal_lesion,
  CASE WHEN s.vob_own_site       THEN 'path + VOB own site'
       WHEN s.vob_sharing_site   THEN 'path + VOB at a sibling'
       WHEN s.vob_enterprise     THEN 'path + VOB elsewhere'
       WHEN p.nm IS NOT NULL     THEN 'path only (name appears on a VOB out of window)'
       ELSE                           'path only'
  END                                        AS match_type,
  CASE WHEN p.nm IS NOT NULL THEN 'Yes' ELSE 'No' END      AS any_vob_for_this_patient,
  -- How many copies of this lesion were delivered, across every file. 1 is the normal
  -- case. Carried rather than discarded, the same way `vobs_for_this_biopsy` carries a
  -- collapsed duplicate verification: someone reconciling this tab against a PDF page
  -- needs to know a second copy existed and which file it came from.
  l._copies                                  AS copies_of_this_lesion,
  l.path_class, l.is_scanned, l.layout
FROM les l
LEFT JOIN scoped s
       ON s.account_name = l.account_name
      AND s.biopsy_date  = {_DT("l.collected_date")}
      AND s.patient_id   = coalesce(nullif(regexp_replace(trim(l.accession), '\\\\.0$', ''), ''),
                                    regexp_replace(lower(trim(l.patient_name)), '\\\\s+', ' '))
LEFT JOIN pat p ON p.nm = lower(trim(l.patient_name))
""")
_ru = spark.sql(f"""
SELECT count(*) AS rows,
       sum(copies_of_this_lesion - 1)                            AS collapsed,
       sum(CASE WHEN copies_of_this_lesion > 1 THEN 1 ELSE 0 END) AS with_a_copy,
       max(copies_of_this_lesion)                                 AS most_copies
FROM {C}.gold.raw_unfiltered
""").first()
print(f"wrote {C}.gold.raw_unfiltered: {_ru['rows']:,} lesion rows")
print(f"  {_ru['collapsed']:,} re-sent copies collapsed onto {_ru['with_a_copy']:,} "
      f"lesions; most copies of one lesion: {_ru['most_copies']}")

# This tab is a lesion count, so it must agree with the lesion count the patient-day. See wiki.
_lc = spark.sql(f"""
SELECT (SELECT count(*) FROM {C}.gold.raw_unfiltered)                 AS tab_rows,
       (SELECT sum(n_lesions) FROM {C}.staged.appropriate_patients)   AS appropriate_lesions
""").first()
print(f"  cross-check: {_lc['tab_rows']:,} rows here vs "
      f"{_lc['appropriate_lesions']:,} lesions on appropriate patient-days "
      f"(the tab is every lesion, appropriate or not, so it is the larger)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `gold.vob_path_match` and `gold.paths_marked_no_match`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.vob_path_match USING DELTA AS
SELECT
  coalesce(m.matched_folder, m.vob_folder)   AS account,
  m.vob_folder                               AS vob_owned_by,
  m.matched_folder                           AS biopsy_at,
  CASE WHEN m.matched                        THEN 'own site'
       WHEN m.leak_outcome = 'ambiguous namesake' THEN 'AMBIGUOUS NAMESAKE — do not trust'
       WHEN m.leak_folders IS NOT NULL       THEN 'biopsy at another folder'
       ELSE                                       'no biopsy found'
  END                                        AS match_kind,
  m.leak_folders                             AS also_matched_at,
  m.vob_id, m.account_id,
  m.patient_name_raw                         AS vob_patient,
  m.vob_created, m.status                    AS vob_status,
  m.biopsy_date, m.gap_days                  AS days_biopsy_to_vob,
  m.simulation_scheduled,
  (m.simulation_scheduled IS NOT NULL)       AS reached_sim,
  m.appropriate                              AS biopsy_day_appropriate,
  m.marked                                   AS biopsy_day_marked
FROM {VOBM} m
WHERE m.matched OR m.leak_folders IS NOT NULL
""")
print(f"wrote {C}.gold.vob_path_match: "
      f"{spark.table(f'{C}.gold.vob_path_match').count():,} matched VOBs")

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.paths_marked_no_match USING DELTA AS
SELECT
  d.account_name                             AS account,
  concat_ws(' | ', d.facilities)             AS facility,
  d.month_label                              AS month,
  d.biopsy_date                              AS date,
  d.patient_name                             AS patient,
  CASE WHEN d.keyed_on_mrn THEN d.patient_id END AS mrn,
  d.n_lesions                                AS lesions_that_date,
  d.has_actions,
  d.vob_mentioned                            AS vob_mentioned_in_log,
  d.any_unknown                              AS has_unknown_pathology,
  -- Two populations land on this tab once the namesake guard is on, and telling a
  -- reader they are the same thing would be a lie: one patient has no VOB anywhere,
  -- the other has one the guard declined to credit. Name both.
  CASE WHEN coalesce(s.vob_enterprise, false)
       THEN 'VOB only at an ambiguous namesake folder — not credited'
       ELSE 'no VOB at own site, a sibling, or anywhere in the enterprise'
  END                                        AS why_listed
FROM {DAYS} d
LEFT JOIN {SCOPE} s
       ON s.account_name = d.account_name
      AND s.patient_id   = d.patient_id
      AND s.biopsy_date  = d.biopsy_date
WHERE d.marked
  AND d.biopsy_date >= '{W0}' AND d.biopsy_date < '{W1}'
  -- THIS MUST BE THE EXACT NEGATION OF `vob_ent` IN §2, INCLUDING THE {{_ns}} GUARD.
  -- It used to read `AND NOT coalesce(s.vob_enterprise, false)` -- the raw scope
  -- column, ungated. A marked day whose only VOB was a namesake match then fell in a
  -- hole: vob_ent was false so vob_total skipped it, and vob_enterprise was true so
  -- this tab skipped it too. Measured 24 Aug: 6,738 + 10,181 = 16,919 against 17,051
  -- marked days -- 132 patient-days in neither bucket. The two must partition.
  AND NOT (coalesce(s.vob_own_site,     false)
        OR coalesce(s.vob_sharing_site, false)
        OR (coalesce(s.vob_enterprise,  false) {_ns}))
""")
_pm = spark.table(f"{C}.gold.paths_marked_no_match").count()
print(f"wrote {C}.gold.paths_marked_no_match: {_pm:,} marked patient-days with no VOB anywhere")

# The two tabs plus the matched-own count must account for every marked patient-day.
_recon = spark.sql(f"""
SELECT
  (SELECT sum(marked_total) FROM {C}.gold.account_summary)                  AS marked_days,
  (SELECT sum(vob_total)    FROM {C}.gold.account_summary)                  AS marked_with_a_vob,
  (SELECT count(*) FROM {C}.gold.paths_marked_no_match)                     AS marked_no_vob
""").first()
assert _recon["marked_with_a_vob"] + _recon["marked_no_vob"] == _recon["marked_days"], (
    f"marked patient-days do not reconcile: {_recon['marked_with_a_vob']:,} with a VOB "
    f"+ {_recon['marked_no_vob']:,} without = "
    f"{_recon['marked_with_a_vob'] + _recon['marked_no_vob']:,}, but there are "
    f"{_recon['marked_days']:,}. Every marked day is in exactly one of the two, so a "
    "gap means the scope join dropped rows or the window filters differ.")
print(f"reconciles: {_recon['marked_with_a_vob']:,} marked days with a VOB + "
      f"{_recon['marked_no_vob']:,} without = {_recon['marked_days']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. `gold.review` — one tab for everything that needs a human

# COMMAND ----------

_parts = []

# --- 1. unknown pathology, only where it could matter -----------------------------
_parts.append(f"""
SELECT 'unknown pathology' AS review_type,
       CASE WHEN srt_marked THEN 'check' ELSE 'hygiene' END AS severity,
       account_name AS account,
       trim(pathology) AS detail,
       concat('rows: ', cast(count(*) AS string),
              CASE WHEN srt_marked THEN ' — SRT-MARKED, so these DO count'
                   ELSE ' — not marked, counts nothing' END) AS note
FROM {LES}
WHERE path_class = 'unknown' AND trim(coalesce(pathology, '')) <> ''
GROUP BY account_name, trim(pathology), srt_marked
""")

# --- 2. tolerant name matches: the judgement calls the matcher made ---------------
_TOK = ("filter(split(regexp_replace(lower(coalesce(%s, '')), '[^a-z ]', ' '), ' +'), "
        "x -> length(x) > 1)")
_parts.append(f"""
SELECT 'name matched, not identical' AS review_type,
       'check' AS severity,
       m.matched_folder AS account,
       concat(m.patient_name_raw, '  <->  ', d.patient_name) AS detail,
       concat('gap ', cast(m.gap_days AS string), 'd',
              CASE WHEN m.simulation_scheduled IS NOT NULL THEN ', reached sim' ELSE '' END) AS note
FROM {VOBM} m
JOIN {DAYS} d ON d.account_name = m.matched_folder
             AND d.patient_id   = m.patient_id
             AND d.biopsy_date  = m.biopsy_date
WHERE m.matched
  AND array_sort({_TOK % 'm.patient_name_raw'}) <> array_sort({_TOK % 'd.patient_name'})
""")

# --- 3. ambiguous namesakes -------------------------------------------------------
_parts.append(f"""
SELECT 'ambiguous namesake' AS review_type,
       'hygiene' AS severity,
       coalesce(vob_folder, '(no folder)') AS account,
       patient_name_raw AS detail,
       concat('matched ', cast(size(leak_folders) AS string),
              ' folders, none of them its own — unresolvable without an MRN on the '
              'VOB feed') AS note
FROM {VOBM}
WHERE leak_outcome = 'ambiguous namesake'
""")

# --- 4. a path-log folder that reaches no account ---------------------------------
_parts.append(f"""
SELECT 'folder maps to no account' AS review_type,
       'blocking' AS severity,
       d.account_name AS account,
       'no primary row in raw.account_map' AS detail,
       concat(cast(count(*) AS string), ' patient-days reach no AccountId, so they '
              'score zero VOBs and read as a practice that converts nothing') AS note
FROM {DAYS} d
LEFT JOIN (SELECT DISTINCT folder_name FROM {C}.raw.account_map WHERE role = 'primary') m
       ON m.folder_name = d.account_name
WHERE m.folder_name IS NULL
GROUP BY d.account_name
""")

# --- 5. an account with VOBs and no path log --------------------------------------
_parts.append(f"""
SELECT 'VOBs but no path log' AS review_type,
       CASE WHEN e.account_id IS NOT NULL THEN 'hygiene' ELSE 'check' END AS severity,
       a.account_name AS account,
       concat(cast(count(DISTINCT v.vob_id) AS string), ' VOBs, no folder') AS detail,
       CASE WHEN e.account_id IS NOT NULL THEN 'recorded in raw.account_exclusion'
            ELSE 'NOT recorded — will read as a blank row on the summary' END AS note
FROM {ADIM} a
JOIN {C}.raw.vob v ON v.account_id = a.account_id
LEFT JOIN (SELECT DISTINCT account_id FROM {C}.raw.account_map) m ON m.account_id = a.account_id
LEFT JOIN {C}.raw.account_exclusion e ON e.account_id = a.account_id
WHERE m.account_id IS NULL
GROUP BY a.account_name, e.account_id
""")

# --- 6. an account pooled onto one folder while owning another --------------------
_parts.append(f"""
SELECT 'pooled AND owns a folder' AS review_type,
       'blocking' AS severity,
       o.folder_name AS account,
       concat('account ', p.account_id, ' is pooled onto ', p.folder_name) AS detail,
       'its own folder is counted too, so the practice is double-counted' AS note
FROM {C}.raw.account_map p
JOIN {C}.raw.account_map o ON o.account_id = p.account_id AND o.role = 'primary'
                          AND o.folder_name <> p.folder_name
WHERE p.role = 'pooled'
""")

# --- 7. a patient key that is not a key ------------------------------------------
_parts.append(f"""
SELECT 'patient key concentration' AS review_type,
       'blocking' AS severity,
       k.account_name AS account,
       concat('key ', k.patient_id, ' on ', cast(k.days AS string), ' patient-days') AS detail,
       'a patient key should be near-unique; a repeated one collapses several patients '
       'into one day and understates Appropriate' AS note
FROM (SELECT account_name, patient_id, count(*) AS days
      FROM {DAYS} GROUP BY account_name, patient_id) k
JOIN (SELECT account_name, count(*) AS t FROM {DAYS} GROUP BY account_name) a
     ON a.account_name = k.account_name
WHERE k.days >= 10 AND k.days >= 0.10 * a.t
""")

# --- 8. path-log VOB mention with no VOB found -----------------------------------
_parts.append(f"""
SELECT 'log mentions a VOB, none found' AS review_type,
       'check' AS severity,
       d.account_name AS account,
       concat(coalesce(d.patient_name, '(no name)'), ' ',
              cast(d.biopsy_date AS string)) AS detail,
       'the practice wrote a VOB mention on the path log and Salesforce has none' AS note
FROM {DAYS} d
LEFT JOIN {SCOPE} s ON s.account_name = d.account_name
                   AND s.patient_id = d.patient_id AND s.biopsy_date = d.biopsy_date
WHERE d.vob_mentioned AND NOT coalesce(s.vob_enterprise, false)
""")

spark.sql(f"CREATE OR REPLACE TABLE {C}.gold.review USING DELTA AS "
          + "\nUNION ALL\n".join(f"SELECT * FROM ({p})" for p in _parts))

print(f"wrote {C}.gold.review: {spark.table(f'{C}.gold.review').count():,} rows")
display(spark.sql(f"""
SELECT severity, review_type, count(*) AS rows, count(DISTINCT account) AS accounts
FROM {C}.gold.review
GROUP BY severity, review_type
ORDER BY CASE severity WHEN 'blocking' THEN 1 WHEN 'check' THEN 2 ELSE 3 END, rows DESC
"""))

_blocking = spark.sql(f"SELECT count(*) c FROM {C}.gold.review "
                      f"WHERE severity = 'blocking'").first()["c"]
if _blocking:
    print(f"\n*** {_blocking} BLOCKING review row(s) — a number is wrong until each is "
          f"resolved. Do not ship the workbook. ***")
    display(spark.sql(f"SELECT * FROM {C}.gold.review WHERE severity = 'blocking' "
                      f"ORDER BY review_type, account"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. `gold.trend` — one row per account per month

# COMMAND ----------

# This notebook has no window or run stamp of its own — prod carries both from
# 00_config, dev derives the window from its widgets. Defined here so a trend row can
# always be traced back to the run that wrote it, which is the first question anyone
# asks of a number that moved.
import datetime as _dt
WINDOW = f"{W0}_{W1}"
RUN_ID = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
print(f"window {WINDOW} · run {RUN_ID}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C}.gold.trend (
  account_name  STRING,
  month         STRING,
  appropriate   BIGINT,
  marked        BIGINT,
  sf_vob        BIGINT,
  sf_sim        BIGINT,
  has_path_log  BOOLEAN,
  window        STRING,
  run_id        STRING,
  updated_at    TIMESTAMP
) USING DELTA PARTITIONED BY (month)
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _trend_new AS
WITH pd AS (
  -- EVERY month the patient-day store holds, not just the window's. patient_days is
  -- additive now, so a month that has rolled out of the window keeps its path-log
  -- figures here rather than needing a re-run to recover them. Counted exactly as
  -- section 2 counts them, so the two cannot disagree.
  SELECT account_name,
         date_format(biopsy_date, 'yyyy-MM')                  AS month,
         sum(CASE WHEN appropriate THEN 1 ELSE 0 END)         AS appropriate,
         sum(CASE WHEN marked      THEN 1 ELSE 0 END)         AS marked
  FROM {_DAYS_TABLE}
  GROUP BY 1, 2
),
sf AS (
  -- Folder level, pooled over EVERY account mapped to it — same rule as sf_rollup,
  -- and no `role` filter, or a two-machine site reports half its verifications.
  SELECT m.folder_name AS account_name, r.month,
         sum(r.vob) AS sf_vob, sum(r.sim) AS sf_sim
  FROM (SELECT DISTINCT folder_name, account_id FROM {C}.raw.account_map) m
  JOIN {C}.raw.account_rollup r ON r.account_id = m.account_id
  GROUP BY 1, 2
)
SELECT
  coalesce(sf.account_name, pd.account_name)        AS account_name,
  coalesce(sf.month, pd.month)                      AS month,
  pd.appropriate,
  pd.marked,
  coalesce(sf.sf_vob, 0)                            AS sf_vob,
  coalesce(sf.sf_sim, 0)                            AS sf_sim,
  (pd.account_name IS NOT NULL)                     AS has_path_log,
  '{WINDOW}'                                        AS window,
  '{RUN_ID}'                                        AS run_id,
  current_timestamp()                               AS updated_at
FROM sf
FULL OUTER JOIN pd
  ON pd.account_name = sf.account_name AND pd.month = sf.month
WHERE coalesce(sf.account_name, pd.account_name) IS NOT NULL
""")

spark.sql(f"""
MERGE INTO {C}.gold.trend t
USING _trend_new s
   ON t.account_name = s.account_name AND t.month = s.month
WHEN MATCHED THEN UPDATE SET
  -- conditional: a later run whose window no longer covers this month must not
  -- overwrite the path-log figures it captured earlier
  t.appropriate  = coalesce(s.appropriate, t.appropriate),
  t.marked       = coalesce(s.marked,      t.marked),
  t.has_path_log = (s.has_path_log OR t.has_path_log),
  t.sf_vob       = s.sf_vob,
  t.sf_sim       = s.sf_sim,
  t.window       = s.window,
  t.run_id       = s.run_id,
  t.updated_at   = s.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

_t = spark.sql(f"""
SELECT count(*) AS rows, count(DISTINCT account_name) AS accounts,
       count(DISTINCT month) AS months, min(month) AS lo, max(month) AS hi,
       sum(CASE WHEN has_path_log THEN 1 ELSE 0 END) AS with_path_log
FROM {C}.gold.trend
""").first()
print(f"wrote {C}.gold.trend: {_t['rows']:,} rows · {_t['accounts']:,} accounts · "
      f"{_t['months']} months ({_t['lo']} .. {_t['hi']})")
print(f"  {_t['with_path_log']:,} rows carry a path log; the rest are Salesforce only")

display(spark.sql(f"""
SELECT month,
       sum(appropriate) AS appropriate, sum(marked) AS marked,
       sum(sf_vob) AS sf_vob, sum(sf_sim) AS sf_sim,
       round(sum(sf_sim) / nullif(sum(sf_vob), 0), 3) AS vob_to_sim,
       sum(CASE WHEN has_path_log THEN 1 ELSE 0 END)  AS accounts_with_path_log
FROM {C}.gold.trend GROUP BY month ORDER BY month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gates on the trend

# COMMAND ----------

_dupe = spark.sql(f"""
SELECT account_name, month, count(*) AS n FROM {C}.gold.trend
GROUP BY 1, 2 HAVING count(*) > 1
""")
_nd = _dupe.count()
if _nd:
    display(_dupe)
assert _nd == 0, (f"{_nd} (account, month) pair(s) appear twice in gold.trend — a "
                  f"duplicated month reads as growth")

_gap = spark.sql(f"""
WITH span AS (
  SELECT account_name, min(month) AS lo, max(month) AS hi, count(*) AS have
  FROM {C}.gold.trend GROUP BY account_name
)
SELECT *, months_between(to_date(concat(hi, '-01')), to_date(concat(lo, '-01'))) + 1 AS need
FROM span
WHERE have <> months_between(to_date(concat(hi, '-01')), to_date(concat(lo, '-01'))) + 1
""")
_ng = _gap.count()
if _ng:
    display(_gap)
    print(f"  *** {_ng} account(s) have a missing month inside their own range. A gap "
          f"plots as a dip and reads as a fall in volume.")
else:
    print("  every account is continuous across the months it covers")

_nopl = spark.sql(f"""
SELECT month, count(*) AS accounts FROM {C}.gold.trend
WHERE month IN ({','.join(repr(ym) for ym, _ in YM)}) AND NOT has_path_log
GROUP BY month ORDER BY month
""")
_nn = _nopl.count()
if _nn:
    display(_nopl)
    print(f"  *** {_nn} month(s) inside the window have accounts with no path-log "
          f"figures. Those read as verifications with no appropriate patients behind "
          f"them. Check the extract covered the window.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The shape the scorecards read

# COMMAND ----------

display(spark.sql(f"""
SELECT month,
       count(*)                                              AS accounts,
       sum(CASE WHEN has_path_log THEN 1 ELSE 0 END)         AS with_path_log,
       CASE WHEN month IN ({','.join(repr(ym) for ym, _ in YM)})
            THEN 'in the analysis window — all four series'
            ELSE 'outside the window — VOB and sims only' END AS role
FROM {C}.gold.trend GROUP BY month ORDER BY month
"""))

if _t["months"] > 1:
    display(spark.sql(f"""
    WITH t AS (
      SELECT account_name, month, appropriate, marked, sf_vob, sf_sim,
             lag(sf_vob) OVER (PARTITION BY account_name ORDER BY month) AS prev_vob,
             lag(sf_sim) OVER (PARTITION BY account_name ORDER BY month) AS prev_sim
      FROM {C}.gold.trend
    )
    SELECT account_name, month, appropriate, marked,
           prev_vob, sf_vob, sf_vob - prev_vob AS d_vob,
           prev_sim, sf_sim, sf_sim - prev_sim AS d_sim
    FROM t
    WHERE prev_vob IS NOT NULL AND month = (SELECT max(month) FROM {C}.gold.trend)
    ORDER BY d_vob DESC
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next