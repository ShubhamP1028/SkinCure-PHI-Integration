# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — Notebook 4: `vob_match` + the §8.3 leak check

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("gap_before", "30", "Days VOB may precede biopsy")
dbutils.widgets.text("gap_after", "180", "Days VOB may follow biopsy")
# [3 Sep] 150 -> 180. Widening can only ADD matches, never remove one, so every
# conversion rate can move up and none can move down. THREE OTHER PLACES READ
# THIS NUMBER and now take it as a widget rather than a literal: 08's maturity
# footnote, 08's timing-anchor bound, and 03's phone-date bound. Set all four to
# the same value or the sheet footnotes a window the matcher is not using.
dbutils.widgets.text("orphan_min", "5", "§8.3 orphan gate: min VOBs")
dbutils.widgets.text("shared_min", "10", "§8.3 shared/unlinked gate: min VOBs")
dbutils.widgets.text("shared_pct", "10", "§8.3 shared/unlinked gate: min % of own VOB")
# The analysis window. Used ONLY by the section 8 reconciliation, which has to count
# VOBs on the same four-month basis the prior master used -- raw.vob spans the whole
# pull, so an unfiltered count compares eight months against four.
dbutils.widgets.text("window_start", "2026-04-01", "Window start (inclusive)")
dbutils.widgets.text("window_end", "2026-10-01", "Window end (exclusive)")

CATALOG = dbutils.widgets.get("catalog")
GAP_B = -int(dbutils.widgets.get("gap_before"))
GAP_A = int(dbutils.widgets.get("gap_after"))
ORPHAN_MIN = int(dbutils.widgets.get("orphan_min"))
SHARED_MIN = int(dbutils.widgets.get("shared_min"))
SHARED_PCT = float(dbutils.widgets.get("shared_pct")) / 100.0
W0 = dbutils.widgets.get("window_start")
W1 = dbutils.widgets.get("window_end")

DAYS = f"{CATALOG}.staged.patient_days"

# ---- WINDOW SCOPE --------------------------------------------------------------. See wiki.
_DAYS_TABLE = DAYS
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _days_window AS
SELECT * FROM {_DAYS_TABLE}
WHERE biopsy_date >= date('{W0}') AND biopsy_date < date('{W1}')
""")
DAYS = "_days_window"
_held = spark.table(_DAYS_TABLE).count()
_dw = spark.sql(
    "SELECT count(*) AS n, count(DISTINCT date_format(biopsy_date,'yyyy-MM')) AS m "
    "FROM _days_window").first()
print(f"patient-days: {_dw['n']:,} in window across {_dw['m']} month(s), "
      f"{_held:,} held in total")

VOB = f"{CATALOG}.raw.vob"
AMAP = f"{CATALOG}.raw.account_map"
ADIM = f"{CATALOG}.raw.account_dim"

print(f"window: VOB may fall {GAP_B}..+{GAP_A} days from the biopsy")

# Does the VOB source actually SPAN that window? raw.vob is a view with date literals
# baked in at CREATE time, so it can be narrower than the matcher without any error --
# the join simply finds nothing and the patient reads as "no VOB found". A silently
# truncated VOB pull understates every conversion rate downstream.
_cov = spark.sql(f"""
  SELECT min(to_date(vob_created)) AS vob_min, max(to_date(vob_created)) AS vob_max,
         count(*) AS vobs
  FROM {CATALOG}.raw.vob
""").first()
_bw = spark.sql(f"""
  SELECT min(biopsy_date) AS b_min, max(biopsy_date) AS b_max
  FROM {DAYS}
""").first()

if _cov["vobs"] and _bw["b_min"]:
    from datetime import timedelta
    _need_lo = _bw["b_min"] + timedelta(days=GAP_B)      # GAP_B is negative
    _need_hi = _bw["b_max"] + timedelta(days=GAP_A)
    print(f"biopsies       : {_bw['b_min']} .. {_bw['b_max']}")
    print(f"VOBs available : {_cov['vob_min']} .. {_cov['vob_max']}  ({_cov['vobs']:,})")
    print(f"VOBs REQUIRED  : {_need_lo} .. {_need_hi}")
    _short_lo = _cov["vob_min"] > _need_lo
    _short_hi = _cov["vob_max"] < _need_hi
    if _short_lo or _short_hi:
        print("\n*** THE VOB SOURCE DOES NOT SPAN THE MATCH WINDOW ***")
        if _short_lo:
            print(f"    short at the start by {(_cov['vob_min'] - _need_lo).days} days")
        if _short_hi:
            print(f"    short at the end by {(_need_hi - _cov['vob_max']).days} days")
        print("    Widen vob_from / vob_to in Notebook 07 and re-run it — raw.vob is a")
        print("    view, so it takes seconds. Matches outside the pull are not errors,")
        print("    they are absent, and every conversion below is understated.")
    else:
        print("VOB pull spans the match window — OK")
else:
    print("cannot check coverage: raw.vob or staged.patient_days is empty")
print(f"§8.3 gates: orphan >= {ORPHAN_MIN} | shared/unlinked >= {SHARED_MIN} and >= {SHARED_PCT:.0%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `account_map` is many-to-one, and that is the fix for Maryville

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AMAP} (
  folder_name STRING,   -- exactly as it appears in SharePoint
  account_id  STRING,   -- Salesforce AccountId; MANY rows may share a folder_name
  role        STRING,   -- 'primary' (owns the path log) | 'pooled' (§8.2 second ID)
  note        STRING
) USING DELTA
""")

SEED_POOLED = [
    # folder_name (= the primary's exact Salesforce name, verified against AllAccounts)
    ("Schaberg Dermatology - Maryville", "0013h00000SbamkAAB", "primary", "owns the path log"),
    ("Schaberg Dermatology - Maryville", "001Do00000Hi5RcIAJ", "pooled",
     "Alton: no path log of its own, pathology logged at Maryville (01_Counting_Rules 8.1)"),
    # [29 Aug] Consolidated to "#1 & 2" on the July drop, the same way the four below
    # were in August. The comment further down warned about exactly this and it still
    # caught us: a seed keyed on a folder that no longer exists stops the notebook.
    ("Kathleen P Soe, D.O. (Virginia Steet Dermatology) - Dunedin #1 & 2", "001UN000004Bc4YYAS", "primary", ""),
    ("Kathleen P Soe, D.O. (Virginia Steet Dermatology) - Dunedin #1 & 2", "001UN00000BJgNOYA1", "pooled", "8.2"),
    ("Reading Dermatology - Reading #1 & 2", "0013h000009j2d7AAA", "primary", ""),
    ("Reading Dermatology - Reading #1 & 2", "001UN00000UpYO9YAN", "pooled", "8.2"),
    ("Renew Family Dermatology - Fort Payne #1 & 2", "0013h00000ScRSzAAN", "primary", ""),
    ("Renew Family Dermatology - Fort Payne #1 & 2", "001Do00000MQmX0IAL", "pooled", "8.2"),
    ("Biltmore Dermatology - Terre Haute #1 & 2", "0013h00001AZBkWAAX", "primary", ""),
    ("Biltmore Dermatology - Terre Haute #1 & 2", "0013h00002AhD4fAAF", "pooled", "8.2"),
    ("Seraly Dermatology - McMurray #1 & 2", "001UN0000096FHOYA2", "primary", ""),
    ("Seraly Dermatology - McMurray #1 & 2", "001UN00000TiqWsYAJ", "pooled", "8.2"),
]

# The folder names above ARE the Salesforce account names — verified: all 54 Phase-1. See wiki.
_seed_folders = {r[0] for r in SEED_POOLED}
_have = {r["account_name"] for r in
         spark.sql(f"SELECT DISTINCT account_name FROM {CATALOG}.raw.path_log_rows").collect()}
_missing = sorted(_seed_folders - _have)
if _missing:
    print("*** SEED_POOLED names with NO matching path-log folder: ***")
    for m in _missing:
        near = [h for h in _have if h.startswith(m.split(" #")[0][:24])]
        print(f"    {m!r}\n      candidates: {near}")
    # The seed only writes when the map is EMPTY. When the map is already built the
    # stale name cannot affect anything, so stopping the run is out of proportion --
    # and it stopped a run whose map was already correct. Loud either way; fatal only
    # when it would actually change what gets written.
    if spark.table(AMAP).count() == 0:
        raise ValueError(
            f"{len(_missing)} pooled seed(s) reference a folder that does not exist, and "
            f"{AMAP} is EMPTY so this seed is what builds it. Fix the names in "
            f"SEED_POOLED -- do not proceed, the sister sites' VOBs will not be counted.")
    print(f"    {AMAP} is already populated, so the seed is inert and this does not stop "
          f"the run. Fix the names before the next fresh build.")
print(f"all {len(_seed_folders)} pooled seed folder names resolve")

if spark.table(AMAP).count() == 0:
    spark.createDataFrame(
        SEED_POOLED, "folder_name STRING, account_id STRING, role STRING, note STRING"
    ).write.mode("append").saveAsTable(AMAP)
    print(f"seeded {len(SEED_POOLED)} rows (6 practices) — the other ~176 accounts still need mapping")
else:
    print(f"{AMAP} already populated ({spark.table(AMAP).count()} rows) — left alone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Map integrity — run this before believing any number below

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tolerant name matching — a direct port of `fuzzy_name.py`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import re


def _toks(n):
    n = re.sub(r"[^a-z ]", " ", str(n or "").lower())
    return frozenset(t for t in n.split() if len(t) > 1)


def _ed1(a, b):
    """True if a and b are within one edit (insert / delete / substitute)."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la > lb:
        a, b, la, lb = b, a, lb, la
    i = j = diff = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            diff += 1
            if diff > 1:
                return False
            if la == lb:
                i += 1
                j += 1
            else:
                j += 1
    return True


def _tok_close(a, b):
    if a == b:
        return True
    if len(a) < 4 or len(b) < 4:      # never fuzz initials
        return False
    if a.startswith(b) or b.startswith(a):    # Wood / Woods
        return abs(len(a) - len(b)) <= 2
    return _ed1(a, b)


def name_ok_fuzzy(a_name, b_name, min_tokens=2):
    """Tolerant name equality.

    [25 Aug] ONE SHORT UNMATCHED TOKEN IS FORGIVEN. Every token of the shorter
    name used to be required to find a counterpart, and `_tok_close` refuses to
    fuzz anything under 4 characters ("never fuzz initials"). So a short middle
    name or a suffix that only one side carries -- `ann` vs `beth`, `jo` vs
    `ann`, `jr` vs `sr`, `ii` vs `iii` -- rejected the whole name even when the
    first name AND surname matched exactly.

    Measured 25 Aug: of 3,464 in-window VOBs matching no biopsy at all, 369 share
    two or more exact name tokens with a patient at their own account. Blocking
    hit; this function refused them.

    The `strong >= 2` guard is what keeps it honest. A skipped token must be
    under 4 characters, only one may be skipped, and two FULL tokens must still
    have matched -- so the 2-token case stays strict and ("Smith, John",
    "Smith, Jane") still fails, because `john` is four letters and can never be
    the skipped one. All 11 documented §5.4 cases reproduce unchanged.
    """
    a, b = _toks(a_name), _toks(b_name)
    if not a or not b:
        return False
    if a == b or a <= b or b <= a:            # the existing exact rule
        return True
    small, large = (a, b) if len(a) <= len(b) else (b, a)
    if len(small) < min_tokens:
        return False
    used, strong, skipped = set(), 0, 0
    # Longest first: a full name claims its counterpart before a short token can
    # consume it, so `strong` counts what it should.
    for t in sorted(small, key=lambda x: -len(x)):
        hit = next((u for u in large if u not in used and _tok_close(t, u)), None)
        if hit is None:
            if len(t) < 4 and skipped == 0:
                skipped += 1
                continue
            return False
        used.add(hit)
        if len(t) >= 4:
            strong += 1
    return strong >= 2


spark.udf.register("name_ok_fuzzy", name_ok_fuzzy, BooleanType())

# regression against the cases §5.4 names
_NAME_CASES = [
    # --- the variants 5.4 documents recovering -----------------------------
    ("Wood, Mary", "Mary Woods", True, "documented recovery"),
    ("Patrica Ellis", "Patricia Ellis", True, "documented recovery"),
    ("Murphee, John A", "John Murphree", True, "documented recovery"),
    ("OMALLEY, SEAN", "O'Malley, Sean", True, "punctuation"),
    ("Paul,NaomiR.", "Naomi Paul", True, "OCR run-together"),
    ("Wilson, James", "Wilson, James R", True, "subset"),

    # --- must NOT match ----------------------------------------------------
    ("Smith, John", "Smith, Jane", False, "different first name, 4+ letters apart"),
    ("Chan", "Chang", False, "one token each, not equal -> min_tokens blocks the fuzz"),
    ("Lee, J", "Lea, K", False, "initials dropped, surnames differ -> min_tokens blocks"),

    # --- TRUE, and it looks wrong ------------------------------------------. See wiki.
    ("Lee, J", "Lee, K", True, "both reduce to {lee}; exact branch precedes min_tokens"),
    ("Smith", "Smith, John", True, "bare surname is a SUBSET - matches by design"),

    # --- [25 Aug] the short-token skip ------------------------------------
    ("Mary Ann Wood",  "Mary Beth Wood", True,  "differing SHORT middle name"),
    ("John Smith Jr",  "John Smith Sr",  True,  "suffix differs"),
    ("Wood, Mary Jo",  "Mary Ann Wood",  True,  "short middle vs short middle"),
    ("Smith, John M",  "Smith, Jane M",  False, "first names 4+ letters -> not skippable"),
    ("Brown, Ann",     "Brown, Eva",     False, "both first names short, but only 2 tokens"),
]
_fail = [(a, b, w, n) for a, b, w, n in _NAME_CASES if name_ok_fuzzy(a, b) != w]
if _fail:
    for a, b, w, n in _fail:
        print(f"  FAIL {a!r} vs {b!r}: got {name_ok_fuzzy(a, b)}, want {w}  ({n})")
    raise AssertionError(f"{len(_fail)} name-matching regressions")
print(f"name matching OK — {len(_NAME_CASES)} cases")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Candidate pairs — every VOB against every path log, no pre-filter

# COMMAND ----------

# Fail by NAME, not with UNRESOLVED_COLUMN three cells down. `any_bcc` / `any_scc`
# were added to notebook 03 on 25 Aug so the matcher can corroborate a name match
# against what the biopsy showed. 03 must run before 04 -- it always had to, but
# until now nothing depended on it hard enough to notice.
_pd_cols = set(spark.table(DAYS).columns)
_need_pd = {"any_bcc", "any_scc"}
if not _need_pd <= _pd_cols:
    raise ValueError(
        f"{DAYS} is missing {sorted(_need_pd - _pd_cols)}. Those columns come from "
        "notebook 03, added 25 Aug for the cell-type corroboration. Re-run 03 "
        "(patient_days) before this notebook — the run order is 03 -> 04.")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _day_tok AS
SELECT account_name, patient_id, patient_name, biopsy_date, appropriate, marked,
       any_bcc, any_scc, tok
FROM (
  SELECT account_name, patient_id, patient_name, biopsy_date, appropriate, marked,
         coalesce(any_bcc, false) AS any_bcc, coalesce(any_scc, false) AS any_scc,
         explode(split(regexp_replace(lower(coalesce(patient_name,'')), '[^a-z ]', ' '), ' +')) AS tok
  FROM {DAYS}
) WHERE length(tok) > 1
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _vob_tok AS
SELECT vob_id, account_id, patient_name_raw, vob_created, simulation_scheduled, status, tok
FROM (
  SELECT vob_id, account_id, patient_name_raw, vob_created, simulation_scheduled, status,
         explode(split(regexp_replace(lower(coalesce(patient_name_raw,'')), '[^a-z ]', ' '), ' +')) AS tok
  FROM {VOB}
) WHERE length(tok) > 1
""")

# ---------------------------------------------------------------- cell type. See wiki.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _vob_cell AS
SELECT Id                                                          AS vob_id,
       max(CASE WHEN lower(cell_type) LIKE '%basal%'    THEN 1 ELSE 0 END) = 1 AS v_bcc,
       max(CASE WHEN lower(cell_type) LIKE '%squamous%' THEN 1 ELSE 0 END) = 1 AS v_scc
FROM {CATALOG}.raw.vob_sites_nonkeloid
GROUP BY Id
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _pairs AS
SELECT DISTINCT
  d.account_name, d.patient_id, d.patient_name, d.biopsy_date, d.appropriate, d.marked,
  v.vob_id, v.account_id AS vob_account_id, v.patient_name_raw,
  v.vob_created, v.simulation_scheduled, v.status,
  datediff(to_date(v.vob_created), d.biopsy_date) AS gap_days,
  -- THREE STATES, AND THE MIDDLE ONE IS THE POINT.
  --   1  the VOB's cell type is also on the biopsy that day
  --   0  one side records nothing comparable -- must cost NOTHING
  --  -1  both sides are populated and they disagree
  -- A missing value can never demote a candidate; only a contradiction can.
  CASE WHEN c.vob_id IS NULL
         OR (NOT c.v_bcc AND NOT c.v_scc)
         OR (NOT d.any_bcc AND NOT d.any_scc)                        THEN 0
       WHEN (c.v_bcc AND d.any_bcc) OR (c.v_scc AND d.any_scc)       THEN 1
       ELSE -1 END                                   AS cell_state
FROM _day_tok d
JOIN _vob_tok v ON v.tok = d.tok           -- BLOCKING: >= 1 exactly-equal token
LEFT JOIN _vob_cell c ON c.vob_id = v.vob_id
WHERE name_ok_fuzzy(d.patient_name, v.patient_name_raw)
  AND datediff(to_date(v.vob_created), d.biopsy_date) BETWEEN {GAP_B} AND {GAP_A}
""")

print(f"candidate (patient-day, VOB) pairs: {spark.table('_pairs').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resolve each VOB to one outcome

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _ranked AS
SELECT * FROM (
  SELECT p.*,
         m.folder_name AS vob_folder,
         (m.folder_name = p.account_name) AS folder_owns_vob,
         -- `p.patient_id` IS A CORRECTNESS TERM, NOT COSMETIC ORDERING.
         -- The three keys above tie completely whenever two patient-days sit at the
         -- same account on the same date -- same `appropriate`, same gap sign, same
         -- abs(gap_days) -- which is exactly the namesake case. row_number() then
         -- picks arbitrarily, and because `_resolved` is a TEMP VIEW (re-executed per
         -- action, not materialised) `staged.vob_match` and `staged.patient_vob_scope`
         -- are written by two separate actions and could crown different winners.
         -- Measured 24 Aug: 5 appropriate patient-days where notebook 08's
         -- has_vob and vob_at_own_site disagreed -- mirror pairs at Heights New
         -- Braunfels 2026-04-09 and Preston Wake Forest 2026-05-12.
         row_number() OVER (
           PARTITION BY p.vob_id, p.account_name
           -- Cell type leads: where one candidate's pathology corroborates the
           -- VOB and another's contradicts it, that decides the attribution
           -- before any date heuristic gets a say. It cannot ADD or REMOVE a
           -- match -- the pool is already fixed by name and gap -- it only
           -- chooses better within it.
           ORDER BY p.cell_state DESC,
                    p.appropriate DESC, (p.gap_days >= 0) DESC, abs(p.gap_days),
                    p.patient_id, p.biopsy_date
         ) AS rn
  FROM _pairs p
  LEFT JOIN {AMAP} m ON m.account_id = p.vob_account_id
) WHERE rn = 1
""")

# how many DIFFERENT folders does each VOB match? >1 with no owning folder = namesake risk
spark.sql("""
CREATE OR REPLACE TEMP VIEW _vob_scope AS
SELECT vob_id,
       count(DISTINCT account_name)                                     AS n_folders,
       max(CASE WHEN folder_owns_vob THEN 1 ELSE 0 END) = 1             AS matched_own_folder,
       max(vob_folder)                                                  AS vob_folder
FROM _ranked GROUP BY vob_id
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _resolved_raw AS
SELECT
  r.*,
  s.n_folders,
  s.matched_own_folder,
  CASE
    WHEN r.folder_owns_vob                       THEN 'matched own'
    WHEN s.matched_own_folder                    THEN 'matched elsewhere too'
    WHEN s.n_folders > 1                         THEN 'ambiguous namesake'
    ELSE                                              'matched other only'
  END AS outcome
FROM _ranked r JOIN _vob_scope s ON s.vob_id = r.vob_id
""")

# [25 Aug] PATHOLOGY SETTLES THE MULTI-ACCOUNT CANDIDATES. See wiki.
spark.sql("""
CREATE OR REPLACE TEMP VIEW _resolved AS
SELECT
  x.* EXCEPT (outcome, _n_surv),
  CASE
    -- both directions of "this candidate is not the one"
    WHEN x.outcome = 'ambiguous namesake' AND x.cell_state = -1
      THEN 'namesake refuted (pathology)'
    -- exactly one candidate survived the strike-out -> it IS the one
    WHEN x.outcome = 'ambiguous namesake' AND x._n_surv = 1
      THEN 'namesake resolved (pathology)'
    ELSE x.outcome
  END                                                        AS outcome,
  (x.outcome = 'ambiguous namesake' AND x.cell_state = -1)   AS refuted
FROM (
  SELECT r.*,
         -- candidates left once contradicted ones are struck. _ranked is one row
         -- per (vob, folder), so counting rows counts folders.
         sum(CASE WHEN r.cell_state >= 0 THEN 1 ELSE 0 END)
           OVER (PARTITION BY r.vob_id)                      AS _n_surv
  FROM _resolved_raw r
) x
""")

display(spark.sql("""
SELECT outcome, count(DISTINCT vob_id) AS vobs, count(*) AS candidate_rows
FROM _resolved GROUP BY outcome ORDER BY vobs DESC
"""))
print("'namesake resolved (pathology)' now counts as a real leak attribution.")
print("'namesake refuted (pathology)' counts NOWHERE -- it no longer sets")
print("vob_enterprise, no longer contributes leak_folders, and is excluded from")
print("the leak detail. That is the change that turns enterprise into a count.")

# ---- IMPACT GATE ----------------------------------------------------------
# The change is only defensible if it moves what it was argued to move and
# nothing else. Two numbers, both asserted rather than printed.
_imp = spark.sql("""
SELECT
  count(*)                                                                AS rows_total,
  count(DISTINCT CASE WHEN refuted THEN vob_id END)                       AS vobs_touched,
  sum(CASE WHEN refuted THEN 1 ELSE 0 END)                                AS credits_removed,
  count(DISTINCT CASE WHEN outcome = 'namesake resolved (pathology)'
                      THEN vob_id END)                                    AS vobs_resolved,
  count(DISTINCT CASE WHEN outcome = 'ambiguous namesake' THEN vob_id END) AS still_ambiguous,
  sum(CASE WHEN refuted AND outcome = 'namesake resolved (pathology)'
           THEN 1 ELSE 0 END)                                             AS MUST_BE_ZERO
FROM _resolved
""").collect()[0]

assert _imp["rows_total"] > 0, (
    "_resolved is EMPTY -- this is not a namesake result, the matcher produced no "
    "candidate pairs at all.")
assert _imp["MUST_BE_ZERO"] == 0, (
    f"{_imp['MUST_BE_ZERO']:,} rows are both refuted and resolved. The CASE in "
    "_resolved has been reordered -- the refuted branch must win.")
# Sanity band, not an exact expectation: measured 25 Aug at 575 resolved / 99
# fully refuted out of 1,381 namesake VOBs. A large move means the name test or
# the cell-type view changed underneath this, and the 1.5% error floor that
# justified the decision no longer applies.
if not (300 <= _imp["vobs_resolved"] <= 900):
    print(f"  !! REVIEW: resolved={_imp['vobs_resolved']:,}, expected ~575. The name "
          f"test or _vob_cell moved; re-measure the contradiction rate on "
          f"matched-own before trusting this.")

# PERSIST THE CANDIDATE GRAIN. `_resolved` is a TEMP VIEW, so every question about. See wiki.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.staged.vob_candidates USING DELTA AS
SELECT vob_id, account_name AS candidate_folder, vob_folder AS owned_by,
       folder_owns_vob, outcome, refuted, n_folders, matched_own_folder,
       cell_state, gap_days, biopsy_date, vob_created, appropriate, marked
FROM _resolved
""")
_vc = spark.table(f"{CATALOG}.staged.vob_candidates")
print(f"wrote {CATALOG}.staged.vob_candidates: {_vc.count():,} (vob, candidate folder) rows, "
      f"{_vc.select('vob_id').distinct().count():,} VOBs")
print("  ambiguous namesakes are queryable from here AFTER the run -- see")
print("  claude/Open_Items for the resolution query.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write `staged.vob_match` — one row per VOB

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.staged.vob_match USING DELTA AS
WITH own AS (
  SELECT vob_id, account_name AS matched_folder, patient_id, biopsy_date,
         appropriate, marked, gap_days
  FROM _resolved WHERE folder_owns_vob
),
leak AS (
  -- `NOT refuted` is load-bearing. `vob_with_a_biopsy_anywhere` in 09 is
  -- (matched OR leak_folders IS NOT NULL), so a refuted candidate left in here
  -- would still make the VOB read as "has a biopsy somewhere" on the strength of
  -- a match the pathology contradicts.
  SELECT vob_id,
         array_sort(array_distinct(collect_list(account_name))) AS leak_folders,
         min(outcome)                                           AS leak_outcome,
         -- the biopsy behind a LEAKED VOB. Without it a leak has no biopsy date
         -- at all in this table (biopsy_date comes from `own`), so it cannot be
         -- cohorted by biopsy month -- which is what the monthly columns now do.
         min(biopsy_date)                                       AS leak_biopsy_date,
         min(patient_id)                                        AS leak_patient_id
  FROM _resolved WHERE NOT folder_owns_vob AND NOT refuted GROUP BY vob_id
),
joined AS (
  SELECT
    v.vob_id, v.account_id, v.patient_name_raw, v.vob_created, v.status,
    v.simulation_scheduled,
    -- [10 Sep, Becky] TWO STAGES, not two definitions. simulation_scheduled is when
    -- the patient was meant to come in; simulation_actual is the treatment log saying
    -- they did.
    --
    -- NOTE the SIM band on Pam's Summary does NOT come through here -- it comes from
    -- 07b (ptl.sim, off comm_silver) via gold.sf_rollup, and is already ATTENDED.
    -- What comes through here is the RATIO: group_has_sim below feeds 08's simm_*,
    -- hence VOB -> Sim and current_sims_per_month, and that IS on the scheduled
    -- basis. So the sheet carries attended counts beside a scheduled ratio. See
    -- 05 §6b. Not changed here -- it needs a decision, not a patch.
    v.simulation_actual,
    m.folder_name                                  AS vob_folder,
    o.matched_folder, o.patient_id, o.biopsy_date, o.appropriate, o.marked, o.gap_days,
    (o.vob_id IS NOT NULL)                         AS matched,
    (v.simulation_scheduled IS NOT NULL)           AS converted,
    l.leak_folders, l.leak_outcome, l.leak_biopsy_date,
    (o.vob_id IS NULL AND l.vob_id IS NOT NULL)    AS is_leak,
    -- THE COHORT KEY. Which patient-day produced this VOB, whether it matched at
    -- its own folder or leaked to another. Everything monthly now keys on this.
    coalesce(o.biopsy_date, l.leak_biopsy_date)    AS cohort_biopsy_date,
    coalesce(o.patient_id, l.leak_patient_id)      AS cohort_patient_id
  FROM {VOB} v
  LEFT JOIN {AMAP} m ON m.account_id = v.account_id
  LEFT JOIN own  o ON o.vob_id = v.vob_id
  LEFT JOIN leak l ON l.vob_id = v.vob_id
)
-- ---------------------------------------------------------------------------
-- §6 DUPLICATE PATIENT. Counting rules §6 defines a bucket -- "the 2nd or later
-- matched VOB for the same patient at the same account, keeping the earliest" --
-- that was never implemented. Two VOBs for one patient-day therefore both landed
-- in the VOB -> Sim denominator, and notebook 08 printed a warning about it every
-- run without anything acting on it.
--
-- Grain per Becky, 25 Aug: ONE PER (folder, patient, biopsy date). A patient with
-- two separate biopsy dates legitimately has two VOBs; two VOBs against the same
-- biopsy is one verification recorded twice.
--
-- CONVERSION IS PRESERVED BY OR-ing, NOT BY KEEPING THE FIRST ROW'S VALUE. §6
-- says "keeping the earliest", but if the earliest VOB has no simulation and the
-- second does, keeping the earliest verbatim would delete a real conversion and
-- understate VOB -> Sim. So the survivor is the earliest VOB, and it carries
-- `group_has_sim` / `group_appropriate` / `group_marked` OR-ed across the group --
-- the same "any of those" rule the patient-day rollup already uses.
-- ---------------------------------------------------------------------------
SELECT
  j.* EXCEPT (cohort_patient_id),
  CASE WHEN j.cohort_biopsy_date IS NULL THEN 1
       ELSE row_number() OVER (
         PARTITION BY coalesce(j.matched_folder, j.vob_folder),
                      j.cohort_patient_id, j.cohort_biopsy_date
         -- earliest first, then vob_id so a same-day tie is deterministic
         ORDER BY j.vob_created, j.vob_id)
  END                                            AS patient_vob_seq,
  CASE WHEN j.cohort_biopsy_date IS NULL THEN false
       ELSE row_number() OVER (
         PARTITION BY coalesce(j.matched_folder, j.vob_folder),
                      j.cohort_patient_id, j.cohort_biopsy_date
         ORDER BY j.vob_created, j.vob_id) > 1
  END                                            AS is_duplicate_patient_vob,
  -- OR-ed across the duplicate group, so collapsing never deletes a signal
  CASE WHEN j.cohort_biopsy_date IS NULL THEN (j.simulation_scheduled IS NOT NULL)
       ELSE max(CASE WHEN j.simulation_scheduled IS NOT NULL THEN 1 ELSE 0 END) OVER (
         PARTITION BY coalesce(j.matched_folder, j.vob_folder),
                      j.cohort_patient_id, j.cohort_biopsy_date) = 1
  END                                            AS group_has_sim,
  -- [10 Sep] the same OR, on the ATTENDED date. Carried so the two stages can be
  -- compared at the SAME grain -- group_has_sim is OR-ed across a §6 duplicate
  -- group, so comparing it against a bare simulation_actual would be apples to
  -- oranges. NOTHING reads this yet; group_has_sim above is still what counts.
  -- group_has_sim AND NOT group_simulated_actual, once the scheduled date has
  -- passed, is the no-show at patient-day grain.
  CASE WHEN j.cohort_biopsy_date IS NULL THEN (j.simulation_actual IS NOT NULL)
       ELSE max(CASE WHEN j.simulation_actual IS NOT NULL THEN 1 ELSE 0 END) OVER (
         PARTITION BY coalesce(j.matched_folder, j.vob_folder),
                      j.cohort_patient_id, j.cohort_biopsy_date) = 1
  END                                            AS group_simulated_actual,
  CASE WHEN j.cohort_biopsy_date IS NULL THEN j.appropriate
       ELSE max(CASE WHEN j.appropriate THEN 1 ELSE 0 END) OVER (
         PARTITION BY coalesce(j.matched_folder, j.vob_folder),
                      j.cohort_patient_id, j.cohort_biopsy_date) = 1
  END                                            AS group_appropriate,
  CASE WHEN j.cohort_biopsy_date IS NULL THEN j.marked
       ELSE max(CASE WHEN j.marked THEN 1 ELSE 0 END) OVER (
         PARTITION BY coalesce(j.matched_folder, j.vob_folder),
                      j.cohort_patient_id, j.cohort_biopsy_date) = 1
  END                                            AS group_marked,
  -- How many verifications this one biopsy produced. Carried so a collapsed
  -- duplicate is VISIBLE on the review sheets rather than silently absent --
  -- "this patient has 2 VOBs, we counted 1" is a different and much less
  -- alarming statement than a VOB that appears to have vanished.
  CASE WHEN j.cohort_biopsy_date IS NULL THEN 1
       ELSE count(*) OVER (
         PARTITION BY coalesce(j.matched_folder, j.vob_folder),
                      j.cohort_patient_id, j.cohort_biopsy_date)
  END                                            AS vobs_for_this_biopsy
FROM joined j
""")

print(f"wrote {CATALOG}.staged.vob_match: {spark.table(f'{CATALOG}.staged.vob_match').count():,} VOBs")

display(spark.sql(f"""
SELECT
  count(*)                                            AS vobs,
  sum(CASE WHEN vob_folder IS NULL THEN 1 ELSE 0 END) AS no_folder_mapped,
  sum(CASE WHEN matched THEN 1 ELSE 0 END)            AS matched_to_own_folder,
  sum(CASE WHEN is_leak THEN 1 ELSE 0 END)            AS leaked
FROM {CATALOG}.staged.vob_match
"""))

# ---- §6 DUPLICATE-PATIENT GATE --------------------------------------------
_dup = spark.sql(f"""
SELECT
  count(*)                                                        AS vobs,
  sum(CASE WHEN is_duplicate_patient_vob THEN 1 ELSE 0 END)        AS duplicates,
  sum(CASE WHEN is_duplicate_patient_vob AND group_has_sim
                AND simulation_scheduled IS NULL THEN 1 ELSE 0 END) AS sims_rescued_by_or,
  max(patient_vob_seq)                                            AS deepest_group,
  -- a VOB with no biopsy behind it is seq 1 by definition and can never be a dup
  sum(CASE WHEN cohort_biopsy_date IS NULL
                AND is_duplicate_patient_vob THEN 1 ELSE 0 END)    AS MUST_BE_ZERO
FROM {CATALOG}.staged.vob_match
""").first()

print(f"  §6 duplicate patient: {_dup['duplicates']:,} of {_dup['vobs']:,} VOBs are a "
      f"2nd+ verification of the same (folder, patient, biopsy date)")
print(f"     deepest duplicate group: {_dup['deepest_group']}")
print(f"     conversions preserved by OR-ing that keeping-the-earliest would have "
      f"deleted: {_dup['sims_rescued_by_or']:,}")

assert _dup["MUST_BE_ZERO"] == 0, (
    "a VOB with no biopsy behind it was marked a duplicate. It has no cohort key, "
    "so it cannot be the 2nd of anything -- the CASE guard on cohort_biopsy_date "
    "has been broken.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6a. [21 Aug] VOB scope per patient-day — own site / sharing site / enterprise

# COMMAND ----------

#     g = re.split(r"\s+-\s+|\s*-\s*(?=[A-Z])", name)[0].strip().lower(). See wiki.
_grp = ("trim(replace(lower(regexp_replace(%s, "
        "'\\\\s*-\\\\s*(?=[A-Z]).*$|\\\\s+-\\\\s+.*$', '')), ' & ', ' and '))")

# 5.6 explicit aliases. One pair today (the Soe parenthetical); the 'alias'
# disposition in 8.3 adds rows here.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.raw.group_alias (
  raw_group STRING,   -- the computed group, lowercased
  alias     STRING,   -- what it should be treated as
  note      STRING
) USING DELTA
""")
if spark.table(f"{CATALOG}.raw.group_alias").count() == 0:
    spark.createDataFrame(
        [("kathleen p soe, d.o. (virginia steet dermatology)", "kathleen p soe",
          "parenthetical trading name broke the match; 32 of #2's 35 missing VOBs were at #1"),
         ("kathleen p soe, d.o. dermatology", "kathleen p soe", "")],
        "raw_group STRING, alias STRING, note STRING",
    ).write.mode("append").saveAsTable(f"{CATALOG}.raw.group_alias")

_ga = f"{CATALOG}.raw.group_alias"

# COMMAND ----------

# practice group per FOLDER, via the folder's primary AccountId. Falls back to the
# folder name itself so an unmapped folder is its own group rather than NULL — a NULL
# group would silently make every unmapped folder a peer of every other.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _folder_group AS
SELECT f.account_name AS folder_name,
       -- [25 Aug] `ga.alias` FIRST, and this is the fix.
       --
       -- raw.group_alias existed and was applied ONLY in the leak classifier
       -- (_leak_detail), never here -- because the table was created further down
       -- the notebook than this view. An ordering accident, not a decision.
       --
       -- The consequence was self-contradictory: an aliased pair was classified as
       -- a LEAK (the alias made the two folders one group there) and simultaneously
       -- denied SIBLING credit (no alias here, so they were strangers). The same
       -- two folders were "the same practice" and "different practices" in one run.
       -- That understates vob_sibling and marked_to_vob_sibling, which is the
       -- middle rung of the funnel.
       --
       -- An alias row is an explicit human override, so it outranks the Salesforce
       -- practice_group; the computed name is the last resort, and never NULL --
       -- a NULL group would make every unmapped folder a peer of every other.
       coalesce(ga.alias, d.practice_group, lower(f.account_name)) AS practice_group
FROM (SELECT DISTINCT account_name FROM {DAYS}) f
LEFT JOIN (SELECT folder_name, min(account_id) AS account_id
           FROM {AMAP} WHERE role = 'primary' GROUP BY folder_name) m
       ON m.folder_name = f.account_name
LEFT JOIN {CATALOG}.raw.account_dim d ON d.account_id = m.account_id
LEFT JOIN {_ga} ga ON ga.raw_group = {_grp % 'f.account_name'}
""")

# Say what the alias actually did. A table that silently matches nothing looks
# identical to one that is working.
_ali = spark.sql(f"""
SELECT count(*) AS alias_rows,
       sum(CASE WHEN hit THEN 1 ELSE 0 END) AS folders_realigned
FROM (SELECT a.raw_group,
             exists(collect_list(f.account_name), x -> true) AS hit
      FROM {_ga} a
      LEFT JOIN (SELECT DISTINCT account_name FROM {DAYS}) f
             ON {_grp % 'f.account_name'} = a.raw_group
      GROUP BY a.raw_group)
""").first()
print(f"group_alias: {_ali['alias_rows']} rows, {_ali['folders_realigned']} bind to a folder")
if _ali["alias_rows"] and not _ali["folders_realigned"]:
    print("  !! REVIEW: every alias row binds to nothing. The alias key is the "
          "COMPUTED group (text before the first dash, '&' -> 'and'), so a row "
          "written against a display name will never fire.")

# _resolved is already one row per (vob_id, folder-the-patient-matched-in) with the
# best-ranked patient-day chosen. Re-aggregating it to the patient-day grain is
# therefore a pure roll-up: no new matching, no second interpretation of the rule.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.staged.patient_vob_scope USING DELTA AS
WITH r AS (
  SELECT
    x.account_name, x.patient_id, x.biopsy_date, x.vob_id,
    x.folder_owns_vob,
    x.vob_folder,
    (x.simulation_scheduled IS NOT NULL)    AS has_sim,
    gm.practice_group                       AS matched_group,
    gv.practice_group                       AS vob_group
  FROM _resolved x
  LEFT JOIN _folder_group gm ON gm.folder_name = x.account_name
  LEFT JOIN _folder_group gv ON gv.folder_name = x.vob_folder
  -- A namesake candidate the pathology contradicts must not set vob_enterprise.
  -- This is where the false enterprise credit came from: a same-named patient in
  -- another folder made a patient-day read as verified. Dropping refuted rows here
  -- is the whole point of the "full" option.
  WHERE NOT x.refuted
)
SELECT
  account_name,
  patient_id,
  biopsy_date,
  -- 1. own site
  max(CASE WHEN folder_owns_vob THEN 1 ELSE 0 END) = 1                    AS vob_own_site,
  -- 2. own site OR a site of the same practice. Nested, so it is never false while
  --    vob_own_site is true — the funnel property the report depends on.
  max(CASE WHEN folder_owns_vob
             OR (vob_group IS NOT NULL AND vob_group = matched_group)
           THEN 1 ELSE 0 END) = 1                                          AS vob_sharing_site,
  -- 3. anywhere at all. Constant true BY CONSTRUCTION: this table is built from
  --    _resolved, which only holds patient-days that matched some VOB somewhere. The
  --    absence of a row is what means "no VOB anywhere", which is why every consumer
  --    LEFT JOINs and treats NULL as false. Kept as a column rather than left implicit
  --    so the three scopes read as one funnel in the output.
  true                                                                     AS vob_enterprise,
  -- ---- the SIM side of the same three scopes -------------------------------
  -- Pam's Summary needs SIM to follow the VOB funnel, not sit beside it: a patient
  -- verified at a sibling and simulated there has converted, and today reads as
  -- neither. Same nesting, same monotonicity, so the two funnels are comparable
  -- stage by stage.
  max(CASE WHEN folder_owns_vob AND has_sim THEN 1 ELSE 0 END) = 1          AS sim_own_site,
  max(CASE WHEN has_sim
             AND (folder_owns_vob
               OR (vob_group IS NOT NULL AND vob_group = matched_group))
           THEN 1 ELSE 0 END) = 1                                          AS sim_sharing_site,
  max(CASE WHEN has_sim THEN 1 ELSE 0 END) = 1                             AS sim_enterprise,

  count(DISTINCT vob_id)                                                   AS n_vobs,
  count(DISTINCT CASE WHEN NOT folder_owns_vob THEN vob_folder END)        AS n_enterprise_folders,
  array_sort(array_distinct(collect_list(
      CASE WHEN NOT folder_owns_vob THEN vob_folder END)))                 AS vob_at_other_folders
FROM r
GROUP BY account_name, patient_id, biopsy_date
""")

_sc = f"{CATALOG}.staged.patient_vob_scope"
print(f"wrote {_sc}: {spark.table(_sc).count():,} patient-days with at least one VOB match")

# The funnel must be monotone. If it is not, the group join is wrong and every scope
# figure below it is unreadable — so this raises rather than prints.
_mono = spark.sql(f"""
SELECT
  sum(CASE WHEN vob_own_site AND NOT vob_sharing_site THEN 1 ELSE 0 END)  AS own_not_sharing,
  sum(CASE WHEN vob_sharing_site AND NOT vob_enterprise THEN 1 ELSE 0 END) AS sharing_not_ent,
  sum(CASE WHEN sim_own_site AND NOT sim_sharing_site THEN 1 ELSE 0 END)  AS sim_own_not_sharing,
  sum(CASE WHEN sim_sharing_site AND NOT sim_enterprise THEN 1 ELSE 0 END) AS sim_sharing_not_ent,
  -- a simulation cannot exist without the VOB that carries it, at every scope
  sum(CASE WHEN sim_own_site     AND NOT vob_own_site     THEN 1 ELSE 0 END) AS sim_without_vob_own,
  sum(CASE WHEN sim_sharing_site AND NOT vob_sharing_site THEN 1 ELSE 0 END) AS sim_without_vob_sharing,
  sum(CASE WHEN sim_enterprise   AND NOT vob_enterprise   THEN 1 ELSE 0 END) AS sim_without_vob_ent
FROM {_sc}
""").first()
_bad = {k: _mono[k] for k in _mono.asDict() if _mono[k]}
assert not _bad, (
    f"the scope funnels are not well formed: {_bad}. Each scope must contain the one "
    "before it, and a SIM at a scope requires a VOB at that same scope. A break in the "
    "first four means _folder_group returned a NULL or mismatched practice group; a "
    "break in the last three means the sim flag is reading a different row than its "
    "VOB. Do not read the scope columns until every one of these is 0.")
print("scope funnels are well formed: own <= sharing <= enterprise, and sim <= vob at every scope")

# The whole point, per account: how many appropriate patient-days gain a VOB as the
# scope widens. Column 1 is what the report counts today; the gap to column 2 is the
# referral-and-sharing question; the gap to column 3 is namesake noise plus genuine
# cross-practice verification, and cannot be separated without an MRN on the feed.
display(spark.sql(f"""
SELECT
  d.account_name,
  count(*)                                                                 AS appropriate_days,
  sum(CASE WHEN s.vob_own_site      THEN 1 ELSE 0 END)                     AS vob_own_site,
  sum(CASE WHEN s.vob_sharing_site  THEN 1 ELSE 0 END)                     AS vob_sharing_site,
  sum(CASE WHEN s.vob_enterprise    THEN 1 ELSE 0 END)                     AS vob_enterprise,
  sum(CASE WHEN s.vob_sharing_site AND NOT s.vob_own_site THEN 1 ELSE 0 END)
                                                                            AS gained_at_a_sibling,
  sum(CASE WHEN s.vob_enterprise AND NOT s.vob_sharing_site THEN 1 ELSE 0 END)
                                                                            AS gained_outside_the_practice,
  round(100.0 * sum(CASE WHEN s.vob_own_site THEN 1 ELSE 0 END) / count(*), 0)
                                                                            AS pct_own_site,
  round(100.0 * sum(CASE WHEN s.vob_sharing_site THEN 1 ELSE 0 END) / count(*), 0)
                                                                            AS pct_sharing_site
FROM {DAYS} d
LEFT JOIN {_sc} s
       ON s.account_name = d.account_name
      AND s.patient_id   = d.patient_id
      AND s.biopsy_date  = d.biopsy_date
WHERE d.appropriate
GROUP BY d.account_name
ORDER BY gained_at_a_sibling DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. §8.3 — the leak counters

# COMMAND ----------

# Practice group, ported from group_alias.py:
# (_grp and raw.group_alias are defined earlier, before _folder_group)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _leak_detail AS
SELECT
  r.account_name                                   AS folder,
  r.vob_id, r.vob_account_id, r.vob_folder, r.gap_days,
  r.patient_name, r.patient_name_raw, r.biopsy_date, r.vob_created, r.appropriate, r.marked,
  r.cell_state,
  CASE
    WHEN r.vob_folder IS NULL                              THEN 'orphan'
    WHEN coalesce(ga.alias, {_grp % 'r.vob_folder'})
       = coalesce(gb.alias, {_grp % 'r.account_name'})     THEN 'shared'
    ELSE                                                        'unlinked'
  END                                              AS leak_kind
FROM _resolved r
LEFT JOIN {_ga} ga ON ga.raw_group = {_grp % 'r.vob_folder'}
LEFT JOIN {_ga} gb ON gb.raw_group = {_grp % 'r.account_name'}
WHERE NOT r.folder_owns_vob
  -- Still-ambiguous namesakes are noise, and refuted ones are worse than noise.
  -- But 'namesake resolved (pathology)' is now admitted: the pathology narrowed it
  -- to one surviving folder, so it is a real leak attribution rather than a
  -- coincidence of names, and it belongs in the leak analysis.
  AND r.outcome NOT IN ('ambiguous namesake', 'namesake refuted (pathology)')
  AND NOT r.matched_own_folder              -- it is counted at its own folder already
""")

# Persist the row-level detail, not just the per-folder counters. See wiki.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.staged.vob_leak_detail USING DELTA AS
SELECT folder            AS biopsy_folder,
       vob_folder        AS vob_owned_by,
       leak_kind, vob_id, vob_account_id, gap_days,
       biopsy_date, vob_created, appropriate, marked, cell_state
FROM _leak_detail
""")
print(f"wrote {CATALOG}.staged.vob_leak_detail: "
      f"{spark.table(f'{CATALOG}.staged.vob_leak_detail').count():,} (vob, folder) leak pairs")

# Direction and gap, per folder pair. This is the query that settles whether a
# marked_share row points the right way: read it as "the VOB belongs to
# `vob_owned_by`, but the patient was biopsied at `biopsy_folder`".
display(spark.sql(f"""
SELECT leak_kind, vob_owned_by, biopsy_folder,
       count(DISTINCT vob_id)              AS vobs,
       round(avg(gap_days), 1)             AS avg_gap,
       percentile_approx(gap_days, 0.5)    AS median_gap,
       min(gap_days)                       AS min_gap,
       max(gap_days)                       AS max_gap
FROM {CATALOG}.staged.vob_leak_detail
GROUP BY leak_kind, vob_owned_by, biopsy_folder
HAVING count(DISTINCT vob_id) > 1
ORDER BY leak_kind, vobs DESC
"""))

# MULTI-ACCOUNT CANDIDATES, CORROBORATED OR CONTRADICTED BY PATHOLOGY. See wiki.
display(spark.sql("""
SELECT outcome,
       sum(CASE WHEN cell_state =  1 THEN 1 ELSE 0 END) AS pathology_agrees,
       sum(CASE WHEN cell_state =  0 THEN 1 ELSE 0 END) AS nothing_comparable,
       sum(CASE WHEN cell_state = -1 THEN 1 ELSE 0 END) AS PATHOLOGY_CONTRADICTS,
       count(*)                                          AS candidate_pairs,
       count(DISTINCT vob_id)                            AS vobs
FROM _resolved
GROUP BY outcome ORDER BY candidate_pairs DESC
"""))
print("Read the 'ambiguous namesake' row. A contradicted candidate is one this")
print("VOB should NOT be attributed to; an agreeing one is the likelier owner.")
print("cell_state = 0 means one side records no cell type -- it costs nothing,")
print("by design, because a missing value must never demote a real match.")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.staged.vob_leak_review USING DELTA AS
WITH own AS (
  SELECT matched_folder AS folder, count(DISTINCT vob_id) AS own_vob
  FROM {CATALOG}.staged.vob_match WHERE matched GROUP BY matched_folder
),
lk AS (
  SELECT folder,
    count(DISTINCT CASE WHEN leak_kind='orphan'   THEN vob_id END) AS leak_orphan,
    count(DISTINCT CASE WHEN leak_kind='shared'   THEN vob_id END) AS leak_shared,
    count(DISTINCT CASE WHEN leak_kind='unlinked' THEN vob_id END) AS leak_unlinked,
    array_sort(array_distinct(collect_list(coalesce(vob_folder, concat('(unmapped) ', vob_account_id))))) AS leak_sources
  FROM _leak_detail GROUP BY folder
)
SELECT
  coalesce(lk.folder, own.folder)          AS folder,
  coalesce(own.own_vob, 0)                 AS own_vob,
  coalesce(lk.leak_orphan, 0)              AS leak_orphan,
  coalesce(lk.leak_shared, 0)              AS leak_shared,
  coalesce(lk.leak_unlinked, 0)            AS leak_unlinked,
  coalesce(lk.leak_sources, array())       AS leak_sources,
  round(coalesce(lk.leak_orphan,0)   / nullif(own.own_vob,0), 4) AS orphan_share,
  round(coalesce(lk.leak_shared,0)   / nullif(own.own_vob,0), 4) AS shared_share,
  round(coalesce(lk.leak_unlinked,0) / nullif(own.own_vob,0), 4) AS unlinked_share,
  -- the gates
  (coalesce(lk.leak_orphan,0) >= {ORPHAN_MIN})                                        AS flag_orphan,
  (coalesce(lk.leak_shared,0) >= {SHARED_MIN}
     AND coalesce(lk.leak_shared,0) >= {SHARED_PCT} * coalesce(own.own_vob,0))        AS flag_shared,
  (coalesce(lk.leak_unlinked,0) >= {SHARED_MIN}
     AND coalesce(lk.leak_unlinked,0) >= {SHARED_PCT} * coalesce(own.own_vob,0))      AS flag_unlinked
FROM lk FULL OUTER JOIN own ON own.folder = lk.folder
""")

display(spark.sql(f"""
SELECT folder, own_vob, leak_orphan, leak_shared, leak_unlinked,
       orphan_share, shared_share, unlinked_share, leak_sources
FROM {CATALOG}.staged.vob_leak_review
WHERE flag_orphan OR flag_shared OR flag_unlinked
ORDER BY leak_orphan DESC, leak_shared DESC, leak_unlinked DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 A flag is a work item, not a warning

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.raw.vob_leak_disposition (
  folder      STRING,
  disposition STRING,   -- consolidate | alias | facility split | accepted | namesake
  decided_on  DATE,
  decided_by  STRING,
  note        STRING
) USING DELTA
""")

open_flags = spark.sql(f"""
SELECT r.folder, r.own_vob, r.leak_orphan, r.leak_shared, r.leak_unlinked, r.leak_sources
FROM {CATALOG}.staged.vob_leak_review r
LEFT JOIN {CATALOG}.raw.vob_leak_disposition d ON d.folder = r.folder
WHERE (r.flag_orphan OR r.flag_shared OR r.flag_unlinked) AND d.folder IS NULL
ORDER BY r.leak_orphan DESC, r.leak_shared DESC
""").collect()

if open_flags:
    print(f"§8.3 QA GATE NOT PASSED — {len(open_flags)} unresolved leak flag(s):\n")
    for r in open_flags:
        bits = []
        if r["leak_orphan"]:
            bits.append(f"orphan {r['leak_orphan']}")
        if r["leak_shared"]:
            bits.append(f"shared {r['leak_shared']}")
        if r["leak_unlinked"]:
            bits.append(f"unlinked {r['leak_unlinked']}")
        print(f"  {r['folder']}  (own VOB {r['own_vob']}) — {', '.join(bits)}")
        print(f"      sources: {r['leak_sources']}")
else:
    print("§8.3 QA gate passed — no unresolved leak flags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Drill into one flag

# COMMAND ----------

dbutils.widgets.text("drill_folder", "", "Drill: folder name (blank = worst flag)")
_drill = dbutils.widgets.get("drill_folder").strip()
if not _drill and open_flags:
    _drill = open_flags[0]["folder"]

if _drill:
    print(f"drilling: {_drill}")
    display(spark.sql(f"""
    SELECT leak_kind, vob_folder, vob_account_id, patient_name, patient_name_raw,
           biopsy_date, vob_created, gap_days, appropriate, marked
    FROM _leak_detail WHERE folder = '{_drill.replace("'", "''")}'
    ORDER BY leak_kind, abs(gap_days)
    """))
    display(spark.sql(f"""
    SELECT leak_kind, count(*) AS vobs,
           round(avg(gap_days), 1)     AS avg_gap,
           percentile_approx(gap_days, 0.5) AS median_gap,
           min(gap_days) AS min_gap, max(gap_days) AS max_gap
    FROM _leak_detail WHERE folder = '{_drill.replace("'", "''")}'
    GROUP BY leak_kind
    """))
else:
    print("no flag to drill")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Reconciliation against the legacy run

# COMMAND ----------

C = "dev_pathology_data"

display(spark.sql(f"""
SELECT outcome, count(DISTINCT vob_id) AS vobs, count(*) AS candidate_rows
FROM {C}.staged.vob_candidates GROUP BY outcome ORDER BY vobs DESC
"""))

display(spark.sql(f"""
WITH v AS (
  SELECT vob_id,
         sum(CASE WHEN outcome = 'namesake refuted (pathology)'  THEN 1 ELSE 0 END) AS n_refuted,
         sum(CASE WHEN outcome = 'namesake resolved (pathology)' THEN 1 ELSE 0 END) AS n_resolved,
         sum(CASE WHEN outcome = 'ambiguous namesake'            THEN 1 ELSE 0 END) AS n_ambig,
         count(*) AS n_cand
  FROM {C}.staged.vob_candidates GROUP BY vob_id
)
SELECT CASE WHEN n_resolved = 1 THEN '1 resolved to one folder'
            WHEN n_resolved > 1 THEN '0 IMPOSSIBLE - two survivors'
            WHEN n_ambig   > 0  THEN '2 still ambiguous'
            WHEN n_refuted > 0  THEN '3 refuted everywhere - counts nowhere'
            ELSE                     '4 not a namesake' END AS disposition,
       count(*) AS vobs, sum(n_cand) AS candidate_rows
FROM v GROUP BY 1 ORDER BY disposition
"""))

# COMMAND ----------

# ---- REBUILD _vob_side, RIGHT HERE ------------------------------------------
# Paste this cell immediately ABOVE section 4 and run it. It is the same
# statement as section 3 of this notebook, so running it when section 3 has
# already run is a no-op (CREATE OR REPLACE), and running it when section 3 has
# not is what lets section 4 proceed.
#
# Needs C, VOBM, W0, W1 from the config cell. If this raises NameError on any of
# them, the config cell has not run and THAT is the real problem.

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


# Prove it exists before moving on.
assert spark.catalog.tableExists("_vob_side"), "_vob_side still not registered"
print("_vob_side is present — section 4 can run.")

# COMMAND ----------

dbutils.widgets.text("prior_master", "", "Prior master .xlsx or .csv (blank = skip)")
_prior = dbutils.widgets.get("prior_master").strip()

# Sheet and column names are RESOLVED, not assumed. This cell used to hardcode
# sheet_name="Pam's Summary" and a column matching "TOTALVOB Matched" -- both are v6.2
# names. The 8.5.26 master calls the sheet `Account Summary` and the column `TOTAL VOB `
# (with a trailing space), so the old cell raised before comparing anything.
_SHEETS = ["Account Summary", "Pam's Summary", "Pams Summary", "Summary"]
_VOBCOL = ["TOTAL VOB", "TOTALVOB Matched", "Total VOB"]
_NMSC   = ["Total NMSC", "Total Appropriate", "N Total Appropriate"]
_MARK   = ["Total Marked", "Total SRT-Marked PT days", "S Total SRT-Marked PT days"]


def _pick(cols, wanted):
    """Exact match on the stripped name first, then a containment fallback."""
    for w in wanted:
        for c in cols:
            if str(c).strip().lower() == w.lower():
                return c
    for w in wanted:
        for c in cols:
            if w.lower() in str(c).strip().lower():
                return c
    return None


if _prior:
    import pandas as pd
    # .xlsx or .csv. The workbook carries band labels on row 1 so its real header is
    # row 2 (header=1); an extracted CSV has its header on row 1 (header=0). Getting
    # that wrong reads the first account as a column name and silently drops it, so it
    # keys off the extension rather than being guessed.
    if _prior.lower().endswith(".csv"):
        pm = pd.read_csv(_prior)
        _sheet = "(csv)"
    else:
        _xl = pd.ExcelFile(_prior)
        _sheet = next((s for s in _SHEETS if s in _xl.sheet_names), None)
        if _sheet is None:
            raise ValueError(
                f"none of {_SHEETS} is a sheet in {_prior}. "
                f"Sheets present: {_xl.sheet_names}")
        pm = pd.read_excel(_prior, sheet_name=_sheet, header=1)
    acc  = _pick(pm.columns, ["Account"]) or pm.columns[0]
    vcol = _pick(pm.columns, _VOBCOL)
    ncol = _pick(pm.columns, _NMSC)
    mcol = _pick(pm.columns, _MARK)
    print(f"prior master: {_prior}")
    print(f"  sheet   : {_sheet!r}  ({len(pm)} rows)")
    print(f"  account : {acc!r}")
    print(f"  VOB     : {vcol!r}\n  NMSC    : {ncol!r}\n  Marked  : {mcol!r}")
    if vcol is None:
        raise ValueError(f"no VOB column found. Columns: {list(pm.columns)}")

    _keep = {acc: "folder", vcol: "prior_vob"}
    if ncol: _keep[ncol] = "prior_nmsc"
    if mcol: _keep[mcol] = "prior_marked"
    _pdf = pm[list(_keep)].rename(columns=_keep)
    _pdf = _pdf[_pdf["folder"].notna()]
    for c in _pdf.columns:
        if c != "folder":
            _pdf[c] = pd.to_numeric(_pdf[c], errors="coerce")
    _pdf = _pdf.rename(columns={"folder": "master_name"})
    spark.createDataFrame(_pdf).createOrReplaceTempView("_prior_raw")

    # RESOLVE THE MASTER'S NAMES TO OUR FOLDER NAMES BEFORE JOINING. See wiki.
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _prior AS
    WITH r AS (
      SELECT p.*,
             (SELECT min(m.folder_name)
                FROM {CATALOG}.raw.account_dim d
                JOIN {AMAP} m ON m.account_id = d.account_id
               WHERE d.account_name = p.master_name)          AS mapped_folder
      FROM _prior_raw p
    )
    SELECT coalesce(mapped_folder, master_name)               AS folder,
           (mapped_folder IS NOT NULL)                        AS name_resolved,
           concat_ws(' + ', array_sort(collect_list(master_name))) AS master_names,
           sum(prior_vob)                                     AS prior_vob,
           sum(prior_nmsc)                                    AS prior_nmsc,
           sum(prior_marked)                                  AS prior_marked
    FROM r
    GROUP BY coalesce(mapped_folder, master_name), (mapped_folder IS NOT NULL)
    """)
    _res = spark.sql("SELECT name_resolved, count(*) n FROM _prior GROUP BY 1").collect()
    for r in _res:
        print(f"  master names {'resolved to a folder' if r['name_resolved'] else 'NOT resolved'}: {r['n']}")

    # WHICH BASIS. The master's VOB column is every DISTINCT VOB on the account,. See wiki.
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _new_vob AS
    SELECT vob_folder                                        AS folder,
           count(DISTINCT vob_id)                            AS new_vob_all_distinct,
           count(DISTINCT CASE WHEN matched THEN vob_id END)  AS new_vob_matched_own
    FROM {CATALOG}.staged.vob_match
    WHERE vob_folder IS NOT NULL
      AND vob_created >= '{W0}' AND vob_created < '{W1}'
    GROUP BY vob_folder
    """)
    print(f"comparing VOBs created {W0} .. {W1} — the master's basis, not the whole pull")

    # A name that never resolved is a DIFFERENT finding from a count that fell, and
    # mixing them buries the real one. Split explicitly.
    display(spark.sql(f"""
      SELECT p.folder, p.master_names, p.prior_vob,
             coalesce(v.new_vob_all_distinct, 0)                    AS new_vob_in_window,
             coalesce(v.new_vob_all_distinct, 0) - p.prior_vob      AS delta_same_basis,
             coalesce(v.new_vob_matched_own, 0)                     AS new_vob_matched_own,
             r.leak_orphan, r.leak_shared, r.leak_unlinked,
             CASE
               WHEN NOT p.name_resolved AND v.folder IS NULL
                 THEN '1 NO SUCH FOLDER — this account is not in the run at all'
               WHEN coalesce(v.new_vob_all_distinct, 0) < p.prior_vob
                 THEN '2 *** DECREASE — investigate'
               WHEN coalesce(v.new_vob_all_distinct, 0) > p.prior_vob
                 THEN '3 increase (expect where a folder pools a 2nd AccountId)'
               ELSE '4 unchanged' END                               AS verdict
      FROM _prior p
      LEFT JOIN _new_vob v ON v.folder = p.folder
      LEFT JOIN {CATALOG}.staged.vob_leak_review r ON r.folder = p.folder
      WHERE coalesce(v.new_vob_all_distinct, 0) <> p.prior_vob
      ORDER BY verdict, abs(coalesce(v.new_vob_all_distinct, 0) - p.prior_vob) DESC
    """))

    # The portfolio ratio is the tell. A uniform multiple across every account is a
    # BASIS difference; a real regression hits some accounts and not others.
    _tot = spark.sql(f"""
      SELECT sum(p.prior_vob)                          AS prior_total,
             sum(coalesce(v.new_vob_all_distinct, 0))   AS new_total,
             count(*)                                   AS accounts,
             sum(CASE WHEN coalesce(v.new_vob_all_distinct,0) > p.prior_vob
                      THEN 1 ELSE 0 END)                AS increased,
             sum(CASE WHEN coalesce(v.new_vob_all_distinct,0) < p.prior_vob
                      THEN 1 ELSE 0 END)                AS decreased
      FROM _prior p LEFT JOIN _new_vob v ON v.folder = p.folder
      WHERE p.name_resolved
    """).first()
    _ratio = (_tot["new_total"] / _tot["prior_total"]) if _tot["prior_total"] else 0
    print(f"\nresolved accounts: {_tot['accounts']}  "
          f"prior {_tot['prior_total']:,} -> new {_tot['new_total']:,}  ({_ratio:.2f}x)")
    print(f"  increased {_tot['increased']}  decreased {_tot['decreased']}")
    if _tot["accounts"] and _tot["increased"] >= 0.95 * _tot["accounts"]:
        print("*** nearly EVERY account moved the same way. That is a basis difference,")
        print("    not a regression — check the window, the matched-vs-distinct choice,")
        print("    and which column of the master is being read, before reading any row.")

    # NMSC and Marked matter MORE than the VOB count and the old cell never compared
    # them. A classifier or patient-key regression shows up here first, and it is the
    # comparison QA item 7 actually asks for.
    if ncol and mcol and spark.catalog.tableExists(f"{CATALOG}.staged.patient_days"):
        display(spark.sql(f"""
        WITH new AS (
          SELECT account_name AS folder,
                 sum(CASE WHEN appropriate THEN 1 ELSE 0 END) AS new_nmsc,
                 sum(CASE WHEN marked      THEN 1 ELSE 0 END) AS new_marked
          FROM {DAYS} GROUP BY account_name
        )
        SELECT p.folder, p.prior_nmsc, n.new_nmsc,
               n.new_nmsc - p.prior_nmsc                                   AS d_nmsc,
               round((n.new_nmsc - p.prior_nmsc) / nullif(p.prior_nmsc,0), 3) AS d_nmsc_pct,
               p.prior_marked, n.new_marked,
               n.new_marked - p.prior_marked                               AS d_marked
        FROM _prior p LEFT JOIN new n ON n.folder = p.folder
        WHERE abs(coalesce(n.new_nmsc,0) - coalesce(p.prior_nmsc,0)) > 0
           OR abs(coalesce(n.new_marked,0) - coalesce(p.prior_marked,0)) > 0
        ORDER BY abs(coalesce(n.new_nmsc,0) - coalesce(p.prior_nmsc,0)) DESC
        """))
        print("Expected movements this run: Allentown NMSC UP sharply (the 'MRN' patient-key")
        print("fix), portfolio appropriate DOWN ~6 (the bowen(?!oid) narrowing), FlagsReview")
        print("657 -> 187. Anything else needs an explanation before the workbook ships.")
else:
    print("no prior master supplied — reconciliation skipped (QA item 7 not satisfied)")
    print()
    print("To satisfy it: put the prior master somewhere the cluster can read and set the")
    print("prior_master widget to that path, e.g.")
    print("  /Volumes/dev_pathology_data/raw/reference/Pathology Data w. KPIs 8.5.26.xlsx")
    print("A workspace path works too (/Workspace/...). This cell resolves the sheet and")
    print("column names itself, so the 8.5.26 layout and the older v6.2 one both work.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next