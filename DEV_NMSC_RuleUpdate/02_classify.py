# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — Notebook 2: `classify`

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")

CATALOG = dbutils.widgets.get("catalog")
VIEW = f"{CATALOG}.staged.lesion_classified"   # built by Notebook 1, shared definition

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.staged")
print("view:", VIEW)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The vocabulary — pulled in, not redeclared

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rebuild the silver view, then report on it

# COMMAND ----------

spark.sql(classified_sql(f"{CATALOG}.raw.path_log_rows", VIEW))
print("rebuilt", VIEW)

n = spark.table(VIEW).count()
b = spark.table(f"{CATALOG}.raw.path_log_rows").count()
print(f"{VIEW}: {n:,} rows")
print(f"bronze: {b:,} rows")
assert n == b, (
    f"{VIEW} has {n:,} rows against {b:,} in bronze — the silver view must be 1:1. "
    "A filter or a fan-out here invalidates every number downstream."
)
print("1:1 with bronze — OK")

display(spark.sql(f"DESCRIBE {VIEW}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2a. Benign-sweep neutrality gate — [21 Aug]

# COMMAND ----------

_res = norm_sql("b.pathology")
_with_sweep = path_class_sql(_res)

# Same function, same source rows, BENIGN temporarily emptied. Mutated in place and
# restored in a finally, because path_class_sql closes over the module-level list —
# rebinding a local name here would not reach it.
_saved = list(BENIGN)
try:
    BENIGN.clear()
    _no_sweep = path_class_sql(_res)
finally:
    BENIGN.clear()
    BENIGN.extend(_saved)
assert len(BENIGN) == len(_saved) and _no_sweep != _with_sweep, "BENIGN was not restored"

_srt = f"{search_text_sql('b')} RLIKE '{sql_rx(SRT_BROAD)}'"
# [18 Sep] RULE D is in here too. The gate compares sweep-on against sweep-off and
# both sides use this one expression, so omitting it would still be self-consistent
# — but `appropriate_rows` below would then not match the production view, which is
# the number a reader checks it against.
_sup = (f"({rlike_any(_res, CLEARED_RESULT)} "
        f"OR {rlike_any(norm_sql('b.actions'), CLEARED_PLAN)} "
        f"OR {cleared_topical_sql(norm_sql('b.actions'), norm_sql('b.comments'))} "
        f"OR {rlike_any(norm_sql('b.actions'), PLAN_REMOVED)})")

_gate = spark.sql(f"""
WITH r AS (
  SELECT {_with_sweep} AS pc_new,
         {_no_sweep}   AS pc_old,
         {_srt}        AS marked,
         {_sup}        AS suppressed
  FROM {CATALOG}.raw.path_log_rows b
),
a AS (
  SELECT *,
         ((pc_new = 'acceptable' AND NOT suppressed) OR marked) AS ap_new,
         ((pc_old = 'acceptable' AND NOT suppressed) OR marked) AS ap_old
  FROM r
)
SELECT count(*)                                                    AS rows_total,
       sum(CASE WHEN ap_new <> ap_old            THEN 1 ELSE 0 END) AS appropriate_flips,
       sum(CASE WHEN pc_new = 'acceptable'       THEN 1 ELSE 0 END) AS acceptable_new,
       sum(CASE WHEN pc_old = 'acceptable'       THEN 1 ELSE 0 END) AS acceptable_old,
       sum(CASE WHEN ap_new                      THEN 1 ELSE 0 END) AS appropriate_rows,
       sum(CASE WHEN pc_old = 'unknown'          THEN 1 ELSE 0 END) AS review_before,
       sum(CASE WHEN pc_new = 'unknown'          THEN 1 ELSE 0 END) AS review_after,
       sum(CASE WHEN pc_old = 'unknown' AND pc_new = 'excluded'
                                                 THEN 1 ELSE 0 END) AS swept,
       sum(CASE WHEN pc_old = 'unknown' AND pc_new = 'excluded' AND marked
                                                 THEN 1 ELSE 0 END) AS swept_but_marked
FROM a
""").first()

for k in _gate.asDict():
    print(f"  {k:<18} {_gate[k]:>9,}")

assert _gate["appropriate_flips"] == 0, (
    f"benign sweep changed lesion_appropriate on {_gate['appropriate_flips']:,} rows. "
    "It is only permitted to move rows that were heading for UNKNOWN, and UNKNOWN and "
    "EXCLUDED are equally non-acceptable — so this means a BENIGN term is now stealing "
    "rows ACCEPTABLE used to claim, or the sweep's branch has moved above ACCEPTABLE in "
    "path_class_sql. Do not trust any downstream count until this reads 0."
)
assert _gate["acceptable_new"] == _gate["acceptable_old"], (
    f"acceptable row count moved: {_gate['acceptable_old']:,} -> {_gate['acceptable_new']:,}"
)
assert _gate["review_after"] <= _gate["review_before"], "the sweep grew the review list"
print(f"\nneutral — 0 appropriate flips; FlagsReview {_gate['review_before']:,} -> "
      f"{_gate['review_after']:,} rows ({_gate['swept']:,} swept, "
      f"{_gate['swept_but_marked']:,} of them SRT-marked and therefore still counting)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. What it did — per account

# COMMAND ----------

display(spark.sql(f"""
SELECT
  account_name,
  count(*)                                                     AS lesions,
  sum(CASE WHEN path_class = 'acceptable' THEN 1 ELSE 0 END)   AS acceptable,
  sum(CASE WHEN path_class = 'excluded'   THEN 1 ELSE 0 END)   AS excluded,
  sum(CASE WHEN path_class = 'unknown'    THEN 1 ELSE 0 END)   AS unknown_path,
  sum(CASE WHEN is_cleared      THEN 1 ELSE 0 END)             AS cleared,
  sum(CASE WHEN is_cleared_topical THEN 1 ELSE 0 END)          AS cleared_topical,
  sum(CASE WHEN is_plan_removed THEN 1 ELSE 0 END)             AS plan_removed,
  sum(CASE WHEN lesion_appropriate THEN 1 ELSE 0 END)          AS appropriate,
  sum(CASE WHEN srt_marked      THEN 1 ELSE 0 END)             AS marked_broad,
  sum(CASE WHEN srt_marked_strict THEN 1 ELSE 0 END)           AS marked_strict,
  sum(CASE WHEN vob_mentioned   THEN 1 ELSE 0 END)             AS vob_mentioned,
  round(100.0 * avg(CASE WHEN has_actions THEN 1 ELSE 0 END), 0) AS actions_cov_pct
FROM {VIEW}
GROUP BY account_name
ORDER BY account_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Broad vs strict — the rows where the choice matters

# COMMAND ----------

display(spark.sql(f"""
SELECT account_name, patient_name, collected_date,
       substr(pathology, 1, 60)  AS pathology,
       substr(actions,   1, 80)  AS actions,
       substr(notifications, 1, 60) AS notifications
FROM {VIEW}
WHERE srt_marked AND NOT srt_marked_strict
ORDER BY account_name, patient_name
LIMIT 200
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. FlagsReview — sheet 6 of the output spec

# COMMAND ----------

display(spark.sql(f"""
SELECT
  substr(regexp_replace(trim(pathology), '\\\\s+', ' '), 1, 120) AS unknown_pathology,
  count(*)                        AS n,
  count(DISTINCT account_name)    AS accounts,
  min(account_name)               AS example_account
FROM {VIEW}
WHERE path_class = 'unknown' AND trim(coalesce(pathology, '')) <> ''
GROUP BY 1
ORDER BY n DESC
LIMIT 300
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Regression gate

# COMMAND ----------


CASES = [
    # (result, actions, expected path_class, expected srt_marked, note)
    ("Basal cell carcinoma, nodular type", "",         "acceptable", False, "plain BCC"),
    ("BCC, biopsy site changes",           "",         "acceptable", False, "acceptable beats excluded"),
    ("asal cell carcinoma",                "",         "acceptable", False, "OCR lost the B"),
    ("Dermatofibrosarcom a Protuberans",   "",         "excluded",   False,
     "OCR split the word; the stem pattern must still exclude it (21 Aug)"),
    ("Dermatofibrosarcoma Protuberans",    "",         "excluded",   False, "intact spelling"),
    ("Carcimona, basal cell",              "",         "acceptable", False, "recurring misspelling"),
    ("Squamous cell carcinoma in situ",    "",         "acceptable", False, "squam stem"),
    ("Keratoacanthoma",                    "",         "acceptable", False, "keratoa stem"),
    ("SSC-is, left cheek",                 "",         "acceptable", False, "transposed SCC"),
    ("Malignant melanoma",                 "",         "excluded",   False, "not NMSC"),
    ("Seborrheic keratosis",               "",         "excluded",   False, "benign"),
    ("History of cutaneous neoplasm",      "",         "excluded",   False, "history-of guard"),
    ("Compound nevus",                     "",         "excluded",   False, "nevus"),
    ("",                                   "",         "unknown",    False, "empty result"),
    # ---- benign sweep, 21 Aug. The sweep runs LAST and is vetoed by BENIGN_BLOCK.
    # Its whole safety property is that it can only move rows that were heading for
    # 'unknown', so these cases test the ordering as much as the terms.
    ("Fibrous papule",                     "",         "excluded",   False,
     "swept - was a vocabulary gap on FlagsReview until 21 Aug"),
    ("Epidermal inclusion cyst",           "",         "excluded",   False, "swept"),
    ("Chronic spongiotic / eczematous dermatitis", "", "excluded",   False, "swept"),
    ("Scarring alopecia",                  "",         "excluded",   False, "swept"),
    ("Prurigo nodularis",                  "",         "excluded",   False,
     "swept - and must NOT reach the carcinoma-nodular pattern in ACCEPTABLE"),
    ("Hypertrophic actinic keratosis",     "",         "excluded",   False,
     "AK is a precursor, never NMSC on its own"),
    ("Hypertrophic actinic keratosis with focus concerning for superficially invasive"
     " well- differentiat",                "",         "unknown",    False,
     "THE GUARD CASE: a real truncated row. Benign token present, but 'invasive' and "
     "'concerning' veto the sweep so it stays visible on FlagsReview"),
    ("Atypical keratinocytic proliferation", "",       "unknown",    False,
     "guard: any atypia stays for human review, never swept"),
    ("Porokeratosis",                      "",         "excluded",   False,
     "benign - and its stem must NOT reach porocarcinoma, the case below"),
    ("Porocarcinoma",                      "",         "excluded",   False,
     "malignant adnexal, EXCLUDED by its own rule. This case expected 'unknown' for "
     "an hour on 21 Aug: it was written while porocarcinoma was still a vocabulary "
     "gap, and the non-NMSC terms added later that day claimed it. The stale "
     "expectation is what the regression is for -- both terms are kept because the "
     "point of the pair is that `porokeratosis` and `porocarcinoma` land in the same "
     "bucket for OPPOSITE reasons, and a future edit must not merge them"),
    ("Dermatofibroma",                     "",         "excluded",   False,
     "swept - and must stay distinct from the DFSP stem two cases up"),
    # ---- bowen lookahead, 21 Aug
    ("Bowen's disease",                    "",         "acceptable", False,
     "SCC in situ under another name"),
    ("Squamous cell carcinoma in situ, bowenoid type", "", "acceptable", False,
     "genuine SCCis - rescued by `squam`, not by the bowen term"),
    ("Bowenoid papulosis",                 "",         "excluded",   False,
     "HPV anogenital, not the NMSC this programme treats - the (?!oid) lookahead"),
    # ---- malignant but not NMSC, 21 Aug
    ("Eccrine porocarcinoma",              "",         "excluded",   False,
     "adnexal carcinoma - and the benign `porokeratosis` term must not reach it"),
    ("Atypical lymphoid proliferation",    "",         "excluded",   False,
     "lymphoid - excluded by rule now, not left as a vocabulary gap"),
    ("Langerhans cell histiocytosis",      "",         "excluded",   False,
     "histiocytos stem; must NOT collide with `fibrous histiocytoma` in BENIGN"),
    ("Cellular dermatofibroma (cellular fibrous histiocytoma)", "", "excluded", False,
     "the benign synonym - swept, and NOT caught by the histiocytos stem above"),
    ("Fibroepithelioma of Pinkus",         "",         "unknown",    False,
     "a BCC variant the rules do not claim. Left UNKNOWN on purpose (Becky, 21 Aug) "
     "- rescuing it would move a count, so it stays a business decision"),
    # ---- count-neutrality of the sweep: swept, but SRT-marked, so it still counts
    ("Actinic keratosis", "Refer: Superficial Radiation Therapy", "excluded", True,
     "swept to excluded YET lesion_appropriate via OR srt_marked - this is why the "
     "sweep cannot change a count"),
    # SRT marking
    ("Melanoma",              "Discussed IG-SRT",      "excluded",   True,  "SRT overrides pathology"),
    ("BCC",                   "Patient declined SRT",  "acceptable", True,  "NO negation guard - broad rule"),
    ("BCC",                   "Schedule: Brachytherapy", "acceptable", True, "brachy signals SRT"),
    ("BCC",                   "mohs vs sgRT",          "acceptable", True,  "SGRT where SRT is meant"),
    ("BCC",                   "Gentle Cure consult",   "acceptable", True,  "brand written open"),
    ("BCC",                   "eBx scheduled",         "acceptable", True,  "electronic brachytherapy"),
    ("BCC",                   "Exc with rt thigh",     "acceptable", False, "bare rt is anatomical - REJECTED"),
    ("BCC",                   "Excision with Mohs",    "acceptable", False, "no SRT vocabulary at all"),
    # cleared / removed
    ("No residual basal cell carcinoma", "",           "acceptable", False, "cleared - not a new candidate"),
    ("No Residual",           "",                      "unknown",    False, "bare re-excision: unknown, and cleared"),
    ("Margins free of tumor", "",                      "unknown",    False, "cleared, no tumour named"),
    ("BCC",                   "Reassure: benign.",     "acceptable", False, "plan removed"),
]

# expected (is_cleared, is_plan_removed, lesion_appropriate) for the rows where. See wiki.
CLEARED_EXPECT = {
    ("No residual basal cell carcinoma", ""):                  (True, False, False),
    ("No Residual",                      ""):                  (True, False, False),
    ("Margins free of tumor",            ""):                  (True, False, False),
    ("BCC",                              "Reassure: benign."): (True, True,  False),
    # The benign sweep's neutrality, asserted rather than asserted-about: this row
    # is 'excluded' by pathology and still appropriate, because the SRT mark carries
    # it. If a future edit ever made path_class gate the whole of lesion_appropriate,
    # this is the case that would fail.
    ("Actinic keratosis", "Refer: Superficial Radiation Therapy"): (False, False, True),
}

import pandas as pd
probe = spark.createDataFrame(pd.DataFrame(
    [{"file_hash": f"case{i}", "path": "", "account_name": "__REGRESSION__",
      "file_name": "", "is_scanned": False, "patient_name": f"Case {i}", "dob": "",
      "accession": "", "collected_date": "2026-01-01", "result_date": "",
      "facility": "", "pathology": r, "body_site": "", "actions": a,
      "notifications": "", "notes": "", "comments": "", "flags": "",
      "provider": "", "page_no": 1, "layout": "TEST", "run_id": "regression"}
     for i, (r, a, _pc, _srt, _n) in enumerate(CASES)]))
probe.createOrReplaceTempView("_regression_rows")

# The SAME definition the production view is built from, pointed at the probe rows.
# classified_sql() comes from 00_vocab via %run. A regression that
# tested a *copy* of the logic would test nothing.
spark.sql(classified_sql("_regression_rows", "_regression_out", temp=True))

got = {r["patient_name"]: r for r in spark.table("_regression_out").collect()}

fails = []
checked = 0
for i, (r, a, want_pc, want_srt, note) in enumerate(CASES):
    g = got[f"Case {i}"]
    checked += 3                      # path_class, is_nmsc_type, srt_marked

    if g["path_class"] != want_pc:
        fails.append(f"path_class {g['path_class']!r} != {want_pc!r}  |  {r!r} / {a!r}  ({note})")

    # is_nmsc_type IS path_class == 'acceptable' and must never drift from it.
    # It answers "is this a lesion type SRT treats" — NOT "does this lesion count".
    # lesion_appropriate is the one that counts; see CLEARED_EXPECT below.
    if bool(g["is_nmsc_type"]) != (want_pc == "acceptable"):
        fails.append(f"is_nmsc_type {bool(g['is_nmsc_type'])} "
                     f"!= {want_pc == 'acceptable'}  |  {r!r} / {a!r}  ({note})")

    if bool(g["srt_marked"]) != want_srt:
        fails.append(f"srt_marked {bool(g['srt_marked'])} != {want_srt}  |  {r!r} / {a!r}  ({note})")

    if (r, a) in CLEARED_EXPECT:
        w_cl, w_rm, w_ap = CLEARED_EXPECT[(r, a)]
        checked += 3
        for col, want in (("is_cleared", w_cl), ("is_plan_removed", w_rm),
                          ("lesion_appropriate", w_ap)):
            if bool(g[col]) != want:
                fails.append(f"{col} {bool(g[col])} != {want}  |  {r!r} / {a!r}  ({note})")

_keys = {(r, a) for r, a, *_ in CASES}
_orphan = sorted(set(CLEARED_EXPECT) - _keys)
if _orphan:
    raise AssertionError(
        f"CLEARED_EXPECT keys with no matching case: {_orphan}. "
        f"A key that matches nothing asserts nothing.")

if fails:
    print(f"REGRESSION FAILED — {len(fails)} of {checked} assertions\n")
    for f in fails:
        print("  ", f)
    raise AssertionError(f"{len(fails)} classifier regressions — do not trust downstream numbers")
print(f"regression OK — {len(CASES)} cases, {checked} assertions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next