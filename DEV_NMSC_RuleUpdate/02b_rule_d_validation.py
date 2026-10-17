# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — Notebook 2b: `rule_d_validation`
# MAGIC
# MAGIC Run **after** `02_classify`, **before** `03_patient_days`. Read-only — it creates
# MAGIC nothing outside temp views and changes no table.
# MAGIC
# MAGIC RULE D (18 Sep 2026) suppresses a lesion whose Actions or Comments say the margins
# MAGIC came back clear **and** name a topical agent. Vocabulary and helper live in
# MAGIC `00_vocab`; this notebook proves the rule reached the column that counts.
# MAGIC
# MAGIC | Gate | Asserts |
# MAGIC |---|---|
# MAGIC | 1 | the rule fires at all — a silent no-op is the failure that reads as success |
# MAGIC | 2 | it only ever suppresses, never admits |
# MAGIC | 3 | no SRT-marked lesion is suppressed — it sits inside `OR srt_marked` |
# MAGIC | 4 | the movement stays inside the measured envelope |
# MAGIC
# MAGIC Gate 4 is the one that matters six months from now: it catches someone relaxing
# MAGIC the conjunction in `cleared_topical_sql` to an `OR`, which would quietly delete
# MAGIC the ~5,900 lesions being field-treated with no clearance language. Those are
# MAGIC IG-SRT candidates, not exclusions.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")

CATALOG = dbutils.widgets.get("catalog")
VIEW    = f"{CATALOG}.staged.lesion_classified"
BRONZE  = f"{CATALOG}.raw.path_log_rows"

# Measured 18 Sep 2026 on 161,215 lesion rows. An envelope, not an equality — the
# population grows every month. Widen TOL rather than deleting the gate.
BASE_RULE_D_ROWS = 25
TOL = 3.0

print("validating RULE D against", VIEW)

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate 1 · the rule fires
# MAGIC
# MAGIC Zero is the dangerous answer, not a wrong number. A mis-escaped backslash, a
# MAGIC view that was never recreated, or an empty `comments` column all read as
# MAGIC "no cases found". **A view definition is frozen at CREATE time** — editing
# MAGIC `00_vocab` does not reach `lesion_classified` until `02_classify` recreates it.

# COMMAND ----------

g1 = spark.sql(f"""
  SELECT count(*)                                                        AS lesion_rows,
         sum(CASE WHEN is_cleared_topical THEN 1 ELSE 0 END)             AS rule_d_rows,
         sum(CASE WHEN is_cleared_topical AND srt_marked THEN 1 ELSE 0 END)
                                                                         AS rule_d_srt_marked,
         sum(CASE WHEN trim(coalesce(comments, '')) <> '' THEN 1 ELSE 0 END)
                                                                         AS rows_with_comments
  FROM {VIEW}
""").first()
for k in g1.asDict():
    print(f"  {k:<20} {g1[k]:>9,}")

assert g1["rows_with_comments"] > 0, (
    f"`comments` is empty on every row of {VIEW}. RULE D reads that field, so it "
    "cannot fire. Check the extract — this is an ingest problem, not a rule problem.")
assert g1["rule_d_rows"] > 0, (
    "RULE D matched 0 rows. In order of cost to check: (1) did 02_classify run after "
    "the vocab edit — a view definition is frozen at CREATE time; (2) does the vocab's "
    "RULE D self-check still pass on this cluster; (3) has the pathology wording "
    "genuinely changed. 25 rows was the 18 Sep baseline.")
print("gate 1 OK — the rule fires")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate 2 · it only suppresses
# MAGIC
# MAGIC Rebuilds `lesion_appropriate` **without** RULE D from the same bronze rows and
# MAGIC diffs against the version with it. Same technique as the benign-sweep neutrality
# MAGIC gate in `02_classify`: compare against a re-derivation, never against a
# MAGIC remembered number.
# MAGIC
# MAGIC The re-derivation mirrors `classified_sql` exactly, melanoma guard included — if
# MAGIC `lesion_appropriate` gains another clause in `00_vocab`, it has to be added here
# MAGIC too or this gate quietly stops testing the real thing.

# COMMAND ----------

_res  = norm_sql("b.pathology")
_act  = norm_sql("b.actions")
_com  = norm_sql("b.comments")
_srch = search_text_sql("b")

_cleared_old = f"({rlike_any(_res, CLEARED_RESULT)} OR {rlike_any(_act, CLEARED_PLAN)})"
_cleared_new = f"({_cleared_old} OR {cleared_topical_sql(_act, _com)})"
_removed     = rlike_any(_act, PLAN_REMOVED)
_never       = (f"({rlike_any(_res, NEVER_APPROPRIATE)} "
                f"AND NOT {rlike_any(_res, NOT_MELANOMA)})")
_srt         = f"{_srch} RLIKE '{sql_rx(SRT_BROAD)}'"
_pc          = path_class_sql(_res)

def _appropriate(cleared):
    return (f"(((({_pc}) = 'acceptable' AND NOT {cleared} AND NOT {_removed}) "
            f"OR {_srt}) AND NOT {_never})")

g2 = spark.sql(f"""
  WITH r AS (
    SELECT {_appropriate(_cleared_old)}            AS ap_old,
           {_appropriate(_cleared_new)}            AS ap_new,
           {_srt}                                  AS marked,
           {cleared_topical_sql(_act, _com)}       AS rule_d
    FROM {BRONZE} b
  )
  SELECT count(*)                                                   AS rows_total,
         sum(CASE WHEN ap_old THEN 1 ELSE 0 END)                    AS appropriate_before,
         sum(CASE WHEN ap_new THEN 1 ELSE 0 END)                    AS appropriate_after,
         sum(CASE WHEN ap_old AND NOT ap_new THEN 1 ELSE 0 END)     AS dropped,
         sum(CASE WHEN ap_new AND NOT ap_old THEN 1 ELSE 0 END)     AS gained,
         sum(CASE WHEN ap_old AND NOT ap_new AND marked THEN 1 ELSE 0 END)
                                                                    AS dropped_but_marked,
         sum(CASE WHEN rule_d THEN 1 ELSE 0 END)                    AS rule_d_rows
  FROM r
""").first()
for k in g2.asDict():
    print(f"  {k:<20} {g2[k]:>9,}")

assert g2["gained"] == 0, (
    f"RULE D ADDED {g2['gained']:,} appropriate lesions. It is a suppression rule and "
    "can only ever remove. This means it was wired somewhere other than is_cleared, "
    "or a NOT was lost.")
print("gate 2 OK — suppression only")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate 3 · no marked lesion is suppressed
# MAGIC
# MAGIC Rule A's call, kept: an explicit SRT mark beats a boilerplate reassurance. If this
# MAGIC fails, RULE D was placed outside the `OR srt_marked` — that is Rule B's placement
# MAGIC in `03_patient_days`, and a different decision than the one taken here.

# COMMAND ----------

assert g2["dropped_but_marked"] == 0, (
    f"{g2['dropped_but_marked']:,} SRT-marked lesions were suppressed by RULE D. The "
    "rule belongs INSIDE `OR srt_marked` — fold it into is_cleared, not into a "
    "separate AND NOT beside lesion_appropriate.")
print(f"gate 3 OK — {g1['rule_d_srt_marked']:,} marked hit(s) survive the rule")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate 4 · the movement is the size it was measured at

# COMMAND ----------

_ceiling = int(BASE_RULE_D_ROWS * TOL)
assert g2["rule_d_rows"] <= _ceiling, (
    f"RULE D now matches {g2['rule_d_rows']:,} rows against a baseline of "
    f"{BASE_RULE_D_ROWS} ({TOL:g}x ceiling = {_ceiling}). Before raising TOL, confirm the "
    "conjunction in cleared_topical_sql is still AND. As an OR it matches the ~5,900 "
    "lesions being field-treated with no clearance language — those are IG-SRT "
    "candidates and must stay in Appropriate.")
print(f"gate 4 OK — {g2['rule_d_rows']:,} rows, ceiling {_ceiling}")

print(f"\nnet: appropriate lesions {g2['appropriate_before']:,} -> "
      f"{g2['appropriate_after']:,}  ({g2['dropped']:,} suppressed, "
      f"{100.0 * g2['dropped'] / max(g2['appropriate_before'], 1):.3f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The topical pool — the number worth more than the rule
# MAGIC
# MAGIC Appropriate lesions naming a topical agent **without** clearance language. Measured
# MAGIC at **5,893** on 18 Sep — 4.6% of appropriate lesions, 367× RULE D itself. These are
# MAGIC superficial BCC and SCCis being field-treated *instead of* definitively: the IG-SRT
# MAGIC opportunity, not an exclusion.
# MAGIC
# MAGIC The marked rate against the portfolio's 31.4% is the question. Well below it means
# MAGIC these patients are being field-treated without SRT ever being raised — the marking
# MAGIC bottleneck in its most addressable form.

# COMMAND ----------

# built against the view's alias, not bronze's — no string surgery on _act/_com
_l_act, _l_com = norm_sql("l.actions"), norm_sql("l.comments")
_topical = (f"({_l_act} RLIKE '{sql_rx(TOPICAL_AGENT)}' "
            f"OR {_l_com} RLIKE '{sql_rx(TOPICAL_AGENT)}') "
            f"AND NOT {cleared_topical_sql(_l_act, _l_com)}")

display(spark.sql(f"""
  SELECT CASE WHEN {_topical} THEN 'topical, no clearance'
              ELSE 'every other appropriate lesion' END              AS grp,
         count(*)                                                    AS lesion_rows,
         round(100.0 * avg(CASE WHEN srt_marked THEN 1 ELSE 0 END), 1) AS pct_srt_marked,
         round(100.0 * avg(CASE WHEN vob_mentioned THEN 1 ELSE 0 END), 1) AS pct_vob_mentioned
  FROM {VIEW} l
  WHERE lesion_appropriate
  GROUP BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The hits, verbatim — read them
# MAGIC
# MAGIC At 25 rows the eyes beat the statistics. The measured 6.3% marked rate is 1 row of
# MAGIC 16; its 95% interval runs 1.1%–28.3% and overlaps the 31.4% baseline, so it is
# MAGIC suggestive rather than decisive. What you are looking for is the history trap —
# MAGIC *"margins clear on prior bx, imiquimod trialled 2024"* beside a live plan. Rule A
# MAGIC needed a `^reassur` anchor for exactly that; if these show the same shape, RULE D
# MAGIC needs one too.
# MAGIC
# MAGIC **PHI — keep the results in Databricks.**

# COMMAND ----------

display(spark.sql(f"""
  SELECT account_name, pathology, actions, comments, srt_marked, lesion_appropriate
  FROM {VIEW}
  WHERE is_cleared_topical
  ORDER BY account_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where it lands, by account

# COMMAND ----------

display(spark.sql(f"""
  SELECT account_name,
         count(*)                                              AS rule_d_rows,
         sum(CASE WHEN lesion_appropriate THEN 1 ELSE 0 END)   AS still_appropriate
  FROM {VIEW}
  WHERE is_cleared_topical
  GROUP BY account_name
  ORDER BY rule_d_rows DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patient-days
# MAGIC
# MAGIC Deliberately not computed here. `patient_id` is derived in `03_patient_days`, not
# MAGIC carried on `lesion_classified`, and re-deriving it would be a second copy of the
# MAGIC MRN-junk rule — the exact thing `00_vocab` exists to prevent.
# MAGIC
# MAGIC Suppression is per lesion, so a day keeps its other lesions; only a day where
# MAGIC *every* appropriate lesion is a RULE D hit falls out. Expect the patient-day drop
# MAGIC to be at or below the lesion drop above. Run `03_patient_days` and compare its
# MAGIC appropriate-day count against the prior run.