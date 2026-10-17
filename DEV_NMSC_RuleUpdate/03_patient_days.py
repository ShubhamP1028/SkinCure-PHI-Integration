# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — Notebook 3: `patient_days`

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------

# The vocabulary is loaded so the VOB-mention SNIPPET below extracts on exactly the
# pattern that set the flag. Re-typing the regex here would let the flag and the
# evidence for it drift apart, which is the worst possible failure for a review
# sheet: text that does not contain the thing it is offered as proof of.

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("window_start", "2026-06-01", "Window start (inclusive)")
dbutils.widgets.text("window_end", "2026-10-01", "Window end (exclusive)")

CATALOG = dbutils.widgets.get("catalog")
W0 = dbutils.widgets.get("window_start")
W1 = dbutils.widgets.get("window_end")

LESIONS = f"{CATALOG}.staged.lesion_classified"
FACRULE  = f"{CATALOG}.raw.facility_rule"
FACROUTE = f"{CATALOG}.raw.facility_route"
OUT = f"{CATALOG}.staged.patient_days"

print(f"window: {W0} (inclusive) .. {W1} (exclusive)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The facility rule table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.raw")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACRULE} (
  account_name       STRING,   -- must match path-log account_name exactly
  facility_substring STRING,   -- case-insensitive substring of the Facility field
  rule               STRING,   -- FULL | MARKED_ONLY | DISREGARD | HOME_OVERRIDE
                              -- [23 Sep] VOB_ONLY: the facility's patient-days
                              -- are admitted only when they matched a VOB.
                              -- Unlike the other four this CANNOT be decided
                              -- here -- the match does not exist until 06 -- so
                              -- 03 labels the day and 08 enforces it.
                              -- The DEFAULT for a second facility is FULL. Confirmed
                              -- 21 Aug: several facilities on one account's log are
                              -- normally different doctors' rooms at the same practice,
                              -- not separate sites. MARKED_ONLY / DISREGARD are the
                              -- exception and need a reason.
                              -- FULL is a RECORDED DECISION, not a behaviour change:
                              -- counting everything is already the default. It exists so
                              -- "I checked, both facilities belong to this account" can be
                              -- written down once instead of re-asked every month. The
                              -- gate below tests rule IN ('MARKED_ONLY','DISREGARD'), so
                              -- a FULL row can never alter a count.
  note               STRING
) USING DELTA
""")

SEED = [
    # account                                                 substring                rule
    ("Brilliance Dermatology- Thousand Oaks",                 "Brilliance Dermatology", "MARKED_ONLY",   "Calabasas; home writes itself (TO)"),
    ("Brilliance Dermatology- Thousand Oaks",                 "Brilliance Dermatology (TO)", "HOME_OVERRIDE", "pin the home office"),
    ("Dermatology Associates of Edmond - Edmond",             "Oklahoma",               "MARKED_ONLY",   ""),
    ("Paolini Dermatology & Med Spa - Cape May Court House",  "Marmora",                "MARKED_ONLY",   ""),
    ("Goodman Dermatology and Mohs Surgery - Sun City West",  "Wickenburg",             "MARKED_ONLY",   ""),
    ("Goodman Dermatology and Mohs Surgery - Sun City West",  "Vida Center",            "MARKED_ONLY",   ""),
    ("Dr. Kim Ross, M.D. - San Antonio",                      "N Main Ave",             "MARKED_ONLY",   "changed from FULL, 3 Aug"),
    ("Alliance Dermatology and Mohs Center - Sun City",       "Sun City West Office",   "MARKED_ONLY",   "changed from FULL, 3 Aug"),
    ("Dr. Kim Ross, M.D. - Seguin",                           "Silverado",              "MARKED_ONLY",   "changed from FULL, 3 Aug"),
    ("Central Dermatology Center - Chapel Hill",              "Boyd Hall",              "DISREGARD",     "set to DISREGARD, 3 Aug"),
    # [23 Sep] Alaska. Both facilities are routed onto the Wasilla folder, so the
    # account_name here is Wasilla's. Anchorage is terminated: its days count
    # towards Wasilla ONLY when they matched a verification. Enforced in 08.
    ("Advanced Dermatology of Alaska, LLC - Wasilla",         "Anchorage",              "VOB_ONLY",      "terminated site; admitted only with a matched VOB. Becky, 23 Sep"),
    # 21 Aug — confirmed with the business: Payson operates BOTH addresses on Beeline
    # Hwy, so the 540/443 split across two facilities is one account, not two sites.
    # Recorded as FULL so the multi-facility review stops raising it.
    ("Alliance Dermatology & Mohs Center - Payson, AZ",       "715 S. Beeline",         "FULL",          "both Beeline addresses are this account (confirmed 21 Aug)"),
    ("Alliance Dermatology & Mohs Center - Payson, AZ",       "313 S. Beeline",         "FULL",          "both Beeline addresses are this account (confirmed 21 Aug)"),
    # 21 Aug — confirmed with the business: where one account's log carries several. See wiki.
    ("Goodman Dermatology and Mohs Surgery - Sun City West",   "Sun City",              "FULL",          "different doctor at the same practice (21 Aug). SUBSTRING of 'Sun City West' — see caution"),
    ("Scottsdale Skin Boutique & Dermatology- Scottsdale, AZ", "SSBD Chandler",         "FULL",          "different doctor at the same practice (21 Aug)"),
    ("RealSkin Dermatology - Woodway",                        "Real Skin Woodway Mohs", "FULL",          "Mohs room at the same practice (21 Aug)"),
    # NOTE: Fayetteville's misfiled site is handled by facility_route below, NOT by a. See wiki.
]

# [23 Sep] MERGE, not append-if-empty. This table is already populated, so the
# old guard meant a NEW rule added to SEED could never arrive -- it would sit in
# the list looking applied and change nothing. Same trap as facility_route and
# account_exclusion.
spark.createDataFrame(SEED, "account_name STRING, facility_substring STRING, rule STRING, note STRING") \
     .createOrReplaceTempView("_facrule_seed")
spark.sql(f"""
MERGE INTO {FACRULE} t
USING _facrule_seed s
   ON t.account_name = s.account_name AND t.facility_substring = s.facility_substring
WHEN MATCHED THEN UPDATE SET t.rule = s.rule, t.note = s.note
WHEN NOT MATCHED THEN INSERT *
""")
print(f"{FACRULE}: {spark.table(FACRULE).count()} rule(s) after merge")

display(spark.table(FACRULE).orderBy("account_name", "rule"))

# The pre-flight asks you to group rows by Facility and decide anything with more than. See wiki.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _fac_undecided AS
WITH fac AS (
  SELECT account_name, trim(facility) AS facility, count(*) AS rows
  FROM {LESIONS}
  WHERE trim(coalesce(facility, '')) <> ''
  GROUP BY account_name, trim(facility)
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY account_name ORDER BY rows DESC) AS rn,
            sum(rows)  OVER (PARTITION BY account_name)                      AS acct_rows,
            count(*)   OVER (PARTITION BY account_name)                      AS n_facilities,
            -- case/punctuation-only variant of a bigger value at the same account?
            count(*)   OVER (PARTITION BY account_name,
                             regexp_replace(lower(facility), '[^a-z0-9]', ''))  AS n_variants
  FROM fac
),
decided AS (SELECT DISTINCT account_name FROM {FACRULE})
SELECT r.account_name, r.facility, r.rows,
       round(100.0 * r.rows / r.acct_rows, 1) AS pct_of_account,
       r.n_facilities,
       CASE
            -- a column header or a phone fragment that leaked into the facility cell.
            -- Measured 21 Aug: 'Notifications' and '(843)' at Waccamaw Conway, 1 row
            -- each. Not sites, and they were being offered as rule candidates.
            WHEN lower(r.facility) RLIKE
                 '^(notification|comment|note|flag|action|plan|result|pathology|provider|patient|body site|facility|dob|accession)s?$'
                 THEN 'column header leaked into facility — parser artefact'
            WHEN r.facility RLIKE '^[^A-Za-z]*$'
                 THEN 'no letters — phone/number fragment, parser artefact'
            WHEN r.n_variants > 1 THEN 'case/punctuation variant — normalise, not a rule'
            WHEN EXISTS (SELECT 1 FROM fac b WHERE b.account_name = r.account_name
                           AND b.facility <> r.facility
                           AND lower(b.facility) LIKE concat('%', lower(r.facility), '%'))
                 THEN 'fragment of a longer facility at this account — parser artefact'
            ELSE 'CANDIDATE — a real second site?' END AS assessment
FROM ranked r
LEFT ANTI JOIN decided d ON d.account_name = r.account_name
WHERE r.n_facilities > 1 AND r.rn > 1
ORDER BY r.rows DESC
""")
_nu = spark.table("_fac_undecided").count()
print(f"\n{_nu} secondary facilit(ies) at accounts with NO recorded decision")
if _nu:
    display(spark.table("_fac_undecided"))
    _cand = spark.sql("""SELECT count(*) c FROM _fac_undecided
                         WHERE assessment LIKE 'CANDIDATE%'""").first()["c"]
    print(f"of which {_cand} are CANDIDATE — the rest are artefacts or normalisation")
    print("Only 'CANDIDATE' rows need a rule. Record the answer either way — a FULL row")
    print("changes no number and stops the same question being asked next month.")
    print("\nBEWARE THE SUBSTRING TRAP: if the secondary facility is contained in the")
    print("main one ('Sun City' inside 'Sun City West'), a MARKED_ONLY or DISREGARD rule")
    print("matches BOTH. Pair it with a HOME_OVERRIDE on the main value, as the")
    print("Brilliance Dermatology seed does.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Does the rule table actually bind?

# COMMAND ----------

display(spark.sql(f"""
WITH acct AS (SELECT DISTINCT account_name FROM {LESIONS}),
     fac  AS (SELECT DISTINCT account_name, facility FROM {LESIONS})
SELECT
  r.account_name, r.facility_substring, r.rule,
  CASE WHEN a.account_name IS NULL THEN 'NO SUCH ACCOUNT in path log'
       WHEN NOT EXISTS (SELECT 1 FROM fac f
                        WHERE f.account_name = r.account_name
                          AND lower(coalesce(f.facility,'')) LIKE
                              concat('%', lower(r.facility_substring), '%'))
            THEN 'account found, but NO FACILITY matches'
       ELSE 'binds' END                                   AS status
FROM {FACRULE} r
LEFT JOIN acct a ON a.account_name = r.account_name
ORDER BY status, r.account_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Patient key and deduplication

# COMMAND ----------

# A value that is a COLUMN LABEL is not an identifier. Found 21 Aug: the literal. See wiki.
_MRN_JUNK = "'mrn', 'accession', 'accession #', 'acc', 'acc #', 'patient', 'patient id', 'id', 'name', 'dob', 'chart', 'chart #', 'mr #', 'mr'"

# ---------------------------------------------------------------- phone-call date. See wiki.
_PHONE_RX = r"phone call\s*-\s*[^-]*?-\s*(\d{1,2}/\d{1,2}/\d{2,4})"
_PHONE_RX_SQL = _PHONE_RX.replace("\\", "\\\\").replace("'", "\\'")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _lesions_keyed_pre AS
SELECT
  l.*,
  -- every phone-call date on the row, unbounded. The 0..180 day bound needs the
  -- biopsy date and is applied in _days_raw, where it is a grouping key.
  filter(
    transform(
      regexp_extract_all(lower(coalesce(l.notifications, '')), '{_PHONE_RX_SQL}', 1),
      x -> {date_parse_sql('x')}
    ),
    d -> d IS NOT NULL
  )                                          AS _phone_dates,
  CASE
    WHEN coalesce(trim(l.accession), '') NOT IN ('', 'nan', 'None', '0')
     AND lower(trim(l.accession)) NOT IN ({_MRN_JUNK})
      THEN regexp_replace(trim(l.accession), '\\\\.0$', '')
    ELSE regexp_replace(lower(trim(coalesce(l.patient_name, ''))), '\\\\s+', ' ')
  END                                        AS patient_id,
  -- [31 Aug] All four hand-written date chains in this notebook now come from
  -- 00_vocab. A format missing from one copy and present in another is
  -- invisible: an unparseable date is a NULL, and a NULL biopsy_date is filtered out
  -- downstream rather than raising. That is how Cincinnati, Caledonia and Petoskey
  -- published a hard zero for July off 603 rows that had reached bronze intact.
  {date_parse_sql('l.result_date')}          AS _rd,
  {date_parse_sql('l.collected_date')}       AS biopsy_date,
  (coalesce(trim(l.accession), '') NOT IN ('', 'nan', 'None', '0')
   AND lower(trim(l.accession)) NOT IN ({_MRN_JUNK}))            AS keyed_on_mrn
FROM {LESIONS} l
""")

# ---------------------------------------------------------------- [24 Sep] MRN CONSOLIDATION
# The key above is decided PER LESION ROW: use the accession when it is usable,
# otherwise fall back to the name. A patient whose accession is filled on one
# lesion row and blank on the next therefore becomes TWO patients, and the
# patient-day is counted twice.
#
# Measured on the 24 Sep export: 139 double-counted patient-days across 14
# accounts, 92 of them marked. Three Yellowstone sites carried 91 of the 139.
#
# The fix resolves the key at the (account, name, biopsy_date) level: if exactly
# ONE usable accession appears anywhere in that group, every row in the group
# takes it. Zero accessions leaves the name key alone; TWO OR MORE means two real
# people share a name and a date, and those are left untouched -- merging them
# would be a worse error than the one being fixed. Six such groups exist.
spark.sql("""
CREATE OR REPLACE TEMP VIEW _lesions_keyed AS
SELECT k.* EXCEPT (patient_id),
       CASE
         WHEN size(array_distinct(filter(
                collect_list(CASE WHEN k.keyed_on_mrn THEN k.patient_id END) OVER w,
                x -> x IS NOT NULL))) = 1
           THEN element_at(array_distinct(filter(
                collect_list(CASE WHEN k.keyed_on_mrn THEN k.patient_id END) OVER w,
                x -> x IS NOT NULL)), 1)
         ELSE k.patient_id
       END AS patient_id
FROM _lesions_keyed_pre k
WINDOW w AS (PARTITION BY k.account_name,
                          regexp_replace(lower(trim(coalesce(k.patient_name, ''))), '\\s+', ' '),
                          k.biopsy_date)
""")

# No stable row id exists on the lesion view, so count the effect at the grain that
# matters instead of trying to join row to row.
_merged = spark.sql("""
SELECT
  (SELECT count(DISTINCT concat_ws('|', account_name, patient_id, cast(biopsy_date AS string)))
     FROM _lesions_keyed_pre WHERE biopsy_date IS NOT NULL) AS days_before,
  (SELECT count(DISTINCT concat_ws('|', account_name, patient_id, cast(biopsy_date AS string)))
     FROM _lesions_keyed     WHERE biopsy_date IS NOT NULL) AS days_after
""").first()
_collapsed = _merged["days_before"] - _merged["days_after"]
print(f"MRN consolidation: patient-days {_merged['days_before']:,} -> {_merged['days_after']:,} "
      f"({_collapsed:,} duplicate identity collapsed)")

# A group holding two or more DISTINCT accessions is two real people sharing a name
# and a date. The rule deliberately leaves them alone; surface them so the count is
# never a surprise.
_ambig = spark.sql("""
SELECT account_name,
       regexp_replace(lower(trim(coalesce(patient_name, ''))), '\\s+', ' ') AS name_norm,
       biopsy_date,
       count(DISTINCT patient_id) AS distinct_accessions
FROM _lesions_keyed_pre
WHERE keyed_on_mrn AND biopsy_date IS NOT NULL
GROUP BY 1, 2, 3
HAVING count(DISTINCT patient_id) > 1
""")
_na = _ambig.count()
print(f"  same name + same date carrying MORE THAN ONE accession: {_na} group(s) — left unmerged")
if _na:
    display(_ambig.orderBy("account_name"))

# COMMAND ----------

# ---------------------------------------------------- patient-key integrity gate. See wiki.
_unparsed = spark.sql("""
SELECT account_name,
       count(*)                                                   AS rows,
       count(DISTINCT file_name)                                  AS files,
       slice(array_sort(array_distinct(collect_list(collected_date))), 1, 6) AS samples
FROM _lesions_keyed
WHERE biopsy_date IS NULL
  AND trim(coalesce(collected_date, '')) NOT IN ('', 'nan', 'None')
GROUP BY account_name ORDER BY rows DESC
""")
_nu = _unparsed.count()
if _nu:
    display(_unparsed)
    _tot = spark.sql("""
      SELECT count(*) c FROM _lesions_keyed
      WHERE biopsy_date IS NULL
        AND trim(coalesce(collected_date, '')) NOT IN ('', 'nan', 'None')""").first()["c"]
    print(f"  *** {_tot:,} lesion row(s) across {_nu} account(s) CARRY a date this "
          f"pipeline cannot read. They are dropped silently downstream.")
    print(f"  *** Add the format to DATE_FORMATS in 00_vocab and re-run "
          f"this notebook. No re-extract is needed -- the rows are already in bronze.")
else:
    print("  every lesion row with a date has a readable one")

# COMMAND ----------

_key_bad = spark.sql("""
WITH per_value AS (
  SELECT account_name, patient_id, count(*) AS rows_on_this_key
  FROM _lesions_keyed
  WHERE keyed_on_mrn AND trim(coalesce(patient_id, '')) <> ''
  GROUP BY account_name, patient_id
),
per_account AS (
  SELECT account_name, count(*) AS acct_rows FROM _lesions_keyed GROUP BY account_name
)
SELECT v.account_name, v.patient_id AS key_value, v.rows_on_this_key, a.acct_rows,
       round(v.rows_on_this_key / a.acct_rows, 3) AS share_of_account
FROM per_value v JOIN per_account a ON a.account_name = v.account_name
WHERE v.rows_on_this_key >= 25
  AND v.rows_on_this_key / a.acct_rows >= 0.10
ORDER BY share_of_account DESC
""").collect()

if _key_bad:
    print("*** one patient key carries an implausible share of an account's rows ***")
    for r in _key_bad:
        print(f"    {r['share_of_account']:.1%} of {r['account_name']!r}: "
              f"{r['rows_on_this_key']:,} of {r['acct_rows']:,} rows on key {r['key_value']!r}")
    raise AssertionError(
        f"{len(_key_bad)} account(s) have a patient key that is not a key. Every row "
        "sharing that value collapses into one patient-day per date, so Appropriate is "
        "understated by however many real patients are behind it. Either the value is a "
        "parser artefact — add it to _MRN_JUNK above — or that account's accession column "
        "is not an MRN and needs a rule of its own. Do not proceed: the account's "
        "conversion rates are wrong in a direction that looks like poor performance.")
print("patient-key integrity: no account has a single key over 10% of its rows")

# --------------------------------------------------------------- facility routing. See wiki.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACROUTE} (
  facility_substring STRING,   -- case-insensitive substring of the Facility field
  owner_account      STRING,   -- the folder/account that owns this facility
  applies_within     STRING,   -- LIKE pattern bounding where the rule may fire
  note               STRING
) USING DELTA
""")

ROUTE_SEED = [
    # Both directions, so it self-corrects whichever folder the files land in.
    ("Fay 110", "Polley Clinic of Dermatology - Fayetteville #1",
     "Polley Clinic of Dermatology - Fayetteville%",
     "site 110 is Fayetteville #1 (21 Aug: 3 files misfiled into #2)"),
    ("Fay 106", "Polley Clinic of Dermatology - Fayetteville #2",
     "Polley Clinic of Dermatology - Fayetteville%",
     "site 106 is Fayetteville #2"),

    # [23 Sep] ALASKA. The folder is named Anchorage but holds BOTH sites, and
    # Wasilla is the larger and the growing one: 122 of 145 appropriate
    # patient-days and all of the growth (0/0/52/70 against Anchorage 0/6/9/8).
    # The two substrings are mutually exclusive -- no facility value contains
    # both -- so the fan-out guard below stays satisfied.
    ("Wasilla",   "Advanced Dermatology of Alaska, LLC - Wasilla",
     "Advanced Dermatology of Alaska%",
     "folder misnamed Anchorage; Wasilla is the practice. Becky, 23 Sep."),
    # [23 Sep, revised] Anchorage routes to the SAME folder -- one row, not two.
    # Anchorage is terminated, so its patient-days are admitted only when they
    # matched a VOB: the VOB_ONLY facility rule below does that. Routing both
    # here and filtering there keeps the two concerns apart -- routing decides
    # WHOSE line it is, the facility rule decides WHETHER it counts.
    ("Anchorage", "Advanced Dermatology of Alaska, LLC - Wasilla",
     "Advanced Dermatology of Alaska%",
     "terminated site folded onto Wasilla; admitted only with a matched VOB"),
]

# [23 Sep] MERGE, not append-if-empty. The old guard was
#     if spark.table(FACROUTE).count() == 0:
# so once the two Polley rows existed, no later seed could ever arrive -- a new
# route would sit in this list looking applied and do nothing.
spark.createDataFrame(ROUTE_SEED,
    "facility_substring STRING, owner_account STRING, applies_within STRING, note STRING") \
     .createOrReplaceTempView("_route_seed")
spark.sql(f"""
MERGE INTO {FACROUTE} t
USING _route_seed s
   ON t.facility_substring = s.facility_substring
  AND t.applies_within     = s.applies_within
WHEN MATCHED THEN UPDATE SET t.owner_account = s.owner_account, t.note = s.note
WHEN NOT MATCHED THEN INSERT *
""")
print(f"{FACROUTE}: {spark.table(FACROUTE).count()} route(s) after merge")

display(spark.table(FACROUTE).orderBy("applies_within", "facility_substring"))

# An owner_account that does not exist in the path log would route rows into a folder
# with no primary in account_map, which scores zero VOBs -- worse than the misfiling.
# [23 Sep] A route may now target a name that exists only in the ACCOUNT MAP.
# That is how a misnamed folder becomes two real sites: 03 invents
# "... - Wasilla" / "... - Anchorage", neither of which bronze has ever seen.
# The check's real purpose is to stop rows landing on a name that cannot score
# VOBs, and a name with a primary row in account_map satisfies that.
_bad_owner = spark.sql(f"""
  SELECT r.owner_account, r.facility_substring
  FROM {FACROUTE} r
  LEFT ANTI JOIN (
        SELECT DISTINCT account_name AS nm FROM {LESIONS}
        UNION
        SELECT DISTINCT folder_name  AS nm FROM {CATALOG}.raw.account_map
               WHERE role = 'primary'
  ) d ON d.nm = r.owner_account
""").collect()
if _bad_owner:
    for r in _bad_owner:
        print(f"*** route target {r['owner_account']!r} is neither a path-log folder "
              f"nor a primary row in account_map")
    raise ValueError(
        "facility_route owner_account must resolve to an account that can score "
        "VOBs — add it to raw.account_map first (see ROUTED_FOLDERS in 04)")
print("all route targets resolve")

# COMMAND ----------

# Two rules matching one lesion row would fan it out into two rows in the LEFT JOIN
# below. Count RULES per (account, facility) — collapsing the lesions to distinct
# combinations first. Counting rows out of the join instead counts lesions x rules,
# so a single working rule matching 175 lesions reads as "175 overlapping rules".
_fan = spark.sql(f"""
  WITH combos AS (
    SELECT DISTINCT account_name, coalesce(facility, '') AS facility FROM _lesions_keyed
  )
  SELECT c.account_name, c.facility,
         count(*)                              AS rules_matched,
         collect_list(r.facility_substring)     AS substrings,
         collect_list(r.owner_account)          AS targets
  FROM combos c
  JOIN {FACROUTE} r
    ON c.account_name LIKE r.applies_within
   AND lower(c.facility) LIKE concat('%', lower(r.facility_substring), '%')
   AND c.account_name <> r.owner_account
  GROUP BY c.account_name, c.facility
  HAVING count(*) > 1
""").collect()
if _fan:
    for r in _fan:
        print(f"*** {r['rules_matched']} rules match {r['account_name']!r} / {r['facility']!r}")
        print(f"      substrings {r['substrings']}  ->  {r['targets']}")
    raise ValueError(
        "overlapping facility_route rules would duplicate lesions — make the "
        "facility_substring values mutually exclusive, or narrow applies_within")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _lesions_routed AS
SELECT k.* EXCEPT (account_name),
       -- folder canon LAST, so the facility rules above still match on the name
       -- the rows were filed under. Folding two folder names into one here is
       -- what lets the (account_name, patient_id, biopsy_date) grain below
       -- collapse the same patient sent twice under two folder names.
       {folder_canon_sql("coalesce(r.owner_account, k.account_name)")} AS account_name,
       k.account_name                            AS account_name_as_filed,
       (r.owner_account IS NOT NULL)             AS rerouted
FROM _lesions_keyed k
LEFT JOIN {FACROUTE} r
       ON k.account_name LIKE r.applies_within
      AND lower(coalesce(k.facility, '')) LIKE concat('%', lower(r.facility_substring), '%')
      AND k.account_name <> r.owner_account
""")

_rk = spark.table("_lesions_keyed").count()
_rr = spark.table("_lesions_routed").count()
assert _rk == _rr, f"routing changed the row count {_rk:,} -> {_rr:,} — it must only relabel"
_moved = spark.sql("SELECT count(*) c FROM _lesions_routed WHERE rerouted").first()["c"]
print(f"rerouted {_moved:,} lesion rows of {_rr:,}")
if _moved:
    display(spark.sql("""
      SELECT account_name_as_filed, facility, account_name AS routed_to,
             count(*) AS rows, count(DISTINCT file_name) AS files
      FROM _lesions_routed WHERE rerouted
      GROUP BY 1, 2, 3 ORDER BY rows DESC
    """))
    print("These rows were filed in the wrong folder. The duplicates among them collapse")
    print("in the dedup below; anything unique to the misfiled copy is retained.")

# COMMAND ----------

# [31 Aug] THE RULE MOVED TO 00_vocab, IT DID NOT CHANGE. See wiki.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _lesions_dedup AS
SELECT * FROM (
  SELECT *, {lesion_dedup_rank_sql()} AS _rn,
            {lesion_copies_sql()}     AS _copies
  FROM _lesions_routed
) WHERE _rn = 1
""")

_all = spark.table("_lesions_keyed").count()
_ded = spark.table("_lesions_dedup").count()
print(f"lesion rows: {_all:,}  ->  {_ded:,} after dedup  ({_all - _ded:,} duplicates removed)")

# A lesion arriving in more than two copies means a log was sent three or more times.
# Worth seeing rather than silently collapsing, because it says the delivery, not the
# parser, is the thing to fix.
_many = spark.sql("SELECT count(*) c FROM _lesions_dedup WHERE _copies > 2").first()["c"]
if _many:
    display(spark.sql("""
      SELECT account_name, _copies, count(*) AS lesions,
             count(DISTINCT biopsy_date) AS days
      FROM _lesions_dedup WHERE _copies > 2
      GROUP BY 1, 2 ORDER BY lesions DESC LIMIT 40
    """))
    print(f"  {_many:,} lesion(s) arrived in 3+ copies — the same log was delivered "
          f"three or more times for those accounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Rows that cannot be counted

# COMMAND ----------

display(spark.sql("""
SELECT account_name,
       count(*)                                                        AS lesions,
       sum(CASE WHEN biopsy_date IS NULL THEN 1 ELSE 0 END)            AS no_date,
       sum(CASE WHEN trim(coalesce(patient_id,'')) = '' THEN 1 ELSE 0 END) AS no_patient,
       sum(CASE WHEN keyed_on_mrn THEN 1 ELSE 0 END)                   AS keyed_on_mrn,
       min(biopsy_date)                                                AS first_biopsy,
       max(biopsy_date)                                                AS last_biopsy,
       datediff(max(biopsy_date), min(biopsy_date))                    AS span_days
FROM _lesions_dedup
GROUP BY account_name ORDER BY account_name
"""))

# The table above is displayed, and a displayed table is not a gate. Rejecting a junk. See wiki.
_dropped = spark.sql("""
SELECT account_name,
       count(*)                                                            AS lesions,
       sum(CASE WHEN trim(coalesce(patient_id,'')) = '' THEN 1 ELSE 0 END)  AS no_patient
FROM _lesions_dedup GROUP BY account_name
HAVING sum(CASE WHEN trim(coalesce(patient_id,'')) = '' THEN 1 ELSE 0 END) >= 25
   AND sum(CASE WHEN trim(coalesce(patient_id,'')) = '' THEN 1 ELSE 0 END) / count(*) >= 0.10
ORDER BY 3 DESC
""").collect()

if _dropped:
    print("*** accounts losing a material share of rows for want of a patient identifier ***")
    for r in _dropped:
        print(f"    {r['account_name']!r}: {r['no_patient']:,} of {r['lesions']:,} rows "
              f"({r['no_patient']/r['lesions']:.1%}) have neither an MRN nor a name")
    raise AssertionError(
        f"{len(_dropped)} account(s) would silently lose 10%+ of their lesions in section 5. "
        "Most likely the column parse is misaligned for that layout, so BOTH the accession "
        "and the name are wrong — check a source file against the extracted row before "
        "trusting the account's numbers.")
print("no account loses 10%+ of its rows for want of a patient identifier")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Roll up to patient-days

# COMMAND ----------


# ---------------------------------------------------------- VOB-mention evidence. See wiki.
_RX   = sql_rx(VOB_MENTION)
_SNIP = sql_rx("(.{0,110}(?:" + VOB_MENTION + ").{0,110})")

# which of the six searched fields actually contained the phrase
_FIELDS = ",\n      ".join(
    f"CASE WHEN lower(coalesce({c}, '')) RLIKE '{_RX}' THEN '{c}' END"
    for c in SEARCH_COLS)

# the searched text, labelled per field, so a snippet is traceable to its column
_LABELLED = " || ' | ' || ".join(
    f"concat('[{c.upper()}] ', coalesce({c}, ''))" for c in SEARCH_COLS)

VOB_EVIDENCE_SQL = f"""
  concat_ws(', ', array_sort(array_distinct(flatten(collect_list(
    CASE WHEN vob_mentioned THEN filter(array(
      {_FIELDS}
    ), x -> x IS NOT NULL) END)))))                            AS vob_mention_fields,
  max(CASE WHEN vob_mentioned
           THEN regexp_extract(lower({_LABELLED}), '{_SNIP}', 1) END)
                                                               AS vob_mention_snippet,
  max(CASE WHEN vob_mentioned THEN substr({_LABELLED}, 1, 2000) END)
                                                               AS vob_mention_raw,
"""

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _days_raw AS
SELECT
  account_name,
  patient_id,
  biopsy_date,
  max(patient_name)                                           AS patient_name_any,
  bool_or(keyed_on_mrn)                                       AS keyed_on_mrn,

  bool_or(lesion_appropriate)                                 AS _appropriate_path,
  -- `AND NOT is_never_appropriate` is load-bearing, and its absence was a real hole.
  --
  -- `lesion_appropriate` already ends in `AND NOT is_never_appropriate`, so a
  -- melanoma lesion contributes FALSE to `_appropriate_path`. But the day-level flag
  -- is `(_appropriate_path OR srt)`, and bare `bool_or(srt_marked)` was TRUE for an
  -- SRT-MARKED melanoma row -- which put the day straight back into Appropriate
  -- through the OR. The melanoma exclusion therefore only ever removed days whose
  -- melanoma lesion was UNMARKED, which is the easy half.
  --
  -- Marked-melanoma is exactly the case Becky ruled on: "we should not include those
  -- 4 examples, they will not be srt appropriate if its a melanoma mix."
  bool_or(srt_marked AND NOT is_never_appropriate)            AS srt,
  bool_or(srt_marked_strict)                                  AS srt_strict,
  bool_or(vob_mentioned)                                      AS vob_mentioned,
{VOB_EVIDENCE_SQL}
  bool_or(path_class = 'acceptable')                          AS any_acceptable,
  bool_or(path_class = 'unknown')                             AS any_unknown,
  -- CELL TYPE, carried up so notebook 04 can corroborate a name match against
  -- what the biopsy actually showed. Salesforce records a cell type on every VOB
  -- (BCC 55%, SCC 22%, SCC in situ 20%), and measured 25 Aug it contradicted 0
  -- of 169 genuine shared pairs while contradicting 74 of 785 suspect ones --
  -- so it never argues against a real match, which is what makes it usable.
  bool_or(lower(coalesce(pathology, '')) LIKE '%basal%')      AS any_bcc,
  bool_or(lower(coalesce(pathology, '')) LIKE '%squamous%')   AS any_scc,
  bool_or(has_actions)                                        AS has_actions,
  count(*)                                                    AS n_lesions,
  array_sort(array_distinct(collect_list(
      nullif(trim(coalesce(facility, '')), ''))))             AS facilities,
  -- [26 Aug] BOUNDED. Adding format branches means malformed strings now PARSE
  -- rather than falling through to NULL -- a wrapped year in a scanned log
  -- ('07/09/202 6' losing its space) becomes a date Spark holds happily. Two
  -- accounts (Branson, Heights Portland) carried one each, and because notebook
  -- 09 measures result -> VOB by subtraction their timing averages came out at
  -- MINUS THREE MILLION DAYS. Notebook 09's _DT() helper already had this bound;
  -- patient_days did not, so the bad value shipped one layer earlier.
  max(CASE WHEN _rd BETWEEN DATE'1990-01-01' AND DATE'2100-01-01' THEN _rd END)
                                                              AS result_date,
  -- PHONE-CALL DATE, ported from the v6 build_phone.py. The results call is the
  -- moment the practice actually tells the patient, and Becky's timing rule
  -- (25 Aug) uses it as the middle rung: result date if available, then phone
  -- call, then biopsy.
  --
  -- Notifications is a repeating free-text log, one entry per contact:
  --     Phone Call - <staff>, <staff> - 04/30/26 01:41 PM
  -- Only entries labelled `phone call` count. `left voice mail` (16,872 entries)
  -- is an ATTEMPT, not a contact, and must not be read as the patient having
  -- been told -- that distinction is the whole point of the column.
  --
  -- Bounded 0..180 days after the biopsy, same as build_phone.py: an unbounded
  -- min() would pick up a call about a previous lesion.
  array_min(filter(
    flatten(collect_list(_phone_dates)),
    d -> datediff(d, biopsy_date) BETWEEN 0 AND 180
  ))                                                          AS phone_date,
  date_format(biopsy_date, 'MMM')                             AS month_label
FROM _lesions_dedup
WHERE biopsy_date >= date('{W0}')
  AND biopsy_date <  date('{W1}')
  AND trim(coalesce(patient_id, '')) <> ''
GROUP BY account_name, patient_id, biopsy_date
""")

# ---------------------------------------------------------------------------. See wiki.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _superseded AS
SELECT account_name, patient_id, biopsy_date
  FROM (
    SELECT account_name, patient_id, biopsy_date,
           bool_and({rlike_any(norm_sql('actions'), PLAN_TREATED_AT_BIOPSY)})
                                                       AS completed_says_treated
      FROM _lesions_routed
     WHERE lower(coalesce(file_name, '')) LIKE '%complet%'
     GROUP BY account_name, patient_id, biopsy_date)
 WHERE completed_says_treated
""")

# THE FILENAME CONVENTION IS THE FRAGILE PART, so check it before trusting a
# count. `%complet%` matched 63,863 of 88,802 lesion rows across 711 files on the
# Mar-Jun corpus. If a future export renames the logs, this filter quietly matches
# nothing and Rule B becomes a no-op that looks like "no cases found".
_cf = spark.sql("""
  SELECT count(*) AS rows, count(DISTINCT file_name) AS files
    FROM _lesions_routed
   WHERE lower(coalesce(file_name, '')) LIKE '%complet%'
""").first()
assert _cf["rows"] > 0, (
    "no source file name contains 'complet' — the completed/pending naming "
    "convention has changed and Rule B cannot fire. Check file_name before "
    "reading its patient-day count as zero cases.")
print(f"Rule B — completed logs: {_cf['rows']:,} lesion rows in {_cf['files']:,} files")
print(f"         superseded patient-days: {spark.table('_superseded').count():,}")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _days AS
SELECT
  d.*,
  (s.patient_id IS NOT NULL)                            AS superseded_by_completed,
  -- RULE B applied here. Rule A needs nothing at this level — it already arrived
  -- through lesion_appropriate, because PLAN_TREATED_AT_BIOPSY suppresses the
  -- LESION, so a day whose every lesion is treated has no appropriate lesion left
  -- and `_appropriate_path` is already false.
  --
  -- `AND s.patient_id IS NULL` sits OUTSIDE the `OR srt`, so Rule B beats an SRT
  -- mark where Rule A does not. The asymmetry is intended: Rule A trusts an
  -- explicit mark on the same document, while Rule B is the case where a LATER
  -- document says the lesion is gone. The later document wins.
  ((_appropriate_path OR srt) AND s.patient_id IS NULL)  AS appropriate,
  -- MARKED MUST CARRY THE SAME SUPPRESSION. classify.py computes
  -- marked = srt AND appropriate; appropriate has already absorbed srt, so this
  -- would reduce to bare `srt` — and a superseded day would then be marked but
  -- not appropriate, which trips the "marked never exceeds appropriate" gate in
  -- section 9 and would read as a pipeline bug rather than a rule.
  (srt AND (_appropriate_path OR srt)
       AND s.patient_id IS NULL)                         AS marked
FROM _days_raw d
LEFT JOIN _superseded s
       ON s.account_name = d.account_name
      AND s.patient_id   = d.patient_id
      AND s.biopsy_date  = d.biopsy_date
""")

_dq = spark.sql("""
SELECT count(*)                                                  AS days,
       sum(CASE WHEN superseded_by_completed THEN 1 ELSE 0 END)  AS superseded,
       sum(CASE WHEN appropriate THEN 1 ELSE 0 END)              AS appropriate,
       sum(CASE WHEN marked THEN 1 ELSE 0 END)                   AS marked
FROM _days
""").first()
print(f"patient-days in window: {_dq['days']:,}")
print(f"  appropriate {_dq['appropriate']:,} · marked {_dq['marked']:,} "
      f"· superseded by a completed log {_dq['superseded']:,}")
assert _dq["marked"] <= _dq["appropriate"], (
    f"marked {_dq['marked']:,} exceeds appropriate {_dq['appropriate']:,} — a "
    "suppression was applied to one and not the other.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply the facility rule

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _fac_kind AS
SELECT
  d.account_name, d.patient_id, d.biopsy_date, f.facility,
  CASE
    WHEN EXISTS (SELECT 1 FROM {FACRULE} r WHERE r.rule = 'DISREGARD'
                   AND r.account_name = d.account_name
                   AND lower(f.facility) LIKE concat('%', lower(r.facility_substring), '%'))
         THEN 'DISREGARD'
    WHEN EXISTS (SELECT 1 FROM {FACRULE} r WHERE r.rule = 'HOME_OVERRIDE'
                   AND r.account_name = d.account_name
                   AND lower(f.facility) LIKE concat('%', lower(r.facility_substring), '%'))
         THEN 'FULL'
    WHEN EXISTS (SELECT 1 FROM {FACRULE} r WHERE r.rule = 'MARKED_ONLY'
                   AND r.account_name = d.account_name
                   AND lower(f.facility) LIKE concat('%', lower(r.facility_substring), '%'))
         THEN 'MARKED_ONLY'
    -- [23 Sep] VOB_ONLY is a LABEL here, not a decision. The verification does
    -- not exist until 06, so the day is kept and carried to 08 on
    -- facility_kinds, which is already written to staged.patient_days.
    WHEN EXISTS (SELECT 1 FROM {FACRULE} r WHERE r.rule = 'VOB_ONLY'
                   AND r.account_name = d.account_name
                   AND lower(f.facility) LIKE concat('%', lower(r.facility_substring), '%'))
         THEN 'VOB_ONLY'
    ELSE 'FULL'
  END AS kind
FROM _days d
LATERAL VIEW explode(d.facilities) f AS facility
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _days_ruled AS
SELECT
  d.*,
  coalesce(k.kinds, array())                                   AS facility_kinds,
  CASE
    -- account carries no rule -> untouched
    WHEN NOT EXISTS (SELECT 1 FROM {FACRULE} r WHERE r.account_name = d.account_name
                       AND r.rule IN ('MARKED_ONLY','DISREGARD'))          THEN true
    -- no facility recorded -> cannot exclude
    WHEN size(coalesce(d.facilities, array())) = 0                         THEN true
    WHEN array_contains(k.kinds, 'FULL')                                   THEN true
    WHEN k.kinds = array('DISREGARD')                                      THEN false
    -- [23 Sep] A VOB_ONLY day MUST survive 03. 06 has to be given the chance to
    -- match it; 08 then drops the ones that did not. Dropping it here would
    -- guarantee it never matches and the rule would read as DISREGARD.
    WHEN array_contains(k.kinds, 'VOB_ONLY')                               THEN true
    ELSE d.marked
  END                                                          AS keep_day
FROM _days d
LEFT JOIN (
  SELECT account_name, patient_id, biopsy_date,
         array_sort(array_distinct(collect_list(kind))) AS kinds
  FROM _fac_kind GROUP BY account_name, patient_id, biopsy_date
) k
  ON k.account_name = d.account_name
 AND k.patient_id   = d.patient_id
 AND k.biopsy_date  = d.biopsy_date
""")

_before = spark.table("_days").count()
_kept = spark.sql("SELECT count(*) c FROM _days_ruled WHERE keep_day").first()["c"]
print(f"facility rule: {_before:,} patient-days -> {_kept:,} kept ({_before - _kept:,} dropped)")

display(spark.sql("""
SELECT account_name,
       count(*)                                        AS days_before,
       sum(CASE WHEN keep_day THEN 1 ELSE 0 END)       AS days_kept,
       sum(CASE WHEN NOT keep_day THEN 1 ELSE 0 END)   AS days_dropped
FROM _days_ruled
GROUP BY account_name
HAVING sum(CASE WHEN NOT keep_day THEN 1 ELSE 0 END) > 0
ORDER BY days_dropped DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write `staged.patient_days`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _days_out AS
SELECT
  account_name, patient_id, patient_name_any AS patient_name, biopsy_date,
  date_format(biopsy_date, 'yyyy-MM')                         AS ym,
  month_label, result_date, phone_date,
  appropriate, marked, srt, srt_strict, vob_mentioned,
  vob_mention_fields, vob_mention_snippet, vob_mention_raw,
  any_acceptable, any_unknown, has_actions, keyed_on_mrn,
  -- THIS SELECT IS AN EXPLICIT LIST, NOT `*`. A column added to the aggregation
  -- above reaches _days_ruled and stops there unless it is named here too --
  -- which is exactly how any_bcc / any_scc were lost on their first run, and
  -- notebook 04 failed with UNRESOLVED_COLUMN on a table that had just been
  -- rebuilt. Add new columns in BOTH places.
  any_bcc, any_scc,
  n_lesions, facilities, facility_kinds
FROM _days_ruled
WHERE keep_day
""")

# Created empty on the first run in a fresh catalog: replaceWhere against a table that
# does not exist yet has nothing to replace.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OUT}
USING DELTA PARTITIONED BY (ym) AS
SELECT * FROM _days_out WHERE 1=0
""")

# A pre-existing unpartitioned table from before this change cannot take a
# replaceWhere on `ym`. Say so by name rather than failing on a predicate error that
# does not mention partitioning.
_parts = [f.name for f in spark.catalog.listColumns(OUT) if f.isPartition]
if "ym" not in _parts:
    raise RuntimeError(
        f"{OUT} is not partitioned by `ym` (partitions: {_parts or 'none'}). It was "
        f"built before patient_days became additive. Drop it once and re-run this "
        f"notebook with a window covering every month you want to keep — the extract "
        f"will not re-read a single file, so this costs SQL time and nothing else.")

_ym_in = ", ".join(
    repr(r["ym"]) for r in spark.sql(
        f"SELECT DISTINCT date_format(d, 'yyyy-MM') AS ym FROM (SELECT explode("
        f"sequence(date('{W0}'), date_sub(date('{W1}'), 1), interval 1 month)) AS d) "
        f"ORDER BY ym").collect())
print(f"replacing months: {_ym_in}")

(spark.table("_days_out").write.mode("overwrite")
   .option("replaceWhere", f"ym IN ({_ym_in})")
   .saveAsTable(OUT))

_kept = spark.sql(f"""
SELECT ym, count(*) AS days,
       CASE WHEN ym IN ({_ym_in}) THEN 'recomputed this run' ELSE 'kept from an earlier run' END AS state
FROM {OUT} GROUP BY ym ORDER BY ym
""")
display(_kept)
print(f"wrote {OUT}: {spark.table(OUT).count():,} patient-days across "
      f"{spark.sql(f'SELECT count(DISTINCT ym) n FROM {OUT}').first()['n']} month(s)")

# ---- PHONE-DATE GATE ------------------------------------------------------. See wiki.
_ph = spark.sql(f"""
SELECT count(*)                                              AS days,
       sum(CASE WHEN phone_date IS NOT NULL THEN 1 ELSE 0 END) AS with_phone,
       count(DISTINCT CASE WHEN phone_date IS NOT NULL
                           THEN account_name END)            AS accts_with_phone,
       count(DISTINCT account_name)                          AS accts,
       sum(CASE WHEN phone_date IS NOT NULL
                 AND datediff(phone_date, biopsy_date) NOT BETWEEN 0 AND 180
                THEN 1 ELSE 0 END)                           AS MUST_BE_ZERO,
       round(avg(CASE WHEN phone_date IS NOT NULL
                      THEN datediff(phone_date, biopsy_date) END), 1) AS avg_bx_to_phone
FROM {OUT}
""").collect()[0]

print(f"  phone date on {_ph['with_phone']:,} of {_ph['days']:,} patient-days "
      f"({100 * _ph['with_phone'] / max(_ph['days'], 1):.1f}%), "
      f"{_ph['accts_with_phone']} of {_ph['accts']} accounts")
print(f"  avg biopsy -> phone call: {_ph['avg_bx_to_phone']} days")

assert _ph["MUST_BE_ZERO"] == 0, (
    f"{_ph['MUST_BE_ZERO']} phone dates fall outside 0..180 days of the biopsy -- "
    "the bound in _days_raw is not being applied")

if _ph["accts_with_phone"] < 90:
    print(f"  !! REVIEW: only {_ph['accts_with_phone']} accounts have any phone date; "
          f"measured 124 of 175 on the Mar-Jun corpus. Either the Notifications "
          f"column stopped being extracted, or the entry label changed from "
          f"'phone call'. Check before using the timing chain -- a missing phone "
          f"rung silently demotes those accounts to the biopsy anchor, which "
          f"reads ~10 days slower.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7a. [21 Aug] Marked sharing between sites of one practice

# COMMAND ----------

MSHARE = f"{CATALOG}.raw.marked_share"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MSHARE} (
  source_account   STRING,   -- whose marked patient-days are lent out
  credited_account STRING,   -- which sibling also gets to count them
  note             STRING
) USING DELTA
""")

# Seeded 21 Aug on Becky's instruction. Modelled first on the March-June figures, and. See wiki.
SHARE_SEED = [
    ("Dr. Kim Ross, M.D. - La Vernia", "Dr. Kim Ross, M.D. - Seguin",
     "marked at La Vernia, credited to Seguin (Becky, 21 Aug)"),
    ("Dr. Kim Ross, M.D. - La Vernia", "Dr. Kim Ross, M.D. - San Antonio",
     "marked at La Vernia, credited to San Antonio (Becky, 21 Aug)"),
    ("Yellowstone Dermatology Associates - Billings",
     "Yellowstone Dermatology Associates - Cody",
     "marked at Billings, credited to Cody (Becky, 21 Aug)"),
    ("Yellowstone Dermatology Associates - Billings",
     "Yellowstone Dermatology Associates - Sheridan",
     "marked at Billings, credited to Sheridan (Becky, 21 Aug)"),
]

if spark.table(MSHARE).count() == 0 and SHARE_SEED:
    spark.createDataFrame(
        SHARE_SEED, "source_account STRING, credited_account STRING, note STRING"
    ).write.mode("append").saveAsTable(MSHARE)
    print(f"seeded {len(SHARE_SEED)} marked-share rows")
else:
    print(f"{MSHARE}: {spark.table(MSHARE).count()} rows — left alone")

# --- guards. A share row naming an account that does not exist shares nothing, in
# --- silence, which is the same failure shape as the four stale SEED_POOLED names.
_acc = {r["account_name"] for r in spark.sql(f"SELECT DISTINCT account_name FROM {OUT}").collect()}
_sh = spark.table(MSHARE).collect()
_bad = []
for r in _sh:
    if r["source_account"] == r["credited_account"]:
        _bad.append(f"self-reference: {r['source_account']!r} credits itself")
    for col in ("source_account", "credited_account"):
        if r[col] not in _acc:
            near = sorted(a for a in _acc if a[:18] == (r[col] or "")[:18])
            _bad.append(f"{col} {r[col]!r} is not an account in {OUT}"
                        + (f" — did you mean {near}?" if near else ""))
if _bad:
    for b in _bad:
        print("   ", b)
    raise ValueError(
        f"{len(_bad)} problem(s) in {MSHARE}. A share row pointing at a name that does not "
        "exist credits nothing and reports nothing — fix the names before trusting any "
        "Marked -> VOB figure for these practices.")

# A -> B and B -> A both present means each site counts the other's marks, so the pair's
# combined marked exceeds the pair's real patients. Legal, but almost never intended.
_pairs = {(r["source_account"], r["credited_account"]) for r in _sh}
_recip = sorted({tuple(sorted(p)) for p in _pairs if (p[1], p[0]) in _pairs})
if _recip:
    print("*** reciprocal share pairs — each site counts the other's marks: ***")
    for x, y in _recip:
        print(f"      {x!r}  <->  {y!r}")

# --- the fan-out. One row per (credited account, shared patient-day), so provenance is
# --- visible and a count is a plain COUNT rather than a join anyone has to reason about.
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.staged.marked_shared AS
SELECT
  s.credited_account       AS account_name,
  s.source_account,
  d.patient_id,
  d.biopsy_date,
  d.month_label,
  d.appropriate            AS appropriate_at_source,
  s.note
FROM {MSHARE} s
JOIN {OUT} d ON d.account_name = s.source_account
WHERE d.marked
""")

_imp = spark.sql(f"""
WITH own AS (
  SELECT account_name,
         sum(CASE WHEN marked THEN 1 ELSE 0 END)      AS marked_own,
         sum(CASE WHEN appropriate THEN 1 ELSE 0 END)  AS appropriate_days
  FROM {OUT} GROUP BY account_name
),
sh AS (
  SELECT account_name, count(*) AS marked_shared_in,
         concat_ws(', ', array_sort(array_distinct(collect_list(source_account)))) AS lent_by
  FROM {CATALOG}.staged.marked_shared GROUP BY account_name
)
SELECT o.account_name, o.appropriate_days, o.marked_own,
       coalesce(s.marked_shared_in, 0)                        AS marked_shared_in,
       o.marked_own + coalesce(s.marked_shared_in, 0)          AS marked_for_conversion,
       s.lent_by
FROM own o JOIN sh s ON s.account_name = o.account_name
ORDER BY marked_shared_in DESC
""")
print("\nmarked-share impact — what each credited account's denominator becomes:")
display(_imp)

# Marked/Appropriate must stay <= 100% (§7.2), and that gate reads `marked`, which is
# untouched. The SHARED denominator has no such ceiling and is not supposed to: a site
# borrowing a sibling's marks can legitimately exceed its own appropriate days. Printed
# so nobody reads it later as a broken roll-up.
for r in _imp.collect():
    if r["marked_for_conversion"] > r["appropriate_days"]:
        print(f"    note: {r['account_name']} borrows enough marks to exceed its own "
              f"appropriate days ({r['marked_for_conversion']} vs {r['appropriate_days']}) "
              f"— expected for a borrowing site, NOT a roll-up error")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Per-account summary — the NMSC and MARKED bands

# COMMAND ----------

summary = spark.sql(f"""
SELECT
  account_name,
  count(*)                                                     AS patient_days,
  sum(CASE WHEN appropriate THEN 1 ELSE 0 END)                 AS appropriate_days,
  count(DISTINCT CASE WHEN appropriate THEN patient_id END)    AS appropriate_patients,
  sum(CASE WHEN marked THEN 1 ELSE 0 END)                      AS marked_days,
  count(DISTINCT CASE WHEN marked THEN patient_id END)         AS marked_patients,
  sum(CASE WHEN vob_mentioned THEN 1 ELSE 0 END)               AS vob_mentioned_days,
  round(100.0 * sum(CASE WHEN marked THEN 1 ELSE 0 END)
              / nullif(sum(CASE WHEN appropriate THEN 1 ELSE 0 END), 0), 0) AS marked_pct,
  round(100.0 * avg(CASE WHEN appropriate AND has_actions THEN 1.0
                         WHEN appropriate THEN 0.0 END), 0)    AS actions_cov_pct
FROM {OUT}
GROUP BY account_name ORDER BY account_name
""")
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. QA gates

# COMMAND ----------

problems = []

# §7.2 — Marked/Appropriate must stay <= 100%
bad = summary.filter("marked_days > appropriate_days").collect()
for r in bad:
    problems.append(f"{r['account_name']}: marked {r['marked_days']} > appropriate "
                    f"{r['appropriate_days']} — roll-up is wrong")

# §4.1 — every account produced rows
src_accts = {r[0] for r in spark.sql(f"SELECT DISTINCT account_name FROM {LESIONS}").collect()}
out_accts = {r[0] for r in spark.sql(f"SELECT DISTINCT account_name FROM {OUT}").collect()}
for a in sorted(src_accts - out_accts):
    problems.append(f"{a}: has lesions but produced ZERO patient-days in the window "
                    f"({W0}..{W1}) — check dates before assuming a quiet quarter")

# §4.1 — coverage of the window. Span alone is not enough: an account with 47 patient. See wiki.
_n_months = spark.sql(f"""
  SELECT count(DISTINCT date_format(d, 'yyyy-MM')) AS n
  FROM (SELECT explode(sequence(date('{W0}'), date_sub(date('{W1}'), 1), interval 1 day)) AS d)
""").first()["n"]

# A short span has three different causes and they need different responses.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _coverage AS
SELECT account_name,
       count(*)                                           AS days,
       datediff(max(biopsy_date), min(biopsy_date))        AS span,
       count(DISTINCT date_format(biopsy_date, 'yyyy-MM')) AS months,
       min(biopsy_date)                                   AS first_biopsy,
       max(biopsy_date)                                   AS last_biopsy,
       -- practice group: text before the first dash, same rule as group_alias
       trim(lower(regexp_replace(account_name, '\\\\s*-\\\\s*.*$', ''))) AS practice_group
FROM {OUT} GROUP BY account_name
""")

# (1) A MISSING MONTH is unambiguous — a calendar month in the window with no
#     patient-days at an account that reported in the others.
for r in spark.sql(f"""
    SELECT * FROM _coverage WHERE months < {_n_months} ORDER BY months, span
""").collect():
    problems.append(
        f"{r['account_name']}: only {r['months']} of {_n_months} months in the window "
        f"({r['first_biopsy']}..{r['last_biopsy']}, {r['days']} patient-days) — a month is "
        f"MISSING, not quiet. Its volume is not comparable to peers.")

# (2) SIBLING SITES SHARING AN IDENTICAL SHORT SPAN. Identical spans are common at the. See wiki.
for r in spark.sql(f"""
    SELECT practice_group, span, count(*) AS sites,
           concat_ws(', ', collect_list(account_name)) AS accounts,
           sum(days) AS days
    FROM _coverage
    WHERE months = {_n_months} AND span < 100
    GROUP BY practice_group, span
    HAVING count(*) > 1
    ORDER BY span
""").collect():
    problems.append(
        f"{r['accounts']}: {r['sites']} sites of one practice share an identical "
        f"{r['span']}-day span with all {_n_months} months present — a fixed-length EMR "
        f"export, not a date range. Their volume is structurally light against peers.")

# (3) Everything else short is thin volume, not a truncated export: an account with 40
#     patient-days over four months naturally has its first and last biopsy inside the
#     window. Reported, not flagged -- the old check counted these as problems and 2 of
#     its 6 findings were exactly this.
_thin = spark.sql(f"""
    SELECT c.* FROM _coverage c
    LEFT ANTI JOIN (
      SELECT practice_group, span FROM _coverage
      WHERE months = {_n_months} AND span < 100
      GROUP BY practice_group, span HAVING count(*) > 1
    ) sib ON sib.practice_group = c.practice_group AND sib.span = c.span
    WHERE c.months = {_n_months} AND c.span < 100
    ORDER BY c.days
""").collect()
if _thin:
    print(f"{len(_thin)} account(s) with a short span but every month present, and no")
    print("sibling sharing the span — thin volume, not a truncated export. Information only:")
    for r in _thin:
        print(f"  - {r['account_name']}: {r['days']} patient-days, span {r['span']}d, "
              f"{r['first_biopsy']}..{r['last_biopsy']}")

# §4.1 — names under 4 letters are OCR name loss
short = spark.sql(f"""
    SELECT account_name, count(DISTINCT patient_id) AS n
    FROM {OUT} WHERE NOT keyed_on_mrn AND length(patient_id) < 4
    GROUP BY account_name
""").collect()
for r in short:
    problems.append(f"{r['account_name']}: {r['n']} patient keys under 4 characters — OCR name loss")

if problems:
    print(f"{len(problems)} QA problem(s):\n")
    for p in problems:
        print("  -", p)
else:
    print("QA gates passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next