# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — build `raw.account_map`

# COMMAND ----------

# 04 is the only notebook downstream of bronze that did NOT load 00_vocab, so it never
# saw FOLDER_CANON. 03 folds a consolidated practice's folder names onto one canonical
# name; 04 read them raw. Three folders here, one account there -- see the _folders cell.

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("sf_accounts", "us_scdb_prod.comm_gold.rpt_account_dim",
                     "Salesforce account table (id + name)")
dbutils.widgets.text("sf_id_col", "account_id", "…its AccountId column")
dbutils.widgets.text("sf_name_col", "account_name", "…its name column")
dbutils.widgets.text("sf_where", "go_live_date IS NOT NULL",
                     "…filter to real practices (SQL predicate, blank = no filter)")
dbutils.widgets.dropdown("write", "no", ["yes", "no"], "Write account_map?")

C = dbutils.widgets.get("catalog")
SF = dbutils.widgets.get("sf_accounts")
SF_ID = dbutils.widgets.get("sf_id_col")
SF_NAME = dbutils.widgets.get("sf_name_col")
SF_WHERE = dbutils.widgets.get("sf_where").strip()
WRITE = dbutils.widgets.get("write") == "yes"

BRONZE = f"{C}.raw.path_log_rows"
AMAP = f"{C}.raw.account_map"

print(f"folders from : {BRONZE}")
print(f"accounts from: {SF} ({SF_ID}, {SF_NAME})")
print(f"write        : {'YES' if WRITE else 'no — dry run, review the output first'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The two sides

# COMMAND ----------

# FOLDER_CANON before DISTINCT, not after. On 21 Sep Dunedin sent per-site exports
# alongside its pooled one, so bronze carried three folder names for one practice.
# 03 folds all three onto "... - Dunedin #1 & 2"; 04 did not, so "- Dunedin #1" and
# "- Dunedin #2" matched Salesforce EXACTLY and each took a primary row holding an
# AccountId the pooled folder already carried. That is the "maps to 2 folders" half of
# the 06 assertion -- written by this notebook, not by a stale map.
# [23 Sep] ROUTED_FOLDERS — names that 03's facility_route CREATES and bronze
# has therefore never held. Without them here they get no account_map row, land
# in patient_days with no AccountId, and trip the blocking "folder maps to no
# account" review row — the Iconic Melbourne failure exactly.
#
# COUPLING WORTH KNOWING: 04 has to be told about names only 03 invents. Any
# future facility route that creates a name must be added to this list too.
ROUTED_FOLDERS = [
    "Advanced Dermatology of Alaska, LLC - Wasilla",
]
_rf = " UNION ".join(f"SELECT '{n}' AS folder_name" for n in ROUTED_FOLDERS)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _folders AS
SELECT DISTINCT {folder_canon_sql('account_name')} AS folder_name FROM {BRONZE}
WHERE trim(coalesce(account_name,'')) <> ''
UNION
{_rf}
""")

_folded = spark.sql(f"""
SELECT DISTINCT account_name AS raw_name,
       {folder_canon_sql('account_name')} AS canonical
FROM {BRONZE}
WHERE account_name IS NOT NULL
  AND account_name <> {folder_canon_sql('account_name')}
ORDER BY 1
""").collect()
if _folded:
    print(f"FOLDER_CANON folded {len(_folded)} folder name(s):")
    for _r in _folded:
        print(f"    {_r['raw_name']!r}\n      -> {_r['canonical']!r}")
    print("  Each gets NO map row of its own. Its patient-days are counted under the")
    print("  canonical folder by 03, so a second row here would double-count the VOBs.")
else:
    print("FOLDER_CANON: nothing to fold")

# rpt_account_dim holds far more than practices. A name search for "Preston" returns. See wiki.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _sf_all AS
SELECT DISTINCT {SF_ID} AS account_id, {SF_NAME} AS account_name FROM {SF}
WHERE {SF_ID} IS NOT NULL AND trim(coalesce({SF_NAME},'')) <> ''
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _sf AS
SELECT DISTINCT {SF_ID} AS account_id, {SF_NAME} AS account_name FROM {SF}
WHERE {SF_ID} IS NOT NULL AND trim(coalesce({SF_NAME},'')) <> ''
  {"AND (" + SF_WHERE + ")" if SF_WHERE else ""}
""")

nf  = spark.table("_folders").count()
ns  = spark.table("_sf").count()
nsa = spark.table("_sf_all").count()
print(f"{nf} distinct path-log folders")
print(f"{nsa} Salesforce accounts total")
if SF_WHERE:
    print(f"{ns} after the filter  ({SF_WHERE})  — removed {nsa - ns:,}")
    if ns == 0:
        raise ValueError(f"sf_where removed every account: {SF_WHERE!r}")
else:
    print(f"{ns} accounts (NO filter — fuzzy matching runs against people too)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exact join, then fuzzy on the remainder

# COMMAND ----------

from pyspark.sql.types import StringType
import re, difflib

sf_rows     = [(r["account_id"], r["account_name"]) for r in spark.table("_sf").collect()]
sf_rows_all = [(r["account_id"], r["account_name"]) for r in spark.table("_sf_all").collect()]
folders     = [r["folder_name"] for r in spark.table("_folders").collect()]


def norm(s):
    s = str(s or "").lower().replace("&", " and ")
    s = re.sub(r"\b(inc|llc|pllc|pc|pa|md|m\.d\.|do|d\.o\.|ltd|corp)\b", " ", s)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", s).split())


# ------------------------------------------------------------------ folder aliases. See wiki.
FOLDER_ALIAS = {
    # The lost character is a "+", not an "&" -- url_decode() decodes "+" as a space
    # (form-encoding). NB01 now pre-escapes it, so a fresh extract produces the correct
    # name and these four keys simply never fire. They stay for the rows already in
    # bronze, which carry the space-run form and are not worth re-extracting.
    "Preston Dermatology   Skin Surgery - Cary":
        "Preston Dermatology + Skin Surgery - Cary",                       # 001Do00000SJwKiIAL
    "Preston Dermatology   Skin Surgery - Holly Springs":
        "Preston Dermatology + Skin Surgery - Holly Springs",              # 001Do00000SJwKjIAL
    "Preston Dermatology   Skin Surgery - Wake Forest":
        "Preston Dermatology + Skin Surgery - Wake Forest",                # 0013h00002AQ5JTAA1
    "Saddleback Dermatology Laser   Cosmetic Center - Lake Forest":
        "Saddleback Dermatology Laser + Cosmetic Center - Lake Forest",    # 0013h000027JfYdAAK

    # --- consolidated sites (21 Aug) -------------------------------------------. See wiki.
    "Biltmore Dermatology - Terre Haute #1 & 2":
        "Biltmore Dermatology - Terre Haute #1",
    "Reading Dermatology - Reading #1 & 2":
        "Reading Dermatology - Reading #1",
    "Renew Family Dermatology - Fort Payne #1 & 2":
        "Renew Family Dermatology - Fort Payne #1",
    "Seraly Dermatology - McMurray #1 & 2":
        "Seraly Dermatology - McMurray #1",

    # --- consolidated site (29 Aug) --------------------------------------------. See wiki.
    "Kathleen P Soe, D.O. (Virginia Steet Dermatology) - Dunedin #1 & 2":
        "Kathleen P Soe, D.O. (Virginia Steet Dermatology) - Dunedin #1",

    # --- practice group vs operating brand (21 Sep) ----------------------------
    # Six folders reported UNMATCHED with score 0.0. The 0.0 is the literal the
    # UNMATCHED branch writes, NOT a computed similarity -- these never reached the
    # fuzzy step, they failed it. norm() strips punctuation but cannot delete a word:
    # "annexus dermatology deland" against "annexus dermatology and aesthetics deland"
    # scores 0.776 -- under the 0.88 cutoff. Measured ratios for all six:
    #   Orange City 0.805 · Port Orange 0.805 · Deland 0.776 · Davenport 0.727
    #   The Villages 0.615 · West Ocala 0.598
    # Lowering the cutoff is the WRONG fix. Catching West Ocala needs ~0.59, and
    # "Kenneth Wallace Dermatology - East Ocala, FL" -- a DIFFERENT SITE -- scores
    # 0.575 against that same folder. A 0.023 margin is not a threshold, it is a
    # coin toss that silently moves one site's VOBs onto another. Name them instead.
    "Annexus Dermatology - DayStar - Davenport":
        "Daystar Dermatology - Davenport",                      # 0013h00001eQbfQAAS
    "Annexus Dermatology - Deland":
        "Annexus Dermatology & Aesthetics - Deland",            # 001Do00000KhWI9IAN
    "Annexus Dermatology - Kenneth Wallace - The Villages":
        "Kenneth Wallace Dermatology - The Villages FL",        # 001Do00000RJepaIAD
    "Annexus Dermatology - Kenneth Wallace - West Ocala":
        "Kenneth Wallace Dermatology - West Ocala, FL",         # 001UN0000073y9OYAQ
    "Annexus Dermatology - Orange City":
        "Annexus Dermatology & Aesthetics - Orange City",       # 0013h00001FPsAHAA1
    "Annexus Dermatology - Port Orange":
        "Annexus Dermatology & Aesthetics - Port Orange",       # 001UN00000EhJxbYAF
    # Decoys in the same LIKE sweep, deliberately NOT aliased:
    #   Kenneth Wallace Dermatology - East Ocala, FL  001Do00000SIca6IAD  different site
    #   Premier Medical Associates  - Villages        001UN00000BBGRJYA5  different practice
    #   Daystar Skin & Cancer Center - Kissimmee      001Do00000KhW4DIAV  different site
    #   Marion Dermatology - Ocala                    001Do00000RK9lpIAD  unrelated
    # 'Caudill Dermatology - Clarkston' (001UN000004A6V6YAK, go-live 2026-06-09) needs
    # no alias -- its folder name and Salesforce name are identical, so it matches exact.
}

# Existence is checked against the UNFILTERED set, then the filter separately. A real
# practice missing its go-live date would otherwise be reported as a spelling mistake,
# which sends you looking in the wrong place.
by_exact     = {n: i for i, n in sf_rows}
by_exact_all = {n: i for i, n in sf_rows_all}
by_norm      = {}
for i, n in sf_rows_all:
    by_norm.setdefault(norm(n), []).append((i, n))

_alias_report = []
for _f, _t in FOLDER_ALIAS.items():
    _in_log      = _f in folders
    _in_sf       = _t in by_exact_all
    _in_filtered = _t in by_exact
    if not _in_log:
        _st = "folder not in the path log"
    elif not _in_sf:
        _st = "*** alias target is not a Salesforce account name -- check the spelling"
    elif not _in_filtered:
        _st = "*** exists but EXCLUDED by sf_where -- check its go-live date"
    else:
        _st = "ok"
    _alias_report.append((_f, _t, _in_log, _in_sf, _st))

if _alias_report:
    print("folder aliases:")
    for _f, _t, _il, _is_, _st in _alias_report:
        print(f"  {_st:62s} {_f!r}\n{'':64s} -> {_t!r}")
    _bad = [r for r in _alias_report if r[3] is False and r[2]]
    if _bad:
        print(f"\n{len(_bad)} alias target(s) did not match a Salesforce name. Nearest:")
        for _f, _t, *_ in _bad:
            print(f"  {_t!r}")
            for _c in difflib.get_close_matches(norm(_t), list(by_norm), n=3, cutoff=0.7):
                print(f"      {by_norm[_c][0][1]!r}")
def indexes(rows):
    """ex: name -> [account_id, ...]   nm: normalised name -> [(id, name), ...]

    ex maps to a LIST, not a single id. It used to be `{n: i for i, n in rows}`, which
    silently kept the last account when two shared a name -- and duplicate names are
    common in this org (`Dermatology Associates - Parent` exists 8 times with 8 ids).
    Measured 21 Aug: 3 folders whose Salesforce name matched exactly were nonetheless
    reported unmapped, because the OTHER account with that name held the VOBs. 277 VOBs
    and 167 simulations, invisible.

    Two AccountIds sharing a name and one folder is the textbook 8.2 case: one primary,
    the rest pooled onto the same folder.
    """
    ex = {}
    nm = {}
    for i, n in rows:
        ex.setdefault(n, []).append(i)
        nm.setdefault(norm(n), []).append((i, n))
    return ex, nm


def ladder(f, ex, nm, suffix=""):
    """alias -> exact -> normalised -> ambiguous -> fuzzy.

    Returns a LIST of rows (one per AccountId sharing the matched name), or None.
    Every id after the first is a `pooled` row on the same folder.
    """
    tgt = FOLDER_ALIAS.get(f, f)
    if tgt in ex:
        kind = "alias" if tgt != f else "exact"
        return [(f, i, tgt, kind + suffix, 1.0, bool(suffix) or tgt != f)
                for i in sorted(ex[tgt])]
    k = norm(tgt)
    if k in nm:
        kind = "normalised" if len(nm[k]) == 1 else "AMBIGUOUS"
        return [(f, i, n, kind + suffix, 1.0, True) for i, n in nm[k]]
    hit = difflib.get_close_matches(k, list(nm), n=1, cutoff=0.88)
    if hit:
        sc = round(difflib.SequenceMatcher(None, k, hit[0]).ratio(), 3)
        return [(f, i, n, "fuzzy" + suffix, sc, True) for i, n in nm[hit[0]]]
    return None


_ex_f,  _nm_f  = indexes(sf_rows)        # filtered — the precise population
_ex_a,  _nm_a  = indexes(sf_rows_all)    # everything, incl. individual people

# [23 Sep] FOLDER_ID_PIN — pin a folder straight to an AccountId, skipping the
# ladder. FOLDER_ALIAS maps to a Salesforce NAME and leaves the ladder to look it
# up; pinning the id means no fuzzy match can land these two elsewhere. That
# matters here: "... LLC - Wasilla" normalises close enough to the Anchorage
# record that difflib's 0.88 cutoff could take it, and the two sites would then
# share one id and trip assertion 3.
FOLDER_ID_PIN = {
    "Advanced Dermatology of Alaska, LLC - Wasilla": (
        "0013h00002BSIDDAA5",
        "23 Sep: the folder was misnamed Anchorage; Wasilla is the practice. "
        "Anchorage's AccountId is POOLED onto this folder below, not pinned — "
        "one row, two ids."),
}

# A pin bypasses the ladder, so none of the ladder's own reporting would catch a
# bad one. These three guards replace it.
_PIN_IDS  = {i for i, _ in FOLDER_ID_PIN.values()}
_PIN_NAME = {r["account_id"]: r["account_name"]
             for r in spark.table("_sf_all").collect()
             if r["account_id"] in _PIN_IDS}
_missing = [(f, i) for f, (i, _) in FOLDER_ID_PIN.items() if i not in _PIN_NAME]
if _missing:
    for f, i in _missing:
        print(f"*** pinned AccountId {i} for folder {f!r} is not in Salesforce")
    raise ValueError(
        "FOLDER_ID_PIN must name real AccountIds — a bad pin scores zero VOBs "
        "and nothing downstream will say so")
_pin_ids = [i for i, _ in FOLDER_ID_PIN.values()]
assert len(_pin_ids) == len(set(_pin_ids)), \
    f"two pinned folders share an AccountId: {FOLDER_ID_PIN}"
_seen = {r["folder_name"] for r in spark.table("_folders").collect()}
_pin_absent = [f for f in FOLDER_ID_PIN if f not in _seen]
if _pin_absent:
    for f in _pin_absent:
        print(f"*** pinned folder {f!r} is not in _folders")
    raise ValueError(
        "a pin on a folder 04 cannot see changes nothing — is ROUTED_FOLDERS missing?")
print(f"{len(FOLDER_ID_PIN)} folder(s) pinned directly to an AccountId:")
for f, (i, _) in FOLDER_ID_PIN.items():
    print(f"    {f!r}\n      -> {i}  {_PIN_NAME[i]!r}")

built, _fellback, _dupname = [], [], []
for f in folders:
    if f in FOLDER_ID_PIN:
        _pid, _pwhy = FOLDER_ID_PIN[f]
        built.append((f, _pid, _PIN_NAME[_pid], "pinned", 1.0, False))
        continue
    rows = ladder(f, _ex_f, _nm_f)
    if rows is None and SF_WHERE:
        # Nothing in the filtered set. Retry unfiltered rather than leaving the folder
        # unmapped -- but mark it, because a hit only found outside the filter is
        # either a practice missing its go-live date or a match onto a person's record.
        rows = ladder(f, _ex_a, _nm_a, suffix="+unfiltered")
        if rows is not None:
            _fellback.append((f, rows[0][2], rows[0][3]))
    if rows is None:
        built.append((f, None, None, "UNMATCHED", 0.0, True))
        continue
    if len(rows) > 1:
        _dupname.append((f, rows[0][2], [r[1] for r in rows]))
    built.extend(rows)

if _dupname:
    print(f"*** {len(_dupname)} folder(s) matched MORE THAN ONE AccountId on the same "
          f"name — first is primary, the rest pooled (8.2): ***")
    for f, n, ids in _dupname:
        print(f"    {f!r}\n      name {n!r} -> {len(ids)} ids: {ids}")
    print("Before this, only one of these ids reached account_map and the other's VOBs")
    print("were counted nowhere. Confirm they are the same practice, not a name clash.")

if _fellback:
    print(f"*** {len(_fellback)} folder(s) matched ONLY outside the sf_where filter ***")
    for f, n, k in _fellback:
        print(f"    {f!r}\n      -> {n!r}  ({k})")
    print("Check each: either the practice is missing its go-live date in Salesforce,")
    print("or the name has matched something that is not a practice at all.")
else:
    print("no folder needed the unfiltered fallback"
          if SF_WHERE else "no filter applied")

matched = spark.createDataFrame(
    built, "folder_name STRING, account_id STRING, matched_name STRING, "
           "match_type STRING, score DOUBLE, needs_review BOOLEAN")
matched.createOrReplaceTempView("_matched")

display(spark.sql("""
SELECT match_type, count(*) AS folders FROM _matched
GROUP BY match_type ORDER BY folders DESC
"""))

display(spark.sql("""
SELECT folder_name, matched_name, account_id, match_type, score
FROM _matched WHERE match_type <> 'exact' ORDER BY match_type, folder_name
"""))
print("Everything above needs your eyes. 'exact' rows do not.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pooled second IDs — the part that cannot be derived

# COMMAND ----------

# ------------------------------------------------------- known account exclusions. See wiki.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C}.raw.account_exclusion (
  account_id   STRING,
  account_name STRING,
  reason       STRING,
  note         STRING
) USING DELTA
""")

# [23 Sep] effective_from, added separately so an existing table upgrades in
# place rather than being dropped and rebuilt.
#   NULL   -> excluded from EVERY window, past and future
#   a DATE -> excluded from that date FORWARD; earlier months keep their history
_cols = {c.name for c in spark.table(f"{C}.raw.account_exclusion").schema}
if "effective_from" not in _cols:
    spark.sql(f"ALTER TABLE {C}.raw.account_exclusion ADD COLUMNS (effective_from DATE)")
    print("added effective_from to raw.account_exclusion")

EXCLUSION_SEED = [
    # (account_id, account_name, reason, note, effective_from)
    # All four NULL = dropped from every window. Becky, 23 Sep.
    # Measured cost on the 23 Sep export: -355 appropriate patient-days (0.66%),
    # -112 marked, -38 matched VOB. Every headline conversion moves <0.02 pts.
    ("0013h00001WN1MJAA1",
     "River Region Dermatology & Laser - Montgomery",
     "Not on the pull roster",
     "Active in Salesforce but not on Jackie's path-log pull list. 83 appropriate pt-days.",
     None),
    ("0013h000009j2bwAAA",
     "Dermatology & Cosmetic Laser (Laserderm Dermatology) - Smithtown",
     "Terminated",
     "last biopsy 2026-07-15; 0 Salesforce VOBs in August. 151 appropriate pt-days.",
     None),
    ("001UN000007wDX9YAM",
     "Cayce Dermatology Center and Medical Spa, PC - Columbia",
     "Terminated",
     "last biopsy 2026-07-22; 0 Salesforce VOBs in August. 85 appropriate pt-days.",
     None),
    ("001UN00000EYV3oYAH",
     "Biltmore Dermatology - Vincennes #1",
     "Terminated",
     "last biopsy 2026-07-07; 0 Salesforce VOBs in August. 36 appropriate pt-days, "
     "35 marked, 23 sims — converting hard right to the end.",
     None),
    # [23 Sep] PETOSKEY. Excluded RETROACTIVELY (effective_from None) at Becky's
    # direction. Flagging the shape of the data, because this one does not look
    # like the four above and the removal is larger than any of them:
    #   appropriate  May 11 -> Jun 10 -> Jul 14 -> Aug 19   (August is its PEAK)
    #   August       14 marked - 4 matched VOB - 2 sims
    #   totals       54 appropriate pt-days, 45 marked (83%), 20 VOBs, 11 sims
    #   latest Salesforce VOB created 2026-09-15
    # The other four had a last biopsy in July and zero August VOBs. Petoskey was
    # still producing when the export was cut, so a retroactive drop removes live
    # history rather than trailing noise: the August master will no longer
    # reconcile, and every prior-period trend point moves. Confirmed as intended.
    ("0013h00001rk5FmAAI",
     "The Derm Institute of West Michigan- Petoskey",
     "Terminated",
     "Terminated per Becky, 23 Sep. Dropped from every window. Removes 54 appropriate "
     "pt-days, 45 marked, 20 VOBs, 11 sims -- August was its strongest month and a "
     "VOB was created 2026-09-15, so this is NOT a trailing-off account.",
     None),
    # NOT excluded: Advanced Dermatology of Alaska. Anchorage is terminated but
    # its log is still counted, separately from Wasilla, to see whether volume
    # recovers. See ROUTED_FOLDERS and FOLDER_ID_PIN.
]

if EXCLUSION_SEED:
    spark.createDataFrame(
        EXCLUSION_SEED,
        "account_id STRING, account_name STRING, reason STRING, note STRING, "
        "effective_from DATE").createOrReplaceTempView("_excl_seed")
    # MERGE, not append-if-empty. The old guard meant one hand-added row would
    # have blocked every future seed silently.
    spark.sql(f"""
    MERGE INTO {C}.raw.account_exclusion t
    USING _excl_seed s ON t.account_id = s.account_id
    WHEN MATCHED THEN UPDATE SET
      t.account_name = s.account_name, t.reason = s.reason,
      t.note = s.note, t.effective_from = s.effective_from
    WHEN NOT MATCHED THEN INSERT *
    """)

# An exclusion naming an account_id no folder maps to removes nothing while
# looking effective. Say so now rather than at the next audit.
_orphan = spark.sql(f"""
SELECT e.account_id, e.account_name
FROM {C}.raw.account_exclusion e
LEFT JOIN (SELECT DISTINCT account_id FROM {C}.raw.account_map) m
       ON m.account_id = e.account_id
WHERE m.account_id IS NULL
""")
if _orphan.count():
    print("WARN — these exclusions match no folder in account_map, so they drop nothing:")
    display(_orphan)

_nex = spark.table(f"{C}.raw.account_exclusion").count()
print(f"{C}.raw.account_exclusion: {_nex} recorded exclusion(s)")
if _nex:
    display(spark.table(f"{C}.raw.account_exclusion").orderBy("reason", "account_name"))
else:
    print("  empty — every VOB-bearing account with no folder will appear for review")
    print("  until its disposition is recorded here.")

# COMMAND ----------

# ---------------------------------------------------------------- parent accounts. See wiki.
PARENT_WATCH = [
    "0013h00002AQ5JSAA1",   # Preston Dermatology + Skin Surgery - Parent
    "0013h000027JfYcAAK",   # Saddleback Dermatology Laser + Cosmetic Center - Parent
    "0013h00002AQ5JSAA1",
]
PARENT_WATCH = sorted(set(PARENT_WATCH))

POOLED = [
    ("Schaberg Dermatology - Maryville", "001Do00000Hi5RcIAJ",
     "Alton — no path log, pathology logged at Maryville (01_Counting_Rules 8.1)"),
    ("Kathleen P Soe, D.O. (Virginia Steet Dermatology) - Dunedin #1 & 2",
     "001UN00000BJgNOYA1", "8.2 — folder consolidated to '#1 & 2' on the July drop"),
    ("Reading Dermatology - Reading #1 & 2", "001UN00000UpYO9YAN", "8.2"),
    ("Renew Family Dermatology - Fort Payne #1 & 2", "001Do00000MQmX0IAL", "8.2"),
    ("Biltmore Dermatology - Terre Haute #1 & 2", "0013h00002AhD4fAAF", "8.2"),
    ("Seraly Dermatology - McMurray #1 & 2", "001UN00000TiqWsYAJ", "8.2"),

    # [23 Sep] ALASKA — Anchorage's AccountId pooled onto the Wasilla folder.
    # This is what makes the VOB_ONLY rule in 03 able to do anything: 06 matches
    # a patient-day against EVERY id on its folder, so an Anchorage patient
    # whose verification sits under the Anchorage account can still be found.
    # Pin the primary and pool this one, and the account has one row and both
    # ids. Pinning both instead would have made two rows.
    ("Advanced Dermatology of Alaska, LLC - Wasilla", "001UN00000DL9dBYAT",
     "terminated Anchorage site, folded onto Wasilla; its VOBs stay reachable"),
]
spark.createDataFrame(POOLED, "folder_name STRING, account_id STRING, note STRING") \
     .createOrReplaceTempView("_pooled")

display(spark.sql("""
SELECT p.folder_name, p.account_id, s.account_name AS pooled_account_name, p.note,
       CASE WHEN f.folder_name IS NULL THEN '*** NO SUCH FOLDER in the path log'
            WHEN s.account_id  IS NULL THEN '*** NO SUCH ACCOUNT in Salesforce'
            ELSE 'ok' END AS status
FROM _pooled p
LEFT JOIN _folders f ON f.folder_name = p.folder_name
LEFT JOIN _sf s      ON s.account_id  = p.account_id
ORDER BY status DESC, p.folder_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The proposed map

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW _proposed AS
-- A folder can now match SEVERAL AccountIds (duplicate names in Salesforce). Exactly
-- one may be `primary` -- the primary owns the path log and Appropriate/Marked are
-- counted from it once -- so rank deterministically and pool the rest. Every row here
-- used to be labelled 'primary', which would fail assertion 3 the moment a folder
-- matched two ids.
SELECT folder_name, account_id,
       CASE WHEN row_number() OVER (PARTITION BY folder_name ORDER BY account_id) = 1
            THEN 'primary' ELSE 'pooled' END AS role,
       concat(match_type, CASE WHEN needs_review THEN ' — REVIEW' ELSE '' END,
              CASE WHEN count(*) OVER (PARTITION BY folder_name) > 1
                   THEN ' — duplicate Salesforce name, 8.2 pooled' ELSE '' END) AS note,
       needs_review
FROM _matched WHERE account_id IS NOT NULL
UNION ALL
SELECT p.folder_name, p.account_id, 'pooled' AS role, p.note, false AS needs_review
FROM _pooled p JOIN _folders f ON f.folder_name = p.folder_name
""")

# A folder can now receive rows from two places: the name match (one primary + any. See wiki.
_dupe_pair = spark.sql("""
  SELECT folder_name, account_id, count(*) AS rows,
         concat_ws(' | ', collect_list(role)) AS roles
  FROM _proposed GROUP BY folder_name, account_id HAVING count(*) > 1
""").collect()
if _dupe_pair:
    for r in _dupe_pair:
        print(f"*** {r['folder_name']!r} / {r['account_id']} appears {r['rows']}x "
              f"({r['roles']}) — remove it from POOLED, the name match already found it")
    raise ValueError("duplicate (folder_name, account_id) in the proposed map")

display(spark.sql("""
SELECT role, count(*) AS rows, sum(CASE WHEN needs_review THEN 1 ELSE 0 END) AS need_review
FROM _proposed GROUP BY role
"""))
display(spark.sql("SELECT * FROM _proposed ORDER BY folder_name, role"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Orphan candidates — §8.3, before it can even run

# COMMAND ----------

_grp = ("trim(replace(lower(regexp_replace(%s, "
        "'\\\\s*-\\\\s*(?=[A-Z]).*$|\\\\s+-\\\\s+.*$', '')), ' & ', ' and '))")

# The version of this cell that shipped claimed each row was "an account WITH VOBs" and. See wiki.
_VOB = f"{C}.raw.vob"
_have_vob = spark.catalog.tableExists(_VOB)
if not _have_vob:
    print(f"NOTE: {_VOB} does not exist yet — run Notebook 07 and re-run this cell.")
    print("      Without it there is no VOB count, so every row below is a *candidate*")
    print("      shape, not evidence that anything is being missed.")

_vob_cte = (f"SELECT account_id, count(DISTINCT vob_id) AS vobs FROM {_VOB} GROUP BY 1"
            if _have_vob else "SELECT '' AS account_id, CAST(NULL AS BIGINT) AS vobs")

display(spark.sql(f"""
WITH mapped AS (SELECT DISTINCT account_id FROM _proposed),
     grp_with_folder AS (
       SELECT DISTINCT {_grp % 'f.folder_name'} AS g FROM _folders f
     ),
     v AS ({_vob_cte})
SELECT
  CASE
    WHEN lower(s.account_name) RLIKE 'grandparent'        THEN '5 grandparent roll-up'
    WHEN lower(s.account_name) RLIKE '\\bparent\\b'       THEN '4 parent roll-up'
    WHEN s.account_name RLIKE '#[0-9]\\s*$'                THEN '1 SECOND ID — pool or separate'
    WHEN s.account_name NOT RLIKE ' - '                   THEN '3 bare group name'
    ELSE                                                       '2 sibling site, no path log'
  END                                        AS kind,
  coalesce(v.vobs, 0)                        AS vobs,
  s.account_id, s.account_name,
  {_grp % 's.account_name'}                  AS practice_group
FROM _sf s
LEFT JOIN mapped m ON m.account_id = s.account_id
LEFT JOIN v       ON v.account_id = s.account_id
LEFT JOIN {C}.raw.account_exclusion x ON x.account_id = s.account_id
WHERE m.account_id IS NULL
  AND x.account_id IS NULL            -- disposition already recorded
  AND {_grp % 's.account_name'} IN (SELECT g FROM grp_with_folder)
  {"AND coalesce(v.vobs, 0) > 0" if _have_vob else ""}
ORDER BY kind, vobs DESC, s.account_name
"""))

print("Ranked, and only rows that actually carry VOBs when raw.vob exists.\n")
print("kind 1 — SECOND ID: a second AccountId at a site whose #1 sends the path log.")
print("         This is the 8.3 Maryville/Alton shape. Pool it (role='pooled') unless")
print("         the second ID has biopsy patients of its own, in which case 8.2 says")
print("         keep them separate.")
print("kind 2 — sibling site with no path log: usually correct, the site is not in the")
print("         programme. Investigate only if it carries a lot of VOBs.")
print("kind 3 — bare group name: legacy or duplicate record. Check before pooling.")
print("kind 4/5 — Parent / Grandparent: hierarchy nodes, NOT treating sites. Never pool")
print("         one into a site folder; a Parent spanning three sites cannot be attributed")
print("         to one of them. VOBs here surface as `orphan` in the 8.3 leak review,")
print("         which is where that decision belongs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write it

# COMMAND ----------

if not WRITE:
    print("DRY RUN — nothing written. Review above, then set write = yes.")
else:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.raw")
    spark.sql(f"""
    CREATE OR REPLACE TABLE {AMAP} USING DELTA AS
    SELECT folder_name, account_id, role, note FROM _proposed
    """)
    n = spark.table(AMAP).count()
    print(f"wrote {AMAP}: {n} rows")

    bad = spark.sql(f"""
      SELECT account_id, count(DISTINCT folder_name) n FROM {AMAP}
      GROUP BY account_id HAVING n > 1
    """).count()
    noprim = spark.sql(f"""
      SELECT folder_name FROM {AMAP} GROUP BY folder_name
      HAVING sum(CASE WHEN role='primary' THEN 1 ELSE 0 END) <> 1
    """).count()
    unmapped = spark.sql("""
      SELECT f.folder_name FROM _folders f
      LEFT JOIN _proposed p ON p.folder_name = f.folder_name
      WHERE p.folder_name IS NULL
    """).count()
    print(f"  AccountIds in 2+ folders : {bad}      (must be 0)")
    print(f"  folders without a primary: {noprim}   (must be 0)")
    print(f"  folders with no AccountId: {unmapped} (each scores ZERO VOBs)")
    print("\nNotebook 04 will re-assert all three.")