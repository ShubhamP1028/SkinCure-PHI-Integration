# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pathology Focus — shared vocabulary

# COMMAND ----------

# ------------------------------------------------------------------ pathology --
# Matched case-insensitively against the RESULT field only (01_Counting_Rules 2).
# The bare forms and misspellings are OCR rescues and are intentional.
ACCEPTABLE = [
    r"\bbcc\b",
    r"\bscc\b",
    r"\bsccis\b",
    r"\bssc\b",                 # transposed SCC, incl. SSC-is
    r"basal cell",              # bare, rescues OCR-mangled BCCs
    r"asal cell carcinoma",     # missing-B typo, lost at the page margin
    r"basaloid",                # bare
    r"basosquamous",
    r"squam",                   # ANY squam* (2026-07-28)
    r"keratoa",                 # truncated KA token (2026-07-28)
    r"carcinoma in situ",       # bare, any differentiation
    # Bowen's disease IS squamous cell carcinoma in situ. Added 21 Aug on the evidence. See wiki.
    r"bowen(?!oid)",
    r"carcimona",               # recurring misspelling in the source logs
    # [27 Aug] "Non-Melanoma Skin Cancer" is literally the programme's target, and. See wiki.
    r"non-?\s*melanoma",
    # requires 'carcinoma' immediately before 'nodular' so that nodular melanoma
    # and prurigo nodularis are not swept in
    r"carcinoma,?\s+(superficial and\s+)?(micro)?nodular",
]

# EXCLUDED decides only rows where NO acceptable token is present — acceptable wins.
EXCLUDED = [
    r"melanoma", r"lentigo maligna",
    r"sebaceous carcinoma", r"atypical sebaceous", r"dfsp",
    r"dermatofibrosarcom",   # stem, not the full word: OCR splits it as
                            # "Dermatofibrosarcom a Protuberans" (9 rows, 21 Aug)
                            # and the trailing "a" then dodges the pattern.
    r"merkel", r"lymphoma", r"mycosis fungoides",
    r"kaposi", r"sarcoma", r"fibroxanthoma", r"adenocarcinoma", r"mucinous",
    r"pilomatrix", r"microcystic adnexal", r"poroma", r"paget",
    r"metasta", r"melanocytic", r"nevus", r"nevi", r"verruc",
    r"seborrheic keratosis", r"biopsy site changes",
    r"negative for carcinoma", r"no malignancy", r"no neoplasm",
    r"\bbenign\b", r"\bscar\b", r"\bulcer\b",
    r"bowenoid papulosis",   # see the lookahead note in ACCEPTABLE
    # ---- malignant, but not NMSC. Added 21 Aug from the first full run's review. See wiki.
    r"porocarcinoma",                  # eccrine, incl. "in-situ and invasive"
    r"adnexal (carcinoma|neoplasm|tumor)",
    r"carcinoma with adnexal",         # "...with adnexal/ductal differentiation"
    r"sweat gland",                    # `mucinous` misses "mucin producing"
    r"pilomatrical",                   # `pilomatrix` above misses this form
    r"lymphoepithelial",
    r"breast carcinoma",               # not a skin primary
    r"spitz",                          # melanocytic
    r"histiocytos",                    # Langerhans; NOT `histiocytoma`, which is
                                       # the benign dermatofibroma synonym
    r"smooth muscle",
    r"intraepithelial neoplasia",       # VIN and friends
    # lymphoid / haematologic
    r"lymphoid", r"lymphocytic", r"lymphoproliferat", r"\bctcl\b", r"leukemia",
    r"t[- ]cell infiltr", r"b-?cell infiltrat",
]

# ------------------------------------------------------------- benign sweep --. See wiki.
BENIGN = [
    # inflammatory / eczematous / papulosquamous
    r"dermatitis", r"psoriasi", r"eczemat", r"spongiotic",
    r"lichen simplex", r"lichen planus", r"lichenoid keratosis",
    r"urticaria", r"rosacea", r"folliculitis", r"vasculitis",
    r"pemphigoid", r"pemphigus", r"lupus erythematosus", r"morphea",
    r"scleroderma", r"hypersensitivity reaction", r"drug reaction",
    r"granuloma", r"granulation tissue", r"papillomatosis",
    r"poikiloderma", r"solar purpura", r"telangiectas",
    # infection / trauma / foreign body
    r"\btinea\b", r"dermatophytosis", r"herpes virus", r"condyloma",
    r"arthropod", r"tick bite", r"insect bite", r"wood splinter", r"molluscum",
    # keratoses / pigment / sun damage
    r"actinic keratosis",       # precursor, never NMSC on its own
    r"\bhak\b",                 # the practice's abbreviation for the above
    r"porokeratosis",           # benign; NOT porocarcinoma, which stays for review
    r"solar lentigo", r"\blentigo\b", r"solar elastosis",
    r"parakeratosis", r"hyperkeratosis", r"acanthosis",
    r"dyskeratoma", r"acantholytic dyskeratosis",
    r"hyperpigmentat", r"pigment incontinence",
    # benign tumours / cysts / appendageal
    r"\bcyst\b", r"cystic", r"hidrocystoma",
    r"dermatofibroma",          # NOT dermatofibrosarcoma — different string, and
                                # that one is in EXCLUDED above regardless
    r"fibrous histiocytoma", r"neurofibroma", r"hemangioma", r"\bangioma\b",
    r"angiolipoma", r"angiokeratoma", r"angioleiomyoma", r"\blipoma\b",
    r"xanthogranuloma", r"fibrous papule", r"angiofibroma",
    r"fibroepithelial polyp", r"acrochordon", r"trichilemmoma",
    r"pilomatricoma",           # benign; `pilomatrix` (carcinoma) is EXCLUDED
    r"sebaceous hyperplasia", r"sebaceous gland hyperplasia",
    r"sebaceous adenoma", r"sebaceoma",   # never bare `sebaceous`
    r"syringoma", r"hidradenoma", r"prurigo nodularis",
    # adnexal / nail / hair, non-neoplastic
    r"alopecia", r"onychodystrophy", r"nail dystrophy", r"keratosis pilaris",
    r"dermal fibrosis", r"healing wound", r"healing biopsy site",
]

# Any of these in the RESULT text vetoes the benign sweep — the row stays UNKNOWN
# and stays on FlagsReview. Deliberately broad; a false veto costs one row of
# reviewer attention, a false sweep costs a missed carcinoma.
BENIGN_BLOCK = [
    r"carcinom", r"carcimona", r"malignan", r"sarcoma", r"melanom",
    r"atypia", r"atypical", r"dysplas", r"invasive", r"in situ",
    r"concerning", r"suspicious", r"neoplasm", r"lymphoma", r"leukemia",
    r"metasta", r"lymphoproliferat",
]

CLEARED_RESULT = [r"no residual", r"margins free", r"margins clear"]
CLEARED_PLAN = [r"reassure:\s*margins clear", r"reassure:\s*benign"]

# ------------------------------------------------- RULE D · 18 Sep 2026 --
# "Margins clear, but lets treat with Imiquimod 5% M-Fx 6 weeks to be safe."
# The lesion was excised, margins came back clear, the topical is
# belt-and-braces. Nothing left to irradiate.
#
# The defect this fixes is the FIELD LIST, not the vocabulary: is_cleared read
# `pathology` and `actions` only. `comments` is in SEARCH_COLS -- so SRT marking
# has always seen it -- but is_cleared never did. A practice recording its
# disposition in Comments / Assigned To has never had its cleared lesions
# suppressed.
#
# Measured 18 Sep on 161,215 lesion rows: 25 hits, 16 of them Appropriate,
# 1 SRT-marked (survives via `OR srt_marked`). 0.013% of appropriate lesions.
#
# CLEARED_ANY is wider than CLEARED_RESULT on purpose. It is never used alone --
# it only ever fires in conjunction with TOPICAL_AGENT.
CLEARED_ANY = [
    r"no residual",
    r"margins?\s+(are\s+)?(free|clear)",
    r"clear margins?",
    r"free margins?",
]

# Brand and generic, because path logs use both.
TOPICAL_AGENT = (
    r"\bimiquimod\b|\baldara\b|\bzyclara\b|"
    r"\b5-?fu\b|fluorouracil|\befudex\b|\bcarac\b|fluoroplex|\btolak\b"
)
# DO NOT widen this to "any topical agent". Measured the same day: 5,893
# appropriate lesions mention one of these agents WITHOUT clearance language --
# 4.6% of all appropriate lesions, 367x this rule. Those are superficial BCC and
# SCCis being field-treated INSTEAD of definitively: the IG-SRT opportunity, not
# an exclusion. Suppressing them would delete 4.6% of the denominator and
# flatter the mark rate in the same move.
# [27 Aug] Its own name because notebook 03 needs exactly THIS test and not the. See wiki.
PLAN_TREATED_AT_BIOPSY = [
    # RULE A -- TREATED AT THE TIME OF BIOPSY. Becky's question, measured. See wiki.
    r"^ ?reassur\w*[:.][^|]*treated (by|at|with)",
    r"^ ?reassur\w*[:.][^|]*adequately treated",
    r"^ ?reassur\w*[:.][^|]*completely excised",
    r"^ ?reassur\w*[:.][^|]*removed at (the )?time",
]

PLAN_REMOVED = [
    r"refer:\s*radiation oncology",
    r"reassure:\s*benign\.",
    r"patient already treated",
] + PLAN_TREATED_AT_BIOPSY

# --------------------------------------- RULES E / F / G / I · 22 Sep 2026 --
# Four rules from the same two passes over the free-text fields. Each was
# measured the same way RULE D was: count the rows, count how many are still
# Appropriate, and compare the SRT-marked rate against the 31.9% baseline. A
# real exclusion sits well BELOW baseline. A phrase at or above it is a
# CONVERSION LOSS and must stay in the denominator -- that is the whole point of
# the report. Rejected on exactly that test: refused/declined 68.2%, "no SRT"
# 100%, insurance denial 52.5%, no-show 26.9%, AK field treatment 44.8%.
#
# All four sit INSIDE `OR srt_marked`, like is_cleared. 12 of their 180 hits are
# SRT-marked and are rescued by it -- so these rules remove 168 lesions, 136
# patient-days (0.25%), and ZERO marked patient-days.

# ---- RULE E -- margins clear in the PLAN, with no treatment attached --------
# 97 lesions / 85 patient-days / 15 accounts. 40 of them are one canned template
# at Rixis Columbus: "Please let pt know margins are clear and OK to recheck in
# 6 mo." Zero SRT marks, flags NO. That single account loses 30% of its
# denominator -- correct, but it needs an explanation line in the master.
#
# THE TRAP IS THE RULE. 11,740 rows carry clearance language and only 102 are
# still Appropriate: a 99.1% spare rate. Almost all the behaviour lives in
# CLEARED_PLAN_TRAP, so treat any edit to it as an edit to the rule itself.
CLEARED_PLAN_ONLY = r"margins?\s+(are\s+|is\s+)?(clear|free)\b"
CLEARED_PLAN_TRAP = (
    r"schedule[^|]{0,30}(excision|mohs|ed&?c)"          # a treatment is booked
    r"|recommend\w*\s+(an?\s+)?(ed&?c|excision|mohs|srt|radiation)"
    r"|referral\s+for\s+radiation"
    r"|perineural"                                      # high-risk feature
    r"|unclear"                                         # "margins unclear" -- the
                                                        # substring trap; without
                                                        # this the rule inverts
    r"|to\s+clear\s+margins?"                            # "reshave TO CLEAR margins"
                                                        # = margins are NOT clear
)

# ---- RULE F -- the patient has died -----------------------------------------
# 16 lesions / 8 patient-days / 9 accounts. Small, and conceptually unarguable:
# a deceased patient cannot convert. Anchored on pt/patient within 12 characters
# so it cannot fire on "pt's mother passed away" or "s/w daughter, her husband
# is deceased" -- the relative-died trap, which is why this is not a bare
# \bdeceased\b.
DECEASED = (
    r"\b(pt|pts|patient|patients)\b[^.|]{0,12}\b(is\s+)?(deceased|expired)\b"
    r"|\b(pt|patient)\b[^.|]{0,12}passed\s+away"
    r"|patient\s*is\s*deceased|patientis\s*deceased"     # OCR runs words together
)
# The 12-character window alone is NOT enough, and the self-check is what proved
# it: "pt's husband is deceased, she is the contact" fits inside it and would
# have deleted a LIVING patient's lesion. A possessive after pt/patient, or a
# relative named just before the death word, spares the row.
DECEASED_TRAP = (
    r"\b(pt|pts|patient|patients)\s*['\u2019]\s*s\b"
    r"|\b(husband|wife|mother|father|daughter|son|spouse|sister|brother|parent"
    r"|family|caregiver|friend|neighbou?r)\b[^.|]{0,25}(deceased|expired|passed\s+away)"
)

# ---- RULE G -- clearance in COMMENTS or NOTES with a benign disposition ------
# 18 lesions / 13 patient-days / 9 accounts. RULE D fixed is_cleared's field list
# only for the cleared-then-topical conjunction. A practice that records "margins
# came back clear, recheck in 6 months" in Comments still has no suppression, so
# this is the same defect in its general form.
#
# The disposition half is required. Clearance language alone in a free-text note
# is not a decision -- it is frequently the pathology quoted back before a
# treatment is chosen.
CLEARED_COMMENT = (
    r"(?<!un)(margins?\s+(are\s+|came\s+back\s+|is\s+)?(clear|free|negative)\b"
    r"|clear\s+margins?\b|free\s+margins?\b|negative\s+margins?\b|no\s+residual\b)"
)
BENIGN_DISPOSITION = (
    r"recheck|re-?check|monitor|watchful|follow\s+clinically|reassess"
    r"|f/?u\b|follow\s+up|no\s+tx|good\s+news|will\s+follow|successfully\s+removed"
)
CLEARED_COMMENT_TRAP = (
    r"un\s?clear|positive|to\s+clear\s+margins?"
    r"|didn'?t\s+get|did\s+not\s+get|pursuit"
)

# ---- RULE I -- the biopsy itself removed the lesion -------------------------
# 37 lesions / 34 patient-days / 9 accounts, and NOT ONE of them is SRT-marked
# (0.0% against a 31.9% baseline). The cleanest separation in either pass. The
# doctor states the biopsy took the whole lesion and the plan is a recheck, not
# a treatment; there is nothing left to irradiate.
#
# Three wordings, one clinical statement. The third is why RULES E and G missed
# this entirely: both look for MARGINS, and the pathologist wrote EDGES.
#   "removed with/at/during the biopsy"   Auburn, Golden State, Polley, Paolini
#   "completely excised / removed"        Clayton Hinshaw (10), Germantown
#   "inked edges are free"                Polley, quoting the path report
#
# Unlike RULE D this is a single-phrase test, not a conjunction, so it is safe
# over the concatenated free_text -- there is no second term to pair across
# fields. The TRAP is deliberately evaluated over that same concatenation: a
# treatment booked ANYWHERE on the row spares the lesion, which errs toward
# keeping a candidate rather than deleting one.
REMOVED_AT_BIOPSY = (
    r"(removed|excised|eradicated|destroyed)\s+(with|at|by|during|in)\s+(the\s+)?(biops|shave|bx)"
    r"|biops\w*\s+removed\s+the\s+entire"
    r"|lesion\s*was\s*removed\s*(with|at|in|during)"
    r"|complete(ly)?\s+(excised|removed|resected)"
    r"|(excised|removed)\s+in\s+(its\s+)?entirety"
    r"|(inked\s+|ink\s+)?edges?\s+(are\s+|is\s+)?(free|fine)\b"
)
# Every clause below was put here by a row it would otherwise have deleted
# wrongly. Do not trim this list without re-reading those rows.
REMOVED_AT_BIOPSY_TRAP = (
    r"not\s+complete|isn'?t\s+complete|incomplete"       # "not completely removed"
                                                         # Advanced Derm Carolinas x4
    r"|almost\s+all"                                     # Auburn, with an excision plan
    r"|may\s+have\s+been"                                # hedged, not asserted
    r"|make\s*sure\s*it\s*is\s*completely?"              # Musick -- OCR drops spaces,
    r"|to\s*(make\s*sure|ensure)\s*it\s*is"              # so the gaps are optional
    r"|reviewed\s*-\s*no\s+further\s+treatment"          # Saddleback stamps this on
                                                         # rows that WERE treated,
                                                         # one of them an SRT row. 19
    r"|treatment\s+options?"                             # Basin -- options offered
    r"|schedule[^|]{0,60}(excision|mohs|ed&?c|electrodesiccation|curettage"
    r"|radiation|srt|topical|efudex|imiquimod)"
    r"|refer[^|]{0,25}(mohs|radiation|srt)"
)

# ---- RULE H -- NOT SHIPPED ---------------------------------------------------
# "not a candidate for IG-SRT" was measured and dropped. All 7 of its hits are
# spared by its own trap, so it nets zero rows. A filter that removes nothing is
# dead code that implies coverage it does not provide. If the phrase is worth
# seeing, tag it into FlagsReview rather than filtering on it. Deliberately left
# here as a record so it is not re-proposed.

# ------------------------------------------------------------------ MELANOMA --. See wiki.
NEVER_APPROPRIATE = [
    r"melanom",              # melanoma · melanomas · melanomatous · OCR "melanom"
    r"lentigo maligna",      # melanoma in situ by another name; 138 rows, all
                            # already excluded today, so this is belt-and-braces
]

# Exempts the target diagnosis from the stem above. Tested against the SAME text.
NOT_MELANOMA = [r"non-?\s*melanom"]

# ----------------------------------------------------------------------- SRT --. See wiki.
SRT_BROAD = (
    r"srt|ig-?srt|igsrt|superficial radiation|radiation therapy|"
    r"\bradiation\b|\brad\b|\bxrt\b|gentle\s*cure|skincure|\bsgrt\b|"
    r"brachy\w*|\bebx\b"
)
SRT_STRICT = r"\bsrt\b|ig-?srt|igsrt|superficial radiation|radiation therapy"

VOB_MENTION = (
    r"\bvob\b|verification of benefits|verify benefits|"
    r"obtain benefits|benefits check"
)

# The six fields classify.py searches, in its order. classify.py's sixth is `Extra`,
# which the EMA export does not produce; `flags` is the nearest bronze equivalent.
SEARCH_COLS = ["pathology", "actions", "comments", "notifications", "notes", "flags"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL helpers — and the escaping trap

# COMMAND ----------

def sql_rx(pattern):
    """Escape a Python regex for embedding in a Spark SQL string literal."""
    return pattern.replace("\\", "\\\\").replace("'", "\\'")


def rlike_any(col_expr, patterns):
    """SQL boolean: does col_expr match any pattern in the list?

    An empty list yields `(false)`, not `()`. That is not defensive padding — the
    benign-sweep neutrality gate in notebook 02 rebuilds the classifier with BENIGN
    emptied in order to compare against it, and `()` is a syntax error that would
    make the gate look like a broken query rather than a passing test.
    """
    if not patterns:
        return "(false)"
    return "(" + " OR ".join(f"{col_expr} RLIKE '{sql_rx(p)}'" for p in patterns) + ")"


def norm_sql(col_expr):
    """Lowercased, whitespace-collapsed — what every pattern above expects."""
    return f"regexp_replace(lower(coalesce({col_expr}, '')), '\\\\s+', ' ')"


def search_text_sql(alias="b"):
    """The six searched fields concatenated, normalised."""
    inner = ", ".join(f"coalesce({alias}.{c}, '')" for c in SEARCH_COLS)
    return f"regexp_replace(lower(concat_ws(' ', {inner})), '\\\\s+', ' ')"


def path_class_sql(result_expr):
    """acceptable | excluded | unknown — ordering is load-bearing.

    History-of guard first, then ACCEPTABLE, then EXCLUDED. A row hitting both
    lists is acceptable, because classify.py returns on the first hit.

    The benign sweep is LAST, after EXCLUDED, and is gated on BENIGN_BLOCK. Its
    position is the whole of its safety: anything ACCEPTABLE already claimed is
    long gone by the time a benign token gets a look, so the sweep can only ever
    move a row that was heading for 'unknown'. Both destinations are equally
    non-acceptable, so no count moves — only what shows up on FlagsReview.
    """
    return f"""CASE
      WHEN trim({result_expr}) = '' THEN 'unknown'
      WHEN {result_expr} LIKE '%history of%'
       AND {result_expr} LIKE '%neoplasm%'
       AND {result_expr} NOT LIKE '%carcinoma%' THEN 'excluded'
      WHEN {rlike_any(result_expr, ACCEPTABLE)} THEN 'acceptable'
      WHEN {rlike_any(result_expr, EXCLUDED)}   THEN 'excluded'
      WHEN {rlike_any(result_expr, BENIGN)}
       AND NOT {rlike_any(result_expr, BENIGN_BLOCK)} THEN 'excluded'
      ELSE 'unknown'
    END"""


# COMMAND ----------

# MAGIC %md
# MAGIC ## The lesion dedup rule

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## The verification scope rule

# COMMAND ----------

def vob_scope_case_sql(v="v", sister="lk.leaked_to_sister"):
    """own site | ambiguous namesake | sister site | enterprise | no biopsy found."""
    return f"""CASE
    WHEN {v}.matched                              THEN 'own site'
    WHEN {v}.leak_outcome = 'ambiguous namesake'  THEN 'ambiguous namesake'
    WHEN coalesce({sister}, false)                THEN 'sister site'
    WHEN {v}.leak_folders IS NOT NULL             THEN 'enterprise'
    ELSE                                               'no biopsy found'
  END"""


def leaked_to_sister_sql(catalog, vob_match=None):
    """The CTE behind `sister site`: did this verification leak inside its own group?

    The explode sits in a subquery on purpose. Spark places every LATERAL VIEW after
    all relations, so `FROM t LATERAL VIEW ... JOIN x` does not parse.
    """
    vm = vob_match or f"{catalog}.staged.vob_match"
    return f"""
grp AS (
  SELECT m.folder_name, d.practice_group
  FROM {catalog}.raw.account_map m
  JOIN {catalog}.raw.account_dim d ON d.account_id = m.account_id
),
lk AS (
  SELECT x.vob_id,
         max(CASE WHEN g2.practice_group IS NOT NULL
                   AND g2.practice_group = g1.practice_group THEN 1 ELSE 0 END) = 1
           AS leaked_to_sister
  FROM (SELECT vob_id, vob_folder, leak_folder
          FROM {vm}
          LATERAL VIEW explode(leak_folders) f AS leak_folder
         WHERE leak_folders IS NOT NULL) x
  LEFT JOIN grp g1 ON g1.folder_name = x.vob_folder
  LEFT JOIN grp g2 ON g2.folder_name = x.leak_folder
  GROUP BY x.vob_id
)"""


def vob_in_window_sql(w0, w1, v="v"):
    """Is this verification inside the reporting window?

    A MATCHED verification is placed by the month of its BIOPSY -- that is what it is
    evidence about. An unmatched one is placed by the month it was CREATED, the only
    date it has. Mixing the two, or using creation for both, is what made the old
    disposition total the whole pool rather than the window.
    """
    return (f"""CASE WHEN {v}.cohort_biopsy_date IS NOT NULL
              THEN {v}.cohort_biopsy_date >= date('{w0}')
               AND {v}.cohort_biopsy_date <  date('{w1}')
              ELSE {v}.vob_created >= date('{w0}')
               AND {v}.vob_created <  date('{w1}')
         END""")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing a date out of bronze

# COMMAND ----------

# Order matters only for speed -- the forms are mutually exclusive. ISO first because
# it is the overwhelming majority. 'MMMM' matches a spelled-out month, 'MMM' an
# abbreviated one; a value failing one falls through to the other.

# COMMAND ----------

# ---------------------------------------------------------------- folder canon. See wiki.
FOLDER_CANON = {
    "Kathleen P Soe, D.O. (Virginia Steet Dermatology) - Dunedin #1":
        "Kathleen P Soe, D.O. (Virginia Steet Dermatology) - Dunedin #1 & 2",
    "Kathleen P Soe, D.O. Dermatology - Dunedin #2":
        "Kathleen P Soe, D.O. (Virginia Steet Dermatology) - Dunedin #1 & 2",
            "Iconic Dermatology - Melbourne":
        "Iconic Dermatology- Melbourne",
    # [23 Sep] Alaska. The folder was renamed Anchorage -> Wasilla at the source,
    # but the OLD folder's 34 rows (27 pts, 6/1-8/19) are still in bronze and the
    # new Wasilla export does NOT contain them -- the two folders write disjoint
    # facilities. Folding is therefore the only option that loses no patients.
    #
    # Deleting the old folder instead would drop those 27. Leaving it unfolded
    # puts AccountId 001UN00000DL9dBYAT on two folders, which 04 asserts on
    # because its VOBs would be counted once under each.
    #
    # The fold does NOT decide whether Anchorage COUNTS. The facility column
    # survives it, so the VOB_ONLY rule in 03 still admits those days only when
    # they matched a verification. Routing decides whose line it is; the facility
    # rule decides whether it counts.
    "Advanced Dermatology of Alaska, LLC, Anchorage, AK":
        "Advanced Dermatology of Alaska, LLC - Wasilla",
}


def folder_canon_sql(expr="account_name"):
    """CASE that folds a duplicate folder name onto its canonical one.

    The canonical name must be the one raw.account_map carries the
    role='primary' row for, or the summary keeps a NULL account_id and the
    sf_rollup lookup keeps missing -- which is the symptom that finds this in
    the first place.
    """
    if not FOLDER_CANON:
        return expr
    q = lambda s: s.replace("'", "''")
    whens = "\n".join(f"      WHEN {expr} = '{q(k)}' THEN '{q(v)}'"
                      for k, v in FOLDER_CANON.items())
    return f"CASE\n{whens}\n      ELSE {expr}\n    END"


# COMMAND ----------

DATE_FORMATS = ["M/d/yyyy", "M/d/yy", "d-MMM-yyyy", "d-MMM-yy",
                "MMMM d, yyyy", "MMM d, yyyy", "MMMM d yyyy"]


def date_parse_sql(c):
    """Every known bronze date form, coalesced. NULL when none of them fits."""
    tries = ", ".join([f"try_to_date({c})"]
                      + [f"try_to_date({c}, '{f}')" for f in DATE_FORMATS])
    return f"coalesce({tries})"


def date_parse_bounded_sql(c):
    """date_parse_sql with implausible years rejected. See the note above."""
    inner = date_parse_sql(c)
    return (f"CASE WHEN {inner} BETWEEN DATE'1990-01-01' AND DATE'2100-01-01' "
            f"THEN {inner} END")


# COMMAND ----------

# Signal first, then file and page for determinism. Read the note above before touching.
LESION_DEDUP_ORDER = ("srt_marked DESC, vob_mentioned DESC, has_actions DESC, "
                      "file_name, page_no")


def lesion_dedup_partition_sql(patient_expr="patient_id", date_expr="biopsy_date"):
    """The grain a re-sent lesion collapses on: one lesion, one patient, one day.

    `body_site` and `pathology` are lowercased and coalesced because the same lesion
    re-exported can differ in case, and in whether an empty field arrives as '' or
    NULL.
    """
    return (f"account_name, {patient_expr}, {date_expr}, "
            f"lower(coalesce(body_site,'')), lower(coalesce(pathology,''))")


def lesion_dedup_rank_sql(patient_expr="patient_id", date_expr="biopsy_date"):
    """row_number() over that grain — rank 1 is the copy to keep."""
    part = lesion_dedup_partition_sql(patient_expr, date_expr)
    return f"row_number() OVER (PARTITION BY {part} ORDER BY {LESION_DEDUP_ORDER})"


def lesion_copies_sql(patient_expr="patient_id", date_expr="biopsy_date"):
    """How many copies of this lesion arrived, across every file.

    Carried onto the surviving row rather than thrown away, the same way
    `vobs_for_this_biopsy` carries a collapsed duplicate verification. Someone
    reconciling the tab against a PDF page needs to know a second copy existed.
    """
    part = lesion_dedup_partition_sql(patient_expr, date_expr)
    return f"count(*) OVER (PARTITION BY {part})"


# COMMAND ----------


def cleared_topical_sql(actions_expr, comments_expr):
    """RULE D: cleared language AND a topical agent, in the SAME field.

    Two things are load-bearing.

    The CONJUNCTION. `margins clear` alone is already handled by CLEARED_RESULT /
    CLEARED_PLAN where it belongs. `imiquimod` alone must never suppress -- see
    the note under TOPICAL_AGENT.

    SAME FIELD. `free_text` concatenates the six searched fields, so a rule
    written against that would happily pair `margins clear` in Actions with
    `imiquimod` in Notes about a different lesion. Hence two per-field tests
    OR-ed, never one test over the concatenation.
    """
    def both(expr):
        return (f"({rlike_any(expr, CLEARED_ANY)} AND "
                f"{expr} RLIKE '{sql_rx(TOPICAL_AGENT)}')")
    return f"({both(actions_expr)} OR {both(comments_expr)})"


def cleared_plan_sql(actions_expr):
    """RULE E: clearance stated in the PLAN with no treatment attached.

    Reads `actions` ONLY -- that is the field it was measured on, and the field
    where a practice's disposition template lives. Widening it to free_text
    would pull in pathology text quoting the margin status before a treatment
    was chosen, which is not a decision.
    """
    return (f"({actions_expr} RLIKE '{sql_rx(CLEARED_PLAN_ONLY)}' "
            f"AND NOT {actions_expr} RLIKE '{sql_rx(CLEARED_PLAN_TRAP)}')")


def cleared_comment_sql(comments_expr, notes_expr):
    """RULE G: clearance in Comments or Notes, WITH a benign disposition.

    The conjunction is load-bearing for the same reason it is in RULE D.
    Clearance language on its own is often the path report quoted back; it only
    becomes a decision when a recheck/monitor disposition sits beside it.

    Both halves are tested over the two fields joined, not per field, because
    practices split "margins clear" and "recheck in 6mo" across Comments and
    Notes routinely. That is safe here -- unlike RULE D, both halves describe
    the same lesion's disposition, not two different lesions' treatments.
    """
    joined = f"concat_ws(' ', {comments_expr}, {notes_expr})"
    return (f"({joined} RLIKE '{sql_rx(CLEARED_COMMENT)}' "
            f"AND {joined} RLIKE '{sql_rx(BENIGN_DISPOSITION)}' "
            f"AND NOT {joined} RLIKE '{sql_rx(CLEARED_COMMENT_TRAP)}')")


def removed_at_biopsy_sql(search_expr):
    """RULE I: the biopsy removed the lesion; nothing left to irradiate.

    Single-phrase test over the concatenated free_text -- no conjunction, so no
    risk of pairing terms across two different lesions. The trap runs over the
    SAME text on purpose: a treatment booked anywhere on the row spares the
    lesion, erring toward keeping a candidate rather than deleting one.
    """
    return (f"({search_expr} RLIKE '{sql_rx(REMOVED_AT_BIOPSY)}' "
            f"AND NOT {search_expr} RLIKE '{sql_rx(REMOVED_AT_BIOPSY_TRAP)}')")


def deceased_sql(search_expr):
    """RULE F: the patient has died. Anchored on pt/patient within 12 chars AND
    guarded by DECEASED_TRAP, because the anchor alone lets "pt's husband is
    deceased" through -- see the note under DECEASED_TRAP."""
    return (f"({search_expr} RLIKE '{sql_rx(DECEASED)}' "
            f"AND NOT {search_expr} RLIKE '{sql_rx(DECEASED_TRAP)}')")


def classified_sql(src_relation, out_relation, temp=False):
    """THE silver view — one row per lesion, every derived column.

    This used to be two views (`path_log_flagged` then `lesion_classified`), split
    only because they were built in that order, never for a reason. Both were 1:1
    over bronze, so the split bought nothing and cost a join and an explanation.

    Defined here rather than in a notebook so the classifier's regression harness
    builds the identical view over probe rows. A regression that tests a *copy* of
    the logic tests nothing.
    """
    search = search_text_sql("b")
    result = norm_sql("b.pathology")
    actions = norm_sql("b.actions")
    comments = norm_sql("b.comments")     # RULE D reads this field
    notes    = norm_sql("b.notes")        # [22 Sep] RULE G reads this one
    kind = "TEMP VIEW" if temp else "VIEW"
    return f"""
CREATE OR REPLACE {kind} {out_relation} AS
WITH flagged AS (
  SELECT
    b.*,
    {search}  AS free_text,
    {result}  AS result_text,
    {actions} AS actions_text,

    -- ---- pathology type ---------------------------------------------------
    {path_class_sql(result)}                           AS path_class,
    ({path_class_sql(result)}) = 'acceptable'          AS is_nmsc_type,

    -- ---- already dealt with ----------------------------------------------
    ({rlike_any(result, CLEARED_RESULT)}
       OR {rlike_any(actions, CLEARED_PLAN)}
       OR {cleared_topical_sql(actions, comments)}
       OR {cleared_plan_sql(actions)}
       OR {cleared_comment_sql(comments, notes)}
       OR {removed_at_biopsy_sql(search)})             AS is_cleared,
    -- exposed so FlagsReview and 02b can separate RULE D suppressions from
    -- every other kind without re-deriving the regex
    {cleared_topical_sql(actions, comments)}           AS is_cleared_topical,
    -- [22 Sep] each new rule exposed on its own column so FlagsReview, 02b and
    -- the per-account QA guard can attribute a suppression without re-deriving
    -- the regex. They are ALSO OR-ed into is_cleared above; these are for audit.
    {cleared_plan_sql(actions)}                        AS is_cleared_plan,      -- RULE E
    {cleared_comment_sql(comments, notes)}             AS is_cleared_comment,   -- RULE G
    {removed_at_biopsy_sql(search)}                    AS is_removed_at_biopsy, -- RULE I
    {deceased_sql(search)}                             AS is_deceased,          -- RULE F
    {rlike_any(actions, PLAN_REMOVED)}                 AS is_plan_removed,

    -- ---- never appropriate, whatever else the row says ---------------------
    ({rlike_any(result, NEVER_APPROPRIATE)}
       AND NOT {rlike_any(result, NOT_MELANOMA)})      AS is_never_appropriate,

    -- ---- SRT marking ------------------------------------------------------
    {search} RLIKE '{sql_rx(SRT_BROAD)}'               AS srt_marked,
    {search} RLIKE '{sql_rx(SRT_STRICT)}'              AS srt_marked_strict,
    {search} RLIKE '{sql_rx(VOB_MENTION)}'             AS vob_mentioned,
    {result} RLIKE 'no residual'                       AS is_no_residual,
    (b.actions IS NOT NULL AND trim(b.actions) <> '')  AS has_actions
  FROM {src_relation} b
)
SELECT
  flagged.*,
  -- An SRT mark makes the lesion count on its own, whatever the pathology says --
  -- EXCEPT for melanoma. `AND NOT is_never_appropriate` sits outside the OR, so it
  -- overrides both an acceptable token and a mark. That is the only exception; do
  -- not add others here without measuring what they cost.
  -- [22 Sep] RULE F (is_deceased) joins is_cleared / is_plan_removed INSIDE the
  -- `OR srt_marked` backstop, deliberately. 5 of its 21 hits carry an SRT mark:
  -- that patient WAS a candidate and died before converting, which is a loss the
  -- report should show, not a row it should delete.
  ((((path_class = 'acceptable')
        AND NOT is_cleared AND NOT is_plan_removed AND NOT is_deceased)
      OR srt_marked)
    AND NOT is_never_appropriate)                      AS lesion_appropriate
FROM flagged
"""


# Kept so an old notebook calling flagged_sql still works — it now builds the
# same single view. Remove once nothing references it.
flagged_sql = classified_sql


# proof the escaping survives this runtime — cheap, and it has bitten before.
# "assert scoring" contains s-r-t inside a word; a working \b rejects it.
_wb = sql_rx(r"\bsrt\b")
_p = spark.sql(
    f"SELECT 'srt discussed' RLIKE '{_wb}' AS hit, "
    f"       'assert scoring' RLIKE '{_wb}' AS should_be_false"
).first()
assert _p["hit"] and not _p["should_be_false"], (
    "word-boundary escaping is broken on this runtime — every \\b pattern would "
    f"silently return false (hit={_p['hit']}, false-positive={_p['should_be_false']})"
)

# proof the negative lookahead survives too — Spark RLIKE is java.util.regex, which
# supports (?!...), but a runtime that quietly did not would silently re-admit
# bowenoid papulosis to Appropriate. Cheap to check, invisible if it breaks.
_bw = sql_rx(r"bowen(?!oid)")
_l = spark.sql(
    f"SELECT \"bowen's disease\" RLIKE '{_bw}' AS keep, "
    f"       'bowenoid papulosis' RLIKE '{_bw}' AS should_be_false"
).first()
assert _l["keep"] and not _l["should_be_false"], (
    "negative lookahead is not honoured on this runtime — bowenoid papulosis would "
    f"classify as NMSC (keep={_l['keep']}, false-positive={_l['should_be_false']})"
)

# proof the benign sweep's guard is wired up: a benign token alone sweeps, the same
# token beside a concerning finding does not. This is the assertion that keeps the
# sweep from ever hiding a carcinoma from FlagsReview.
_cls = path_class_sql("pathology")
_g = spark.sql(f"""
  SELECT
    max(CASE WHEN pathology = 'epidermal inclusion cyst'  THEN {_cls} END) AS plain,
    max(CASE WHEN pathology LIKE 'hypertrophic actinic%'  THEN {_cls} END) AS guarded
  FROM (SELECT 'epidermal inclusion cyst' AS pathology
        UNION ALL SELECT 'hypertrophic actinic keratosis with focus concerning for '
                      || 'superficially invasive well-differentiated carcinoma')
""").first()
assert _g["plain"] == "excluded" and _g["guarded"] == "unknown", (
    "benign sweep is misordered or unguarded — plain benign should sweep to "
    f"'excluded' and a guarded string must stay 'unknown' "
    f"(plain={_g['plain']!r}, guarded={_g['guarded']!r})"
)

# [27 Aug] PLAN_TREATED_AT_BIOPSY, six cases — three that must fire and three that. See wiki.
_tb = rlike_any(norm_sql("plan"), PLAN_TREATED_AT_BIOPSY)
_t = spark.sql(f"""
  SELECT
    max(CASE WHEN id = 1 THEN {_tb} END) AS edc_at_biopsy,
    max(CASE WHEN id = 2 THEN {_tb} END) AS adequately,
    max(CASE WHEN id = 3 THEN {_tb} END) AS leading_space,
    max(CASE WHEN id = 4 THEN {_tb} END) AS history_not_plan,
    max(CASE WHEN id = 5 THEN {_tb} END) AS margins_clear,
    max(CASE WHEN id = 6 THEN {_tb} END) AS live_srt_plan
  FROM (
    SELECT 1 AS id, 'Reassure: treated by electrodesiccation and curettage at time of biopsy' AS plan
    UNION ALL SELECT 2, 'Reassure: adequately treated at the time of biopsy'
    UNION ALL SELECT 3, '  Reassure:   treated at time of biopsy'
    UNION ALL SELECT 4, 'Schedule: Mohs. Previously treated by ED&C'
    UNION ALL SELECT 5, 'Reassure: margins clear'
    UNION ALL SELECT 6, 'Schedule: Mohs vs superficial radiation therapy')
""").first()
assert (_t["edc_at_biopsy"] and _t["adequately"] and _t["leading_space"]
        and not _t["history_not_plan"] and not _t["margins_clear"]
        and not _t["live_srt_plan"]), (
    "PLAN_TREATED_AT_BIOPSY is mis-anchored — it must fire only on a reassurance "
    f"plan, never on a live plan that merely mentions past treatment ({dict(_t.asDict())})"
)

# [27 Aug] MELANOMA, eight cases. The first four must be excluded whatever else
# the row says; the last four are the target diagnosis and must survive. If the
# non-melanoma guard ever regresses, "Non-Melanoma Skin Cancer" — the programme's
# own name for what it treats — silently stops counting.
_mx = (f"({rlike_any(norm_sql('dx'), NEVER_APPROPRIATE)} "
       f"AND NOT {rlike_any(norm_sql('dx'), NOT_MELANOMA)})")
_m = spark.sql(f"""
  SELECT
    max(CASE WHEN id=1 THEN {_mx} END) AS plain_melanoma,
    max(CASE WHEN id=2 THEN {_mx} END) AS in_situ,
    max(CASE WHEN id=3 THEN {_mx} END) AS mixed_with_bcc,
    max(CASE WHEN id=4 THEN {_mx} END) AS lentigo_maligna,
    max(CASE WHEN id=5 THEN {_mx} END) AS non_hyphen,
    max(CASE WHEN id=6 THEN {_mx} END) AS non_space,
    max(CASE WHEN id=7 THEN {_mx} END) AS non_hyphen_space,
    max(CASE WHEN id=8 THEN {_mx} END) AS bcc_alone
  FROM (
    SELECT 1 AS id, 'Malignant Melanoma' AS dx
    UNION ALL SELECT 2, 'Melanoma in situ'
    UNION ALL SELECT 3, 'Malignant Melanoma in situ with Basal Cell Carcinoma, Superficial'
    UNION ALL SELECT 4, 'Lentigo Maligna'
    UNION ALL SELECT 5, 'Non-Melanoma Skin Cancer'
    UNION ALL SELECT 6, 'Non Melanoma Skin Cancer'
    UNION ALL SELECT 7, 'Non- Melanoma Skin Cancer'
    UNION ALL SELECT 8, 'Basal cell carcinoma, nodular type')
""").first()
assert (_m["plain_melanoma"] and _m["in_situ"] and _m["mixed_with_bcc"]
        and _m["lentigo_maligna"]
        and not _m["non_hyphen"] and not _m["non_space"]
        and not _m["non_hyphen_space"] and not _m["bcc_alone"]), (
    "melanoma exclusion is mis-specified — it must catch every melanoma including "
    "one mixed with an NMSC, and must NEVER catch a non-melanoma spelling "
    f"({dict(_m.asDict())})")

# And the same eight through lesion_appropriate end to end, because the guard being
# right in isolation is not the same as it reaching the column that counts.
_cls = classified_sql("_mel_cases", "_mel_out", temp=True)
spark.sql("""CREATE OR REPLACE TEMP VIEW _mel_cases AS
  SELECT 'Malignant Melanoma' AS pathology, 'Refer: SRT' AS actions, '' AS comments,
         '' AS notes, '' AS notifications, '' AS flags, '' AS extra, '' AS body_site
  UNION ALL SELECT 'Melanoma in situ with Basal Cell Carcinoma', 'Schedule: Excision', '', '', '', '', '', ''
  UNION ALL SELECT 'Non-Melanoma Skin Cancer', 'Schedule: SRT', '', '', '', '', '', ''
  UNION ALL SELECT 'Basal cell carcinoma, nodular type', '', '', '', '', '', '', ''""")
spark.sql(_cls)
_r = {r["pathology"]: r["lesion_appropriate"] for r in spark.table("_mel_out").collect()}
assert (not _r['Malignant Melanoma']
        and not _r['Melanoma in situ with Basal Cell Carcinoma']
        and _r['Non-Melanoma Skin Cancer']
        and _r['Basal cell carcinoma, nodular type']), (
    f"lesion_appropriate does not honour the melanoma rule: {_r}")
print("  melanoma rule: 8 pattern cases + 4 end-to-end cases pass")

# [18 Sep] RULE D, ten pattern cases. These exist for the same reason the \\b and
# lookahead proofs above do: a runtime that mishandles an escape, or an edit that
# turns the AND into an OR, converts this rule into either a no-op or a sweep of
# the ~5,900-lesion topical pool. Neither would error.
_RULE_D_CASES = [
    # (actions, comments, expected)
    ("", "margins clear, but lets treat with imiquimod 5% m-fx 6 weeks to be "
         "safe, recheck in 4-6 months assign",                       True),
    ("reassure: margins clear", "",                                  False),
    ("", "start imiquimod to the left cheek ak field",               False),
    ("", "schedule: srt. margins clear, efudex trialled last year",   True),
    ("margins are clear - 5-fu x 4 weeks", "",                        True),
    ("margins clear", "imiquimod 5%",                                False),
    ("", "no residual tumour, aldara nightly x 6 wks",                True),
    ("", "margins clear",                                            False),
    ("", "5fu to the dorsal hand",                                   False),
    ("excision with mohs; pt used efudex in 2023", "",               False),
]
_rd_rows = " UNION ALL ".join(
    "SELECT {} AS id, '{}' AS actions, '{}' AS comments, {} AS want".format(
        i, a.replace("'", "\\'"), c.replace("'", "\\'"), str(e).lower())
    for i, (a, c, e) in enumerate(_RULE_D_CASES))
_rd_expr = cleared_topical_sql(norm_sql("actions"), norm_sql("comments"))
_rd = spark.sql(f"SELECT id, actions, comments, want, {_rd_expr} AS got "
                f"FROM ({_rd_rows})").collect()
_rd_bad = [r for r in _rd if bool(r["got"]) != bool(r["want"])]
assert not _rd_bad, (
    f"RULE D self-check failed on {len(_rd_bad)} of {len(_RULE_D_CASES)} cases. "
    "Most likely causes, in order: backslash escaping (see the \\b proof above), "
    "the conjunction in cleared_topical_sql changed from AND to OR, or the two "
    "per-field tests collapsed into one test over free_text. Failing: "
    + "; ".join(f"A={r['actions']!r} C={r['comments']!r} "
                f"want={r['want']} got={r['got']}" for r in _rd_bad))

# ...and end to end through lesion_appropriate, because a guard being right in
# isolation is not the same as it reaching the column that counts. Case 2 carries
# an explicit SRT mark: RULE D lives inside `OR srt_marked`, so it must SURVIVE.
# Keyed on an explicit case_id. The first version keyed on comments[:28] and the
# lookup strings were written by hand, so a 29-character key raised KeyError before
# any assertion could run - and because this cell also defines classified_sql, the
# %run from notebook 03 then failed and date_parse_sql never reached the caller.
spark.sql("""CREATE OR REPLACE TEMP VIEW _rd_cases AS
  SELECT 1 AS case_id, 'Basal cell carcinoma, superficial' AS pathology, '' AS actions,
         'Margins clear, but lets treat with Imiquimod 5% M-Fx 6 weeks' AS comments,
         '' AS notes, '' AS notifications, '' AS flags, '' AS extra, '' AS body_site
  UNION ALL SELECT 2, 'Basal cell carcinoma, superficial', '',
         'Margins clear, treat with Imiquimod. Discussed SRT, pt interested',
         '', '', '', '', ''
  UNION ALL SELECT 3, 'Basal cell carcinoma, superficial', '',
         'Start imiquimod 5% to the lesion', '', '', '', '', ''
  UNION ALL SELECT 4, 'Basal cell carcinoma, nodular type', '', '', '', '', '', '', ''""")
spark.sql(classified_sql("_rd_cases", "_rd_out", temp=True))
_rd_map = {r["case_id"]: (bool(r["is_cleared_topical"]), bool(r["lesion_appropriate"]))
           for r in spark.table("_rd_out").collect()}
assert _rd_map[1] == (True, False), (
    f"RULE D did not suppress a cleared-then-topical lesion: {_rd_map}")
assert _rd_map[2][1] is True, (
    f"a RULE D lesion carrying an explicit SRT mark was suppressed -- the rule "
    f"belongs INSIDE `OR srt_marked`, as is_cleared is. {_rd_map}")
assert _rd_map[3] == (False, True), (
    f"a lesion being field-treated with NO clearance language was suppressed. "
    f"That is the 5,893-lesion IG-SRT pool, not an exclusion. {_rd_map}")
assert _rd_map[4] == (False, True), (
    f"a plain NMSC row with no comments stopped being appropriate: {_rd_map}")
print(f"  RULE D: {len(_RULE_D_CASES)} pattern cases + 4 end-to-end cases pass")

# [22 Sep] RULES E / F / G / I. Same reasoning as the RULE D cases above: an
# edit that turns a trap off, or a runtime that mishandles an escape, converts
# one of these into either a no-op or a sweep. Neither would raise.
#
# The two cases that matter most are the ones that bit during measurement:
#   RULE E  "margins unclear" -- `unclear` CONTAINS `clear`, so without the trap
#           the rule fires on precisely the rows it must not.
#   RULE F  "pt's mother passed away" -- a bare \bdeceased\b/passed away would
#           delete a living patient's lesion.
_RULE_EFGI_CASES = [
    # (rule, actions, comments, notes, search_extra, expected_suppressed)

    # ---- RULE E ----
    ("E", "please let pt know margins are clear and ok to recheck in 6 mo", "", "", "", True),
    ("E", "reassure: margins are clear, no tx needed", "", "", "", True),
    ("E", "margins unclear - schedule re-excision", "", "", "", False),
    ("E", "margins are clear but schedule mohs to be safe", "", "", "", False),
    ("E", "reshave to clear margins with hr in 6 weeks", "", "", "", False),
    ("E", "margins are clear, perineural invasion noted", "", "", "", False),
    ("E", "margins are clear - recommend excision", "", "", "", False),

    # ---- RULE G ----
    ("G", "", "margins came back clear", "recheck in 6 months", "", True),
    ("G", "", "no residual tumour, will follow clinically", "", "", True),
    ("G", "", "margins clear", "", "", False),   # no disposition = not a decision
    ("G", "", "margins unclear", "recheck in 6 months", "", False),
    ("G", "", "margins positive", "follow up in 3 months", "", False),
    ("G", "", "did not get clear margins", "recheck", "", False),

    # ---- RULE I ----
    ("I", "", "", "", "the lesion was removed at bx, recheck at next visit", True),
    ("I", "", "", "", "cat bcc completely removed.", True),
    ("I", "", "", "", "the examined inked edges are free in the plane of section", True),
    ("I", "", "", "", "no tx needed, lesion was removed during biopsy", True),
    ("I", "", "", "", "is not completely removed and he can rtc for options", False),
    ("I", "", "", "", "may have been completely removed with biopsy, we will recheck", False),
    ("I", "", "", "", "almost all of this lesion was removed with the bx, id like to look", False),
    ("I", "", "", "", "schedule: excision | 7/7/26 treated reviewed - no further treatment", False),
    ("I", "", "", "", "treatment options: 1. curettage and electrodesiccation | "
                      "lesion completely excised", False),

    # ---- RULE F ----
    ("F", "", "", "", "pt is deceased", True),
    ("F", "", "", "", "patient passed away 7/12", True),
    ("F", "", "", "", "patientis deceased per family", True),          # OCR, no space
    ("F", "", "", "", "s/w pt's mother, she passed away last year", False),
    ("F", "", "", "", "pt's husband is deceased, she is the contact", False),
]
_ec_rows = " UNION ALL ".join(
    "SELECT {} AS id, '{}' AS rule, '{}' AS actions, '{}' AS comments, "
    "'{}' AS notes, '{}' AS search, {} AS want".format(
        i, r, a.replace("'", "\\'"), c.replace("'", "\\'"),
        n.replace("'", "\\'"), s.replace("'", "\\'"), str(e).lower())
    for i, (r, a, c, n, s, e) in enumerate(_RULE_EFGI_CASES))
_ec_expr = {
    "E": cleared_plan_sql(norm_sql("actions")),
    "G": cleared_comment_sql(norm_sql("comments"), norm_sql("notes")),
    "I": removed_at_biopsy_sql(norm_sql("search")),
    "F": deceased_sql(norm_sql("search")),
}
_ec_case = "CASE " + " ".join(
    f"WHEN rule = '{k}' THEN {v}" for k, v in _ec_expr.items()) + " END"
_ec = spark.sql(f"SELECT id, rule, actions, comments, notes, search, want, "
                f"{_ec_case} AS got FROM ({_ec_rows})").collect()
_ec_bad = [r for r in _ec if bool(r["got"]) != bool(r["want"])]
assert not _ec_bad, (
    f"RULES E/F/G/I self-check failed on {len(_ec_bad)} of "
    f"{len(_RULE_EFGI_CASES)} cases. A failure on a `want=False` case means a "
    f"TRAP stopped firing -- check CLEARED_PLAN_TRAP (the `unclear` substring), "
    f"CLEARED_COMMENT_TRAP, REMOVED_AT_BIOPSY_TRAP or the pt/patient anchor in "
    f"DECEASED. Failing: "
    + "; ".join(f"[{r['rule']}] A={r['actions']!r} C={r['comments']!r} "
                f"N={r['notes']!r} S={r['search']!r} want={r['want']} got={r['got']}"
                for r in _ec_bad))

# ...and end to end, because a guard being right in isolation is not the same as
# it reaching the column that counts. Case 3 carries an explicit SRT mark and
# must SURVIVE -- all four rules sit inside `OR srt_marked`.
spark.sql("""CREATE OR REPLACE TEMP VIEW _efgi_cases AS
  SELECT 1 AS case_id, 'Basal cell carcinoma, superficial' AS pathology,
         'Please let pt know margins are clear and OK to recheck in 6 mo' AS actions,
         '' AS comments, '' AS notes, '' AS notifications, '' AS flags,
         '' AS extra, '' AS body_site
  UNION ALL SELECT 2, 'Squamous cell carcinoma', '', '',
         'no further tx needed, lesion was removed during biopsy', '', '', '', ''
  UNION ALL SELECT 3, 'Basal cell carcinoma, superficial',
         'Margins are clear, recheck in 6 mo', '',
         'pt interested in SRT, discussed at length', '', '', '', ''
  UNION ALL SELECT 4, 'Basal cell carcinoma, nodular type',
         'Margins unclear - schedule re-excision', '', '', '', '', '', ''
  UNION ALL SELECT 5, 'Squamous cell carcinoma', '', '',
         's/w pt daughter, her mother passed away last year', '', '', '', ''""")
spark.sql(classified_sql("_efgi_cases", "_efgi_out", temp=True))
_ec_map = {r["case_id"]: (bool(r["is_cleared_plan"]), bool(r["is_removed_at_biopsy"]),
                          bool(r["is_deceased"]), bool(r["lesion_appropriate"]))
           for r in spark.table("_efgi_out").collect()}
assert _ec_map[1][0] and not _ec_map[1][3], (
    f"RULE E did not suppress a cleared plan with no treatment: {_ec_map}")
assert _ec_map[2][1] and not _ec_map[2][3], (
    f"RULE I did not suppress a lesion removed at biopsy: {_ec_map}")
assert _ec_map[3][3] is True, (
    f"a RULE E lesion carrying an explicit SRT mark was suppressed -- these rules "
    f"belong INSIDE `OR srt_marked`, as is_cleared is. {_ec_map}")
assert not _ec_map[4][0] and _ec_map[4][3], (
    f"'margins UNCLEAR' was treated as cleared -- the substring trap is off, and "
    f"the rule is now firing on exactly the rows it must protect. {_ec_map}")
assert not _ec_map[5][2] and _ec_map[5][3], (
    f"a RELATIVE's death suppressed a living patient's lesion -- the pt/patient "
    f"anchor in DECEASED is broken. {_ec_map}")
print(f"  RULES E/F/G/I: {len(_RULE_EFGI_CASES)} pattern cases + 5 end-to-end cases pass")


print(
    f"nmsc_vocab loaded — {len(ACCEPTABLE)} acceptable, {len(EXCLUDED)} excluded, "
    f"{len(BENIGN)} benign-sweep terms ({len(BENIGN_BLOCK)} guards); "
    "escaping, lookahead and sweep guard all OK"
)