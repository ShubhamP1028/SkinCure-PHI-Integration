# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "pdfplumber",
#   "pymupdf",
#   "openpyxl",
# ]
# ///
# MAGIC %md
# MAGIC # NMSC Path Log Extraction — Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install dependencies

# COMMAND ----------

# MAGIC %pip install pdfplumber pymupdf openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1c. Verify the install took

# COMMAND ----------

import pdfplumber
import fitz

print("pdfplumber :", pdfplumber.__version__)
print("pymupdf    :", fitz.__doc__.split("\n")[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parameters

# COMMAND ----------

import uuid, io, re, json, glob
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, BooleanType, ArrayType)
import pandas as pd

CONTENT_COL_GUESS = "content"   # the binary column; asserted in section 4

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("schema", "raw", "Schema")
dbutils.widgets.text("source_table", "pathology_monthly_exports", "Lakeflow source table")
dbutils.widgets.text("source_volume", "/Volumes/dev_pathology_data/raw/practice_pathlogs",
                     "Volume path")
dbutils.widgets.dropdown("mode", "binary_table", ["binary_table", "volume_files"], "Source mode")
dbutils.widgets.text("batch_size", "200", "Files per batch (0 = one single batch)")
dbutils.widgets.text("max_minutes", "0", "Stop cleanly after N minutes (0 = no limit)")
dbutils.widgets.text("probe_files", "25", "Files to sample for the scanned estimate")

CATALOG   = dbutils.widgets.get("catalog")
SCHEMA    = dbutils.widgets.get("schema")
SRC_TABLE = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('source_table')}"
SRC_VOL   = dbutils.widgets.get("source_volume")
MODE      = dbutils.widgets.get("mode")
BATCH_SIZE  = int(dbutils.widgets.get("batch_size") or 0)
MAX_MINUTES = float(dbutils.widgets.get("max_minutes") or 0)
PROBE_N     = int(dbutils.widgets.get("probe_files") or 25)

ROWS_TABLE     = f"{CATALOG}.{SCHEMA}.path_log_rows"
MANIFEST_TABLE = f"{CATALOG}.{SCHEMA}.file_manifest"
TODO_TABLE     = f"{CATALOG}.{SCHEMA}._todo_staging"   # one batch at a time
PROBE_TABLE    = f"{CATALOG}.{SCHEMA}._scan_probe"     # the sample from section 6

RUN_ID = str(uuid.uuid4())      # tags this run's rows so the manifest records only
                                # what THIS run processed

print(f"mode     : {MODE}")
print(f"source   : {SRC_VOL if MODE == 'volume_files' else SRC_TABLE}")
print(f"rows     -> {ROWS_TABLE}")
print(f"manifest -> {MANIFEST_TABLE}")
print(f"run id   : {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create catalog objects

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ROWS_TABLE} (
  file_hash      STRING,
  path           STRING,
  account_name   STRING,   -- from the per-account folder in the source path
  file_name      STRING,
  is_scanned     BOOLEAN,
  patient_name   STRING,
  dob            STRING,
  accession      STRING,   -- MRN where the EMA log supplies one
  collected_date STRING,
  result_date    STRING,
  facility       STRING,
  pathology      STRING,
  body_site      STRING,
  -- raw free-text columns, stored verbatim. Interpretation happens in silver.
  actions        STRING,   -- "Actions and Plans" / "Plan"
  notifications  STRING,
  notes          STRING,
  comments       STRING,   -- "Comments / Assigned To"
  flags          STRING,
  provider       STRING,
  page_no        INT,
  layout         STRING,
  run_id         STRING,
  extracted_at   TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
  file_hash      STRING,
  source_path    STRING,
  account_name   STRING,
  file_name      STRING,
  is_scanned     BOOLEAN,
  row_count      INT,
  status         STRING,
  processed_at   TIMESTAMP
) USING DELTA
""")

print("tables ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read the source

# COMMAND ----------

dbutils.widgets.text("expect_min_files", "0", "Fail if fewer files than this (0 = off)")
_MIN = int(dbutils.widgets.get("expect_min_files") or 0)

if MODE == "binary_table":
    # Read the table EAGERLY enough to fail here rather than three cells later. See wiki.
    if not spark.catalog.tableExists(SRC_TABLE):
        print(f"*** {SRC_TABLE} does not exist. Tables in {CATALOG}.{SCHEMA}: ***")
        display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
        raise ValueError(f"{SRC_TABLE} not found -- set the source_table widget")
    src = spark.table(SRC_TABLE)
    print("columns:", src.columns)
    if CONTENT_COL_GUESS not in src.columns:
        raise ValueError(
            f"{SRC_TABLE} has no '{CONTENT_COL_GUESS}' column -- columns are {src.columns}. "
            f"This is probably the connector's metadata table, not the file table.")
else:
    # NOT "*.pdf". Twelve accounts export their whole path log as a spreadsheet, and a
    # glob that admits only PDFs makes those practices vanish rather than read empty.
    src = (spark.read.format("binaryFile")
                .option("pathGlobFilter", "*.{pdf,csv,tsv,xlsx,xlsm,xls}")
                .option("recursiveFileLookup", "true")
                .load(SRC_VOL))

_n_all = src.count()
print("rows in source          :", f"{_n_all:,}")

# Three classes, not two. Lakeflow reports RECORDS processed, which includes folders. See wiki.
_EXT_SHEET = "lower(path) RLIKE '\\.(csv|tsv|xlsx|xlsm|xls)$'"
_EXT_PDF   = "lower(path) LIKE '%.pdf'"

_n_pdf   = src.filter(_EXT_PDF).count()
_n_sheet = src.filter(_EXT_SHEET).count()
_n_other = _n_all - _n_pdf - _n_sheet
print("of which .pdf           :", f"{_n_pdf:,}")
print("of which csv/tsv/xlsx   :", f"{_n_sheet:,}   <- extracted, not dropped")
if _n_sheet:
    display(src.drop(*[c for c in ['content'] if c in src.columns])
               .filter(_EXT_SHEET).limit(100))
if _n_other:
    print(f"  {_n_other:,} row(s) of some OTHER type -- listed, then dropped")
    display(src.drop(*[c for c in ['content'] if c in src.columns])
               .filter(f"NOT ({_EXT_PDF}) AND NOT ({_EXT_SHEET})").limit(50))
src = src.filter(f"({_EXT_PDF}) OR ({_EXT_SHEET})")

# ------------------------------------------------ superseded source paths --------. See wiki.
EXCL = f"{CATALOG}.raw.source_path_exclude"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.raw")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EXCL} (
  path_pattern STRING,   -- RLIKE, matched against the RAW (percent-encoded) path
  note         STRING
) USING DELTA
""")

_ex = [r["path_pattern"] for r in spark.table(EXCL).collect()]
if _ex:
    _before = src.count()
    _hit = {}
    for p in _ex:
        _hit[p] = src.filter(F.col("path").rlike(p)).count()
    src = src.filter(~F.col("path").rlike("|".join(f"({p})" for p in _ex)))
    print(f"source_path_exclude     : {len(_ex)} pattern(s), "
          f"{_before - src.count():,} rows dropped")
    for p, n in _hit.items():
        print(f"    {n:>5,} rows  {p}")
    # A pattern matching nothing is the SEED_POOLED failure shape again: it excludes
    # nothing, says nothing, and someone believes the source is clean because a row
    # exists in a table. Loud, but not fatal -- a pattern can legitimately go stale
    # once a full refresh drops the rows it was written for.
    _dead = [p for p, n in _hit.items() if n == 0]
    if _dead:
        print("*** these exclusion patterns matched ZERO rows -- stale, or a typo: ***")
        for p in _dead:
            print(f"      {p!r}")
else:
    print(f"source_path_exclude     : empty (no superseded paths retired)")

print("distinct folders        :",
      src.selectExpr("url_decode(element_at(split(path, '/'), -2)) AS a")
         .distinct().count())

# NOTE: the byte-identical-file dedup used to sit HERE, and it could not work — it
# groups by `file_hash`, which does not exist until cell 4b derives it two cells down.
# The failure was UNRESOLVED_COLUMN.WITH_SUGGESTION on the first real run. It now lives
# immediately after the hash is computed; do not move it back above 4b.

if _MIN and (_n_pdf + _n_sheet) < _MIN:
    raise ValueError(
        f"only {_n_pdf + _n_sheet:,} path-log files ({_n_pdf:,} PDF + {_n_sheet:,} "
        f"sheet) in {SRC_TABLE}, expected at least {_MIN:,}. "
        f"Wrong source table, an ingest still running, or an over-eager exclude filter. "
        f"Do not extract a partial portfolio -- the manifest will record it as complete.")
if not _MIN:
    print("NOTE: expect_min_files is 0, so the size gate is OFF. Set it to roughly")
    print("      90% of your known file count so a short read fails loudly.")

display(src.drop(*[c for c in ['content'] if c in src.columns]).limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Derive account and content hash

# COMMAND ----------

PATH_COL    = "path"
CONTENT_COL = "content"

# The Lakeflow `path` is a SharePoint URL, so its segments are PERCENT-ENCODED:. See wiki.
_DEC = "url_decode(replace(element_at(split(path, '/'), {n}), '+', '%2B'))"
_seg = lambda n: F.element_at(F.split(F.col(PATH_COL), "/"), n)

src = (src
       .withColumn("file_name",    F.expr(_DEC.format(n=-1)))
       .withColumn("account_name", F.expr(_DEC.format(n=-2)))
       .withColumn("file_hash",    F.sha2(F.col(CONTENT_COL), 256)))

# A practice name that still contains a run of spaces is the signature of a character
# this decode did not protect. Surface it rather than letting it reach account_map.
_runs = src.filter("account_name RLIKE '  '").select("account_name").distinct()
if _runs.count():
    print("*** account_name contains a multi-space run — a character was lost: ***")
    display(_runs)

# Byte-identical files under two paths. The manifest anti-join dedups ACROSS batches,. See wiki.
_dup_rows = (src.groupBy("file_hash").agg(F.count("*").alias("n"),
                                          F.collect_set("path").alias("paths"))
                .filter("n > 1")
                .orderBy(F.col("n").desc())
                .limit(200).collect())
if _dup_rows:
    print(f"*** {len(_dup_rows)} byte-identical file group(s) -- keeping one copy of each ***")
    for r in _dup_rows:
        print(f"    {r['n']} copies: {sorted(r['paths'])}")
    # DEEPEST PATH WINS. This ordering is load-bearing, not cosmetic. See wiki.
    _w = (Window.partitionBy("file_hash")
                .orderBy(F.size(F.split(F.col(PATH_COL), "/")).desc(),
                         F.col(PATH_COL).asc()))
    src = (src.withColumn("_rn", F.row_number().over(_w)).filter("_rn = 1").drop("_rn"))
    print("after hash dedup        :", src.count())
    print("  (kept the DEEPEST path in each group -- a file inside a practice folder")
    print("   beats the same bytes loose at the level above)")
else:
    print("no byte-identical duplicate files in the source")

# Files loose at the archive root instead of inside a practice folder. The pre-flight. See wiki.
_LOOSE = r"^(?i)(phase\d|phase\s*\d|allaccounts|20\d\d-\d\d.*)$"
_loose = (src.filter(F.col("account_name").rlike(_LOOSE))
             .groupBy("account_name")
             .agg(F.count("*").alias("files"),
                  F.collect_set("file_name").alias("file_names")))
if _loose.count():
    print("*** FILES LOOSE AT THE ARCHIVE ROOT — account_name is a folder level, "
          "not a practice ***")
    display(_loose)
    print("Their rows would group under this pseudo-account, map to no AccountId, and")
    print("read as a practice that converts nothing. The `facility` column names the")
    print("real owner.")
    print()
    print("IF YOU HAVE ALREADY TIDIED SHAREPOINT AND STILL SEE THIS: that is expected.")
    print("Lakeflow Connect is append-only and never retracts, so deleting or moving a")
    print("file leaves its row in the ingest table permanently. Which case you are in:")
    print()
    print(f"  SELECT path, _file_metadata.file_modification_time")
    print(f"  FROM {SRC_TABLE}")
    print( "  WHERE path RLIKE '/Phase[0-9]/[^/]+\\\\.pdf$'   -- loose at the root")
    print( "  ORDER BY path")
    print()
    print("  MOVED  -> the same filename also appears one level deeper. Nothing to do:")
    print("            the hash dedup above keeps the deeper copy. If you are seeing")
    print("            this anyway, the two copies are NOT byte-identical -- the")
    print("            practice re-exported rather than moved, so retire the old path.")
    print("  DELETED-> the loose path is alone. Retire it:")
    print()
    print(f"     INSERT INTO {EXCL} VALUES")
    print( "       ('/Phase1/[^/]+\\\\.pdf$', 'Boone files moved into their own folder, 21 Aug');")
    print()
    print("Then re-run this notebook. Do NOT delete from the ingest table -- the")
    print("Lakeflow pipeline owns it and rewriting it risks its checkpoint.")
    raise ValueError(
        "loose files at the archive root: account_name would be a folder level. "
        "Fix the source layout or retire the superseded paths in "
        f"{EXCL} -- do not extract blind.")

display(src.select("account_name", "file_name", "length", "file_hash"))

# Any %XX left after decoding means a double-encoded path — surface it rather than
# letting it reach account_map.
_still = src.filter("account_name RLIKE '%[0-9A-Fa-f]{2}'").select("account_name").distinct()
if _still.count():
    print("*** account_name still percent-encoded after url_decode: ***")
    display(_still)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Manifest — skip files already processed

# COMMAND ----------

def remaining_files():
    """Files with no terminal manifest entry yet. Re-read every batch, so a resumed
    run picks up exactly where the last one stopped.

    EMPTY counts as terminal alongside OK. A file that legitimately parses to zero
    rows never becomes OK, so if EMPTY were retryable the batch loop would select
    the same files forever and never terminate."""
    done = (spark.table(MANIFEST_TABLE)
                 .filter("status IN ('OK', 'EMPTY')")
                 .select("file_hash"))
    return src.join(done, on="file_hash", how="left_anti")

manifest  = spark.table(MANIFEST_TABLE)
_n_src    = src.count()
_n_done   = manifest.filter("status IN ('OK','EMPTY')").count()
_n_todo   = remaining_files().count()

print(f"files in source : {_n_src:,}")
print(f"already done    : {_n_done:,}  (OK + EMPTY)")
print(f"to process      : {_n_todo:,}")
if BATCH_SIZE:
    import math
    print(f"batches         : {math.ceil(_n_todo / BATCH_SIZE)} x {BATCH_SIZE} files")
else:
    print("batches         : 1 (batching OFF -- a failure loses the whole run)")
print(f"\nIf 'to process' is 0 but you expected files, reset with:")
print(f"  spark.sql('TRUNCATE TABLE {MANIFEST_TABLE}')")
print("Never truncate the manifest without also truncating path_log_rows -- a manifest")
print("that outlives its rows is the one state that silently drops files.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Duplicate detection

# COMMAND ----------

display(src.groupBy("file_hash")
           .agg(F.count("*").alias("n"), F.collect_set(PATH_COL).alias("paths"))
           .filter("n > 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Split text-layer vs scanned

# COMMAND ----------

@pandas_udf(BooleanType())
def is_scanned(contents: pd.Series) -> pd.Series:
    """True if the PDF has no meaningful text layer on its first few pages.

    The import sits OUTSIDE the per-row function deliberately. If pdfplumber is
    missing on the executors this must fail loudly — swallowing the ImportError
    would mark every file scanned, route everything to OCR, and produce zero rows
    with no error. A missing library disguised as a data characteristic.
    """
    import pdfplumber

    def one(content):
        if content is None:
            return True
        try:
            with pdfplumber.open(io.BytesIO(bytes(content))) as pdf:
                for page in pdf.pages[:3]:
                    if (page.extract_text() or "").strip():
                        return False
            return True
        except Exception:
            return True        # genuinely unreadable -> send to OCR

    return contents.apply(one)


# This used to classify EVERY file up front, which opened all of them once here and
# again in section 9. Now it samples: the split is a planning number, and the real
# classification happens inside each batch where it is used immediately.
probe = (remaining_files().orderBy("file_hash").limit(PROBE_N)
         # A sheet is never "scanned" -- and must not reach is_scanned(), which opens
         # the bytes with pdfplumber. Decided from the file name, before the UDF.
         .withColumn("is_sheet", F.expr(
             "lower(file_name) RLIKE '\\.(csv|tsv|xlsx|xlsm|xls)$'"))
         .withColumn("is_scanned", F.when(F.col("is_sheet"), F.lit(False))
                                    .otherwise(is_scanned(F.col(CONTENT_COL)))))

(probe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PROBE_TABLE))
probe = spark.table(PROBE_TABLE)

_pn = probe.count()
_ps = probe.filter("is_scanned").count()
print(f"sampled {_pn} of {_n_todo:,} files: {_ps} scanned, {_pn - _ps} with a text layer")
if _pn:
    print(f"extrapolated scanned across the portfolio: ~{round(_ps / _pn * _n_todo):,}")
    print("A SAMPLE, not a census -- scanned files cluster by account, so the true")
    print("figure moves with which accounts the sample happened to land on.")
display(probe.groupBy("account_name", "is_scanned").count().orderBy("account_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Parser

# COMMAND ----------

LESION_ROW = StructType([
    StructField("patient_name",   StringType()),
    StructField("dob",            StringType()),
    StructField("accession",      StringType()),
    StructField("collected_date", StringType()),
    StructField("result_date",    StringType()),
    StructField("facility",       StringType()),
    StructField("pathology",      StringType()),
    StructField("body_site",      StringType()),
    # Raw free text — stored verbatim, never interpreted here.
    StructField("actions",        StringType()),
    StructField("notifications",  StringType()),
    StructField("notes",          StringType()),
    StructField("comments",       StringType()),
    StructField("flags",          StringType()),
    StructField("provider",       StringType()),
    StructField("page_no",        IntegerType()),
    StructField("layout",         StringType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a. Normalisers

# COMMAND ----------

_MONTHS = dict(JAN=1, FEB=2, MAR=3, APR=4, MAY=5, JUN=6,
               JUL=7, AUG=8, SEP=9, OCT=10, NOV=11, DEC=12)


def _clean(s):
    if s is None:
        return None
    return re.sub(r"\s+", " ", str(s).replace("\n", " ")).strip() or None


def _unwrap_token(s):
    """Repair wrapping inside a table cell: '05/04/2 026', 'BP-26-1004 1',
    'Intereste d'. Used for names and accessions — NOT dates, which have their
    own handling below."""
    if not s:
        return s
    s = _clean(s)
    s = re.sub(r"(?<=[\d/\-])\s+(?=\d)", "", s)
    s = re.sub(r"(?<=[a-z])\s+([a-z]{1,2})\b", r"\1", s)
    return s


_DATE_SLASH = re.compile(
    r"\b(\d{1,2})\s*[/-]\s*(\d{1,2})\s*[/-]\s*(\d{1,4})(?:\s+(\d{1,3}))?\b")
# 2- OR 4-digit year: 02_Methodology 2.5 records "4-Mar-26" alongside "3/2/2026".
_DATE_MON = re.compile(r"\b(\d{1,2})\s*-\s*([A-Za-z]{3})[a-z]*\s*-\s*(\d{2,4})\b")
# ISO, optionally followed by a time -- what openpyxl's datetime str()s to.
_DATE_ISO = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")


# Real short words that must NEVER be glued to the word before them. Without this,
# "Excision vs Superficial" becomes "Excisionvs" and "verification of benefits"
# becomes "verificationof" — both of which then fail to match downstream.
_SHORT_WORDS = {
    "a", "i", "an", "as", "at", "be", "by", "do", "go", "he", "if", "in", "is", "it",
    "me", "my", "no", "of", "on", "or", "so", "to", "up", "us", "vs", "we", "am", "pm",
}


def _unwrap_words(s):
    """Repair word-wrapping in free text: 'Intereste d' -> 'Interested',
    'CANCER OUS' -> 'CANCEROUS'.

    A cell reading 'Intereste d' is corrupt, not raw — storing it that way breaks
    every downstream regex. This is extraction correctness, not interpretation.

    Two conservative rules only:
      1. a 1-2 letter fragment that isn't a real short word
      2. an ALL-CAPS fragment following an ALL-CAPS word (wrap artifacts, near always)

    Deliberately NOT the digit-joining rule from _unwrap_token — free text holds dates
    ('VOB submitted 6/2') that must not fuse with adjacent numbers.
    """
    if not s:
        return s
    s = _clean(s)

    def frag(m):
        f = m.group(1)
        return m.group(0) if f.lower() in _SHORT_WORDS else f

    # Rule 1 — lowercase fragments only. An UPPERCASE 1-2 letter token is almost
    # always an acronym ("IG-SRT"), not a wrap remnant; gluing it produces
    # "DiscussedIG-SRT" and corrupts text we promised to store verbatim.
    s = re.sub(r"(?<=[A-Za-z])\s+([a-z]{1,2})\b", frag, s)
    # Rule 2 — an ALL-CAPS fragment after an ALL-CAPS word ("CANCER OUS").
    s = re.sub(r"(?<=[A-Z])\s+([A-Z]{2,4})\b", r"\1", s)
    return s


def norm_date(s):
    """ISO yyyy-mm-dd or None. Handles MM/DD/YYYY, M/D/YY, DD-MON-YYYY, and a year
    split across a line wrap ('07/09/202 6').

    Dates get their own pattern rather than going through _unwrap_token because EMA
    puts a time immediately after the date ('07/16/2026 12:24 PM'). The generic
    digit-joiner would fuse those into '07/16/202612:24'.
    """
    if not s:
        return None
    s = _clean(s)
    m = _DATE_SLASH.search(s)
    if m:
        mo, d, y3, y4 = m.group(1), m.group(2), m.group(3), m.group(4)
        y = y3 if len(y3) >= 4 else (y3 + (y4 or ""))
        try:
            y, mo, d = int(y), int(mo), int(d)
        except ValueError:
            return None
        if y < 100:
            y += 2000 if y < 70 else 1900      # '26' -> 2026
        if not (1 <= mo <= 12 and 1 <= d <= 31):
            return None
        return f"{y:04d}-{mo:02d}-{d:02d}"
    m = _DATE_MON.search(s)
    if m:
        d, mon, y = m.group(1), m.group(2).upper(), m.group(3)
        if mon in _MONTHS:
            yi = int(y)
            if yi < 100:                     # 26 -> 2026, matching the slash branch
                yi += 2000 if yi < 70 else 1900
            return f"{yi:04d}-{_MONTHS[mon]:02d}-{int(d):02d}"
    # ALREADY ISO, possibly with a time. openpyxl hands back a real datetime for an. See wiki.
    m = _DATE_ISO.search(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


# EMA renders action links inside the table cell, so their labels come through the text. See wiki.
_UI_CHROME = [
    r"View Anticipated Plan",
    r"View Plan",
    r"Anticipated Plan",
    r"View Details",
    r"View Result",
    r"View Report",
    # Measured on the real 103,419-row corpus, 21 Aug: 'View More' appears 2,046x in. See wiki.
    r"View More",
    r"View Less",
    r"View All",
    r"Show More",
]
_UI_TAIL = re.compile(r"(?:\s*(?:" + "|".join(_UI_CHROME) + r"))+\s*$", re.I)


# `comments` maps to EMA's "Comments / Assigned To" column, and its Assign button label
# lands in the cell as data on 18,677 rows -- 18% of the corpus. It carries no SRT or VOB
# token so it creates no false marks; it is noise, not a defect. Stripped only when it is
# the WHOLE cell, so "no comment" stays distinguishable from "a comment we could not read".
_UI_ONLY = re.compile(r"^\s*(?:assign(?:ed)?|view\s+more|view\s+all|show\s+more)\s*$", re.I)


def strip_ui_chrome(s):
    """Remove trailing EMA link labels from a cell value."""
    if not s:
        return s
    if _UI_ONLY.match(str(s)):
        return None
    out = _UI_TAIL.sub("", str(s)).strip(" -\u2013\u2014,;")
    # never return empty where there was content — that would look like a missing value
    return out if out else str(s).strip()


def sanitize(s):
    """Neutralise formula injection. Leading = + @ - are evaluated by Excel and
    LibreOffice — that is how an OCR artifact becomes #NAME? in Pam's Summary."""
    s = _clean(s)
    s = strip_ui_chrome(s)
    return "'" + s if s and s[0] in "=+@-" else s


_MRN = re.compile(r"\bMRN\s*[:#]?\s*([A-Za-z0-9\-]+)", re.I)
_PT_TAIL = re.compile(r"\s*\b(MRN|DOB|Phone|Tel|Ph)\b\s*[:#]?.*$", re.I | re.S)
# [26 Aug] the unlabelled EMA 'Patient Info' parenthetical:
#     Salazar, Maria (F, 61 | May 12, 1965 | SALO0001)
# No nested parens, at least one pipe, anchored to the end of the cell.
_PT_PAREN = re.compile(r"\s*\(([^()]*\|[^()]*)\)\s*$")
# An MRN is alphanumeric with no spaces. Deliberately NOT anchored to a digit
# count -- prefixes run 2-6 letters and numbers 3-6 digits across practices.
_MRN_SHAPE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-]{2,19}")


def split_patient(cell):
    """EMA packs name, MRN and phone into one cell. Two layouts, both handled.

    LABELLED, the original:
      'Massey, Thomas MRN: MM0000141844 Phone: (310) 881-9454'
        -> ('Massey, Thomas', 'MM0000141844')

    PARENTHETICAL, the 'Patient Info' column [26 Aug]:
      'Salazar, Maria (F, 61 | May 12, 1965 | SALO0001)'
        -> ('Salazar, Maria', 'SALO0001')

    The MRN is worth keeping: it's a far stronger join key for VOB matching than a
    fuzzy name, and EMA hands it over for free.

    WHY THE SECOND LAYOUT MATTERS. It carries no `MRN:` or `DOB:` label, so
    neither `_MRN` nor `_PT_TAIL` fired and the WHOLE STRING became the patient
    name -- which then became `patient_id`, because these accounts publish no
    separate accession. Three consequences, measured on the Mar-Jun corpus over
    2,096 patient-days at 9 accounts (Cincinnati 557, Wichita Falls 531,
    Caledonia 344, Germantown 194, Robare Cypress 190, Simi Valley 150,
    Beachwood 47, Fairview 42, Petoskey 41):

      1. A DATE OF BIRTH SAT IN A KEY COLUMN, and was exported to CSV and the
         workbook. That alone is reason enough.
      2. The real MRN was discarded while it sat inside the string. These
         accounts fell back to name-keying when the strongest possible key was
         already in hand.
      3. Tokenising the name for VOB matching injected two junk tokens per row --
         a month name from the DOB and the MRN's alpha prefix. Five month names
         are also real given names (April, August, June, March, May), so a
         patient actually called June could match any patient with a June
         birthday at that practice. Measured: 0 such collisions today, and one
         MRN prefix colliding with a real surname. Latent, not active -- but the
         mechanism is one unlucky patient away from a silent wrong attribution.

    Tolerant on purpose: the separator layout varies ('F, 61' and 'F 56' both
    occur), so this matches ANY trailing parenthetical containing a pipe and
    takes the last segment as the MRN when it looks like one. If it does not look
    like an MRN the parenthetical is still stripped from the name -- getting the
    DOB out of the key does not depend on recognising the identifier.
    """
    if not cell:
        return None, None
    c = _clean(cell)
    m = _MRN.search(c)
    mrn = m.group(1) if m else None
    name = _PT_TAIL.sub("", c).strip(" ,;")

    if mrn is None:
        pm = _PT_PAREN.search(name)
        if pm:
            parts = [p.strip() for p in pm.group(1).split("|")]
            tail = parts[-1] if parts else ""
            if _MRN_SHAPE.fullmatch(tail):
                mrn = tail
            # strip regardless of whether the tail parsed as an MRN
            name = name[: pm.start()].strip(" ,;")

    return (name or None), mrn


# Regression for both layouts. The parenthetical cases are the 26 Aug addition;
# the labelled one and the two negatives are what must NOT change.
for _in, _wn, _wm in [
    ("Massey, Thomas MRN: MM0000141844 Phone: (310) 881-9454",
                                                  "Massey, Thomas", "MM0000141844"),
    ("Salazar, Maria (F, 61 | May 12, 1965 | SALO0001)", "Salazar, Maria", "SALO0001"),
    ("Doe, Jane (F 56 | June 17, 1970 | DABA0008)",      "Doe, Jane",      "DABA0008"),
    ("O'Brien, Sean (M, 74 | Jan 2, 1952 | OBRI0012)",   "O'Brien, Sean",  "OBRI0012"),
    ("Lee, Ann (F, 40 | Mar 3, 1986 |)",                 "Lee, Ann",       None),
    # a parenthetical with NO pipe is a real part of the name -- never strip it
    ("Smith, John (Jr)",                                 "Smith, John (Jr)", None),
    ("Wood, Mary",                                       "Wood, Mary",       None),
]:
    _gn, _gm = split_patient(_in)
    assert (_gn, _gm) == (_wn, _wm), (
        f"split_patient({_in!r}) -> ({_gn!r}, {_gm!r}), expected ({_wn!r}, {_wm!r})")
print("split_patient: 7/7 regression cases pass (labelled + parenthetical layouts)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. Header aliases — the extension point

# COMMAND ----------

_ALIASES = {
    # --- provider
    "performed by": "provider", "provider": "provider", "physician": "provider",
    "clinician": "provider", "reviewed by": "provider",
    # --- extra clinical free text worth searching for an SRT signal
    "differential diagnoses": "comments", "differential diagnosis": "comments",
    "follow up(s)": "notifications", "follow ups": "notifications",
    "follow up": "notifications",
    # --- accession / specimen id
    "accession": "accession", "accession no": "accession", "accession no.": "accession",
    "specimen id": "accession", "case": "accession", "case no": "accession",
    "mrn": "accession",
    # Added 21 Aug: the sheet-parser probe found "Accession #" and "MRN #" mapping to
    # nothing, which silently emptied the patient key for a whole spreadsheet export.
    # The '#' forms are common in CSV headers and cost nothing on the PDF path.
    "accession number": "accession",   # EzDerm Biopsy Log
    "accession #": "accession", "accession#": "accession", "acc #": "accession",
    "mrn #": "accession", "mrn#": "accession", "medical record number": "accession",
    "patient id": "accession", "chart #": "accession", "chart no": "accession",
    # --- patient
    "patient": "patient_name", "patient name": "patient_name", "name": "patient_name",
    # EzDerm's "Biopsy Log" packs name + DOB + MRN into one cell, the way EMA packs
    # name + MRN + phone. split_patient() already unpacks that shape.
    "patient info": "patient_name",
    # --- dob (absent from EMA logs — expect this to stay null there)
    "dob": "dob", "date of birth": "dob", "birthdate": "dob",
    # --- collected / biopsy date
    "collected": "collected_date", "date collected": "collected_date",
    "collection date": "collected_date", "biopsy date": "collected_date",
    "date": "collected_date", "date of service": "collected_date",
    "service date": "collected_date", "dos": "collected_date",
    "encounter date": "collected_date",
    # --- result date
    "resulted": "result_date", "date reported": "result_date",
    "reported": "result_date", "result date": "result_date", "final": "result_date",
    "results processed": "result_date", "signed off": "result_date",
    "signed": "result_date", "finalized": "result_date", "final date": "result_date",
    # --- pathology / diagnosis
    "diagnosis": "pathology", "dx": "pathology", "pathology": "pathology",
    "final diagnosis": "pathology", "result": "pathology", "path result": "pathology",
    # --- body site
    # NOTE: "location" and "site" mean the LESION here, never the clinic. Mapping them
    # to facility fills the facility column with body parts and makes the facility-rule
    # decision meaningless.
    "body site": "body_site", "specimen": "body_site", "lesion site": "body_site",
    "location": "body_site", "site": "body_site",
    "biopsy location": "body_site",    # EzDerm — the LESION, not the clinic
    # --- actions / plan (where the SRT signal lives in EMA)
    "actions and plans": "actions", "actions": "actions", "plan": "actions",
    "plan of care": "actions", "treatment plan": "actions", "disposition": "actions",
    "srt status": "actions", "status": "actions",
    # EzDerm keeps the plan in "To Do(s)" -- up to 2,700 characters, and where its SRT
    # signal lives. It maps to `actions` ALONGSIDE "Status", which is why the sheet
    # parser concatenates rather than overwrites when two columns share a target.
    "to do(s)": "actions", "to dos": "actions", "to do": "actions",
    # --- facility: explicit words only
    "facility": "facility", "clinic": "facility", "practice": "facility",
    "office": "facility",
    # --- raw free text, kept verbatim
    "notes": "notes", "note": "notes",
    "notifications": "notifications", "notification": "notifications",
    "comments": "comments", "comment": "comments",
    "comments / assigned to": "comments", "comments/assigned to": "comments",
    "assigned to": "comments",
    "provider": "provider", "physician": "provider", "pathologist": "provider",
    "flags": "flags", "flag": "flags",
}

# The EMA grid rebuild can split a narrow header mid-word ("Commen ts / Assi gned To",
# "Notificati ons"). Matching on a whitespace-stripped key rescues those without
# needing an alias entry per mangling.
_ALIASES_NOSPACE = {re.sub(r"\s+", "", k): v for k, v in _ALIASES.items()}

# A THIRD key, stripped of separators as well as whitespace. Machine-generated. See wiki.
_SEPS = r"[\s_\-./#]+"
_ALIASES_NOSEP = {re.sub(_SEPS, "", k): v for k, v in _ALIASES.items()}


def _alias(label):
    key = (_clean(label) or "").lower().rstrip(":")
    return (_ALIASES.get(key)
            or _ALIASES_NOSPACE.get(re.sub(r"\s+", "", key))
            or _ALIASES_NOSEP.get(re.sub(_SEPS, "", key)))


def map_headers(header_row):
    return [_alias(c) for c in header_row]


def looks_like_header(row):
    mapped = map_headers(row)
    return sum(1 for m in mapped if m) >= 3 and "patient_name" in mapped


def facility_from_header(text):
    """Single-site exports name the facility in the page header:
    'Dermatopathology Results Log | BrightPath Dermatology - Main Campus'"""
    if not text:
        return None
    for line in text.split("\n")[:6]:
        if "|" in line and re.search(r"log|report|result", line, re.I):
            cand = _clean(line.split("|")[-1])
            if cand and not re.search(r"multi-?site|\d+\s+facilit", cand, re.I):
                return cand
    return None


def _blank_row():
    return dict(patient_name=None, dob=None, accession=None, collected_date=None,
                result_date=None, facility=None, pathology=None, body_site=None,
                actions=None, notifications=None, notes=None, comments=None,
                flags=None, provider=None, page_no=None, layout=None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c. Rebuild a grid from horizontal rules (EMA)

# COMMAND ----------

PAD = 2.0


def _h_lines(page, tol=1.0):
    """y positions of full-width horizontal rules."""
    ys = set()
    for ln in list(page.lines) + list(page.rects):
        top, bot = ln.get("top"), ln.get("bottom")
        if top is None or bot is None:
            continue
        if abs(top - bot) <= tol and (ln.get("x1", 0) - ln.get("x0", 0)) > page.width * 0.3:
            ys.add(round(top, 1))
    return sorted(ys)


def _cluster_columns(words, page_width, min_gap=6.0):
    """Header words -> column x-ranges. A gap wider than min_gap starts a new column."""
    if not words:
        return []
    ws = sorted(words, key=lambda w: w["x0"])
    groups, cur = [], [ws[0]]
    for w in ws[1:]:
        if w["x0"] - max(x["x1"] for x in cur) > min_gap:
            groups.append(cur)
            cur = [w]
        else:
            cur.append(w)
    groups.append(cur)

    cols = [{"x0": min(w["x0"] for w in g),
             "x1": max(w["x1"] for w in g),
             "label": " ".join(w["text"] for w in sorted(g, key=lambda w: (w["top"], w["x0"])))}
            for g in groups]

    # Column edges come from header LEFT edges, not the midpoint of the header gap.
    # Cells are left-aligned under their header, so a column runs from its own x0 to
    # the next header's x0. Midpoints put the boundary too far left whenever a short
    # header ("Result") sits above a wide column, and wrapped text bleeds sideways.
    for i, c in enumerate(cols):
        c["lo"] = 0 if i == 0 else c["x0"] - PAD
        c["hi"] = page_width + 50 if i == len(cols) - 1 else cols[i + 1]["x0"] - PAD
    return cols


def extract_ruled_rows(page, min_gap=6.0, header_window=26.0):
    """Rows of cells from a table with horizontal rules only. Same shape as
    extract_table(), so it feeds the same row parser."""
    ys = _h_lines(page)
    if len(ys) < 2:
        return []
    words = page.extract_words(keep_blank_chars=False, use_text_flow=False)
    if not words:
        return []

    # Header sits above the first rule — but so does any report title. Keep only the
    # block immediately above it, or a title higher up merges columns together.
    above = [w for w in words if w["bottom"] <= ys[0] + 1]
    if not above:
        return []
    floor = max(w["bottom"] for w in above) - header_window
    cols = _cluster_columns([w for w in above if w["bottom"] >= floor], page.width, min_gap)
    if len(cols) < 3:
        return []

    rows = [[c["label"] for c in cols]]
    for top, bot in list(zip(ys, ys[1:])) + [(ys[-1], page.height)]:
        band = [w for w in words if w["top"] >= top - 1 and w["bottom"] <= bot + 1]
        if not band:
            continue
        cells = [[] for _ in cols]
        for w in sorted(band, key=lambda w: (round(w["top"], 1), w["x0"])):
            for i, c in enumerate(cols):
                if c["lo"] <= w["x0"] < c["hi"]:     # left edge, matches left-aligned text
                    cells[i].append(w["text"])
                    break
        row = [" ".join(c).strip() for c in cells]
        if any(row):
            rows.append(row)
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7d. Row parsers

# COMMAND ----------

def parse_rows(table, page_no, default_facility=None, layout="TABLE"):
    """table: list of rows, each a list of cell strings."""
    out = []
    if not table or len(table) < 2:
        return out
    hdr_idx = next((i for i, r in enumerate(table[:3]) if looks_like_header(r)), None)
    if hdr_idx is None:
        return out
    fields = map_headers(table[hdr_idx])
    has_fac_col = "facility" in fields

    for raw in table[hdr_idx + 1:]:
        if not any(_clean(c) for c in raw):
            continue
        rec = {}
        for i, c in enumerate(raw):
            if i < len(fields) and fields[i]:
                rec[fields[i]] = _clean(c)
        if not rec.get("patient_name"):
            continue

        name, mrn = split_patient(rec.get("patient_name"))
        if not name:
            continue
        actions = rec.get("actions")

        r = _blank_row()
        r["patient_name"]   = sanitize(_unwrap_token(name))
        r["accession"]      = mrn or _unwrap_token(rec.get("accession"))
        r["dob"]            = norm_date(rec.get("dob"))
        r["collected_date"] = norm_date(rec.get("collected_date"))
        r["result_date"]    = norm_date(rec.get("result_date"))
        # If the export HAS a Facility column, trust it even when the cell is blank,
        # so the gap surfaces in FlagsReview rather than being silently backfilled
        # from the page header. Inherit the header only for single-site exports.
        r["facility"]       = _clean(rec.get("facility")) if has_fac_col else default_facility
        r["pathology"]      = sanitize(rec.get("pathology"))
        r["body_site"]      = sanitize(rec.get("body_site"))
        # Raw free text, verbatim. No interpretation in bronze — see the silver view
        # in section 11 for how srt_marked / vob_mentioned get derived from these.
        r["actions"]        = sanitize(_unwrap_words(actions))
        r["notifications"]  = sanitize(_unwrap_words(rec.get("notifications")))
        r["notes"]          = sanitize(_unwrap_words(rec.get("notes")))
        r["comments"]       = sanitize(_unwrap_words(rec.get("comments")))
        r["flags"]          = sanitize(_unwrap_words(rec.get("flags")))
        r["provider"]       = sanitize(_unwrap_words(rec.get("provider")))
        r["page_no"], r["layout"] = page_no, layout
        out.append(r)
    return out


_BLOCK_SPLIT = re.compile(r"(?=^\s*Accession\s+No\.?\s)", re.I | re.M)


def parse_narrative(text, page_no, default_facility=None):
    rows = []
    if not text or not re.search(r"Accession\s+No\.?", text, re.I):
        return rows
    for block in _BLOCK_SPLIT.split(text):
        if not re.search(r"Accession\s+No\.?", block, re.I):
            continue

        def grab(pat, flags=re.I):
            m = re.search(pat, block, flags)
            return _clean(m.group(1)) if m else None

        name = grab(r"Patient:\s*(.+?)\s*(?:DOB:|$)")
        if not name:
            continue
        dx  = grab(r"DIAGNOSIS:\s*(.+?)(?=\n\s*(?:Comment|Accession)\b|\Z)", re.I | re.S)
        cmt = grab(r"Comment:\s*(.+?)(?=\n\s*Accession\b|\Z)", re.I | re.S)

        r = _blank_row()
        r["patient_name"]   = sanitize(name)
        r["accession"]      = grab(r"Accession\s+No\.?\s*([A-Za-z0-9\-]+)")
        r["dob"]            = norm_date(grab(r"DOB:\s*([0-9A-Za-z/\-]+)"))
        r["collected_date"] = norm_date(grab(r"Date\s+Collected:\s*([0-9A-Za-z/\-]+)"))
        r["result_date"]    = norm_date(grab(r"Date\s+Reported:\s*([0-9A-Za-z/\-]+)"))
        r["facility"]       = default_facility
        r["pathology"]      = sanitize(dx)
        r["body_site"]      = sanitize(grab(r"Specimen:\s*(.+?)(?=\n|$)"))
        r["actions"]        = sanitize(cmt)     # the Comment block carries the plan
        r["page_no"], r["layout"] = page_no, "NARRATIVE"
        rows.append(r)
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7e. Layout dispatch

# COMMAND ----------

def parse_page(page, page_no, default_facility=None):
    text = page.extract_text() or ""
    fac = default_facility or facility_from_header(text)

    # 1. EMA-style — horizontal rules only
    try:
        rows = parse_rows(extract_ruled_rows(page), page_no, fac, layout="EMA")
        if rows:
            return rows
    except Exception:
        pass

    # 2. Fully ruled table
    out = []
    try:
        for tbl in (page.extract_tables() or []):
            out.extend(parse_rows(tbl, page_no, fac, layout="TABLE"))
    except Exception:
        pass
    if out:
        return out

    # 3. Narrative blocks
    return parse_narrative(text, page_no, fac)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7f. Driver-side smoke test

# COMMAND ----------

if MODE == "volume_files":
    _files = sorted(glob.glob(f"{SRC_VOL}/**/*.pdf", recursive=True))
    print(f"{len(_files)} local PDFs\n")
    for _f in _files:
        try:
            with pdfplumber.open(_f) as _pdf:
                _rows = [r for i, pg in enumerate(_pdf.pages, 1) for r in parse_page(pg, i)]
            _lay = {r["layout"] for r in _rows} or {"-"}
            print(f"  {_f.split('/')[-1][:46]:48s} {len(_rows):3d} rows  {sorted(_lay)}")
        except Exception as e:
            print(f"  {_f.split('/')[-1][:46]:48s} ERROR {type(e).__name__}: {e}")
else:
    print("binary_table mode — no local files to probe.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7g. Text-layer UDF

# COMMAND ----------

@F.udf(returnType=ArrayType(LESION_ROW))
def extract_text_layer(content):
    if content is None:
        return []
    import pdfplumber
    out = []
    try:
        with pdfplumber.open(io.BytesIO(bytes(content))) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                out.extend(parse_page(page, i))
    except Exception as e:
        out.append({**_blank_row(),
                    "pathology": f"__EXTRACT_ERROR__ {type(e).__name__}: {e}",
                    "page_no": -1, "layout": "ERROR"})
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7f. CSV and XLSX path logs

# COMMAND ----------

_SHEET_EXT = (".csv", ".tsv", ".xlsx", ".xlsm", ".xls")

# "(01/02/1950)" or "(1950-01-02)" trailing a name. Anchored on a DATE inside the
# parentheses, not any parenthetical -- "Smith, John (Jr)" and a trading name in
# brackets must survive untouched.
_DOB_PARENS = re.compile(r"\s*\((\d{1,4}[/-]\d{1,2}[/-]\d{1,4})\)\s*")


def _sheet_rows(content, file_name):
    """Parse one CSV/TSV/XLSX path log into LESION_ROW dicts.

    Header row is FOUND, not assumed to be row 1: these exports often carry a title
    line, a practice name and a date range above the grid. `looks_like_header` is the
    same test the PDF parser uses — 3+ recognised columns AND a patient column.
    """
    import pandas as pd
    import io as _io
    name = (file_name or "").lower()
    try:
        if name.endswith((".csv", ".tsv")):
            # NOT pandas. `read_csv(header=None)` infers the column count from the FIRST. See wiki.
            import csv as _csv
            sep = "\t" if name.endswith(".tsv") else ","
            text = bytes(content).decode("utf-8-sig", errors="replace")
            lines = list(_csv.reader(text.splitlines(), delimiter=sep))
            if not lines:
                return []
            width = max(len(r) for r in lines)
            raw = pd.DataFrame([r + [""] * (width - len(r)) for r in lines], dtype=str)
        else:
            raw = pd.read_excel(_io.BytesIO(bytes(content)), header=None, dtype=str)
            raw = raw.fillna("")
    except Exception as e:
        return [{**_blank_row(), "pathology": f"__EXTRACT_ERROR__ {type(e).__name__}: {e}",
                 "page_no": -1, "layout": "ERROR"}]

    if raw.empty:
        return []

    hdr_idx, mapped = None, None
    for i in range(min(len(raw), 25)):          # title blocks are short; 25 is generous
        row = [str(x) for x in raw.iloc[i].tolist()]
        if looks_like_header(row):
            hdr_idx, mapped = i, map_headers(row)
            break
    if hdr_idx is None:
        # AN ERROR THAT CANNOT BE DIAGNOSED FROM ITS OWN TEXT COSTS A ROUND TRIP. See wiki.
        best_i, best_n, best_map = None, 0, None
        for i in range(min(len(raw), 25)):
            row = [str(x) for x in raw.iloc[i].tolist()]
            mp = map_headers(row)
            n = sum(1 for m in mp if m)
            if n > best_n:
                best_i, best_n, best_map = i, n, mp
        shapes = [len([c for c in raw.iloc[i].tolist() if str(c).strip()])
                  for i in range(min(len(raw), 8))]
        if best_i is not None:
            cells = [str(x)[:30] for x in raw.iloc[best_i].tolist()]
            unmapped = [c for c, m in zip(cells, best_map) if not m and c.strip()]
            detail = (f"best line {best_i} mapped {best_n} column(s) "
                      f"{[m for m in best_map if m]}; UNMAPPED {unmapped[:12]}; "
                      f"needs 3+ mapped AND one resolving to patient_name")
        else:
            detail = (f"no line in the first 25 had even ONE recognisable column; "
                      f"non-empty field counts per line: {shapes}")
        return [{**_blank_row(),
                 "pathology": f"__EXTRACT_ERROR__ no header row found — {detail}",
                 "page_no": -1, "layout": "ERROR"}]

    out = []
    for i in range(hdr_idx + 1, len(raw)):
        cells = [str(x) for x in raw.iloc[i].tolist()]
        rec = _blank_row()
        for col, val in zip(mapped, cells):
            if col and val is not None:
                v = _clean(val)
                if v:
                    # CONCATENATE, do not overwrite. Two source columns can share one. See wiki.
                    rec[col] = f"{rec[col]} | {v}" if rec.get(col) else v

        # A composite patient cell -- EMA writes "Massey, Thomas MRN: MM123 Phone: ...",
        # EzDerm writes name + DOB + MRN. split_patient() unpacks both, and an MRN is a
        # far stronger join key than a fuzzy name, so take it when the accession column
        # did not already supply one.
        if rec.get("patient_name"):
            _nm, _mrn = split_patient(rec["patient_name"])
            if _nm:
                rec["patient_name"] = _nm
            if _mrn and not rec.get("accession"):
                rec["accession"] = _mrn

            # A PARENTHESISED DOB, which split_patient does not strip: its _PT_TAIL. See wiki.
            _dob = _DOB_PARENS.search(rec["patient_name"] or "")
            if _dob:
                if not rec.get("dob"):
                    rec["dob"] = _dob.group(1)
                rec["patient_name"] = _DOB_PARENS.sub("", rec["patient_name"]).strip(" ,;")
        # NORMALISE THE DATES, exactly as both PDF parsers do. §7f shipped without. See wiki.
        for _dcol in ("collected_date", "result_date", "dob"):
            if rec.get(_dcol):
                rec[_dcol] = norm_date(rec[_dcol]) or rec[_dcol]

        if not any(rec[k] for k in ("patient_name", "accession", "pathology",
                                    "collected_date")):
            continue
        if looks_like_header(cells):
            continue
        rec["page_no"] = 1
        rec["layout"] = "SHEET"
        out.append(rec)
    return out


@F.udf(returnType=ArrayType(LESION_ROW))
def extract_sheet(content, file_name):
    if content is None:
        return []
    try:
        return _sheet_rows(content, file_name)
    except Exception as e:
        return [{**_blank_row(), "pathology": f"__EXTRACT_ERROR__ {type(e).__name__}: {e}",
                 "page_no": -1, "layout": "ERROR"}]


# Self-test. Every case here is one the parser has ACTUALLY failed: a header below a. See wiki.
_SHEET_CASES = [
    ("title block above the header",
     b"Dermatopathology Results Log\nSomewhere Dermatology - Main\n\n"
     b"Date,Patient,MRN,Location,Diagnosis,Actions and Plans,Comments\n"
     b'03/04/2026,"Doe, Jane A",MM123,left ear,"Basal Cell Carcinoma, Nodular Type",'
     b'"Schedule: Mohs vs Superficial Radiation Therapy",Assign\n'
     b'03/05/2026,"Roe, John",MM124,right nose,Seborrheic Keratosis,Reassure: benign.,\n',
     "probe.csv", 2),
    ("alternate labels, header on row 1",
     b"Date of Service,Patient Name,Accession #,Body Site,Final Diagnosis,Plan,Notes\n"
     b'04/01/2026,"Smith, Al",A1,scalp,SCC in situ,Refer: Superficial Radiation Therapy,x\n',
     "a.csv", 1),
    ("header repeated mid-body (paginated export)",
     b'Date,Patient,MRN,Diagnosis\n03/04/2026,"Doe, Jane",M1,BCC\n'
     b'Date,Patient,MRN,Diagnosis\n03/06/2026,"Roe, Sam",M2,SCC\n', "p.csv", 2),
    ("blank and trailer lines",
     b'Date,Patient,MRN,Diagnosis\n,,,\n03/04/2026,"Doe, Jane",M1,BCC\nTotal rows: 1,,,\n',
     "j.csv", 2),
    ("tab separated", b"Date\tPatient\tMRN\tDiagnosis\n03/04/2026\tDoe, Jane\tM1\tBCC\n",
     "t.tsv", 1),
    ("utf-8 BOM and CRLF",
     b"\xef\xbb\xbfDate,Patient,MRN,Diagnosis\r\n03/04/2026,\"Doe, Jane\",M1,BCC\r\n",
     "b.csv", 1),
    ("ragged rows", b'Report\nDate,Patient,MRN,Diagnosis,Actions and Plans\n'
     b'03/04/2026,"Doe, J",M1,BCC\n', "r.csv", 1),
    # 21 Aug: all 16 real CSV/XLSX path logs failed with "no header row found".
    # Underscored machine-generated headers are exactly the shape the whitespace-only
    # alias key cannot reach — and the header hunt refuses any line with no column
    # resolving to patient_name, so one unreachable spelling loses the whole file.
    ("underscored machine headers",
     b"PATIENT_NAME,MRN,DATE_OF_SERVICE,BODY_SITE,FINAL_DIAGNOSIS,ACTIONS_AND_PLANS\n"
     b'"Doe, Jane",M1,03/04/2026,left ear,BCC nodular,'
     b'"Refer: Superficial Radiation Therapy"\n', "u.csv", 1),
    # THE REAL ONE. All 13 CSV path logs are an EzDerm "Biopsy Log" -- a 20-column. See wiki.
    ("EzDerm Biopsy Log, the real 20-column layout",
     b"Flag,Patient Info,Status,Diagnosis,Differential Diagnoses,Biopsy Location,"
     b"Performed,Late,Biopsy Date,Performed By,Accession Number,Path Lab/Person,"
     b"Planned,To Do(s),Note,Follow Up(s),Reviewed Date,Reviewed By,Resolved Date,"
     b"Resolved By\n"
     b'N,"Doe, Jane (01/02/1950) MRN: 12345",Reviewed,"Basal Cell Carcinoma, Nodular",'
     b',"left ear",Yes,N,03/04/2026,"Smith MD",AC-99,"LabCorp",,'
     b'"Discussed Superficial Radiation Therapy",note,,03/10/2026,"Jones",,1\n',
     "ezderm.csv", 1),
]
_sf = []
for _name, _data, _fn, _want in _SHEET_CASES:
    _got = _sheet_rows(_data, _fn)
    _err = [r for r in _got if r.get("layout") == "ERROR"]
    if len(_got) != _want or _err:
        _sf.append(f"{_name}: got {len(_got)} rows, want {_want}"
                   + (f" — {_err[0]['pathology'][:90]}" if _err else ""))
# and the negative case: an unrecognisable header must REPORT, never return 0 silently
_neg = _sheet_rows(b"a,b,c\n1,2,3\n", "n.csv")
if not (_neg and _neg[0].get("layout") == "ERROR"):
    _sf.append("a file with no recognisable header returned no error row — a whole "
               "practice could read as empty with nothing to explain it")
if _sf:
    for _m in _sf:
        print("  FAIL", _m)
    raise AssertionError(f"{len(_sf)} sheet-parser regression(s)")

# the EzDerm case in detail -- these five assertions are the ones that matter
_ez = _sheet_rows(_SHEET_CASES[-1][1], "ezderm.csv")[0]
assert _ez["layout"] == "SHEET", _ez
assert _ez["patient_name"] == "Doe, Jane", _ez          # DOB lifted off the join key
# ISO, not the source spelling: §7f now runs collected_date / result_date / dob
# through norm_date, exactly as both PDF parsers do, so bronze holds one date
# format instead of two. The parenthesised DOB is still lifted off the name --
# that is what this line guards -- but it is stored normalised.
assert _ez["dob"] == "1950-01-02", _ez
assert _ez["accession"] == "AC-99", _ez                 # the column beats the inline MRN
assert _ez["body_site"] == "left ear", _ez              # Biopsy Location = LESION
assert "Reviewed" in _ez["actions"] and "Radiation" in _ez["actions"], (
    "Status and To Do(s) both map to actions and must CONCATENATE — overwriting "
    f"loses the SRT signal: {_ez['actions']!r}")

_pr = _sheet_rows(_SHEET_CASES[0][1], "probe.csv")
assert _pr[0]["patient_name"] == "Doe, Jane A", _pr[0]
assert _pr[0]["accession"] == "MM123", _pr[0]
assert _pr[0]["body_site"] == "left ear", _pr[0]     # Location -> body_site, not facility
assert "Basal Cell" in _pr[0]["pathology"], _pr[0]
assert "Radiation" in _pr[0]["actions"], _pr[0]
assert _pr[0]["collected_date"] == "2026-03-04", _pr[0]   # normalised, see above
print(f"sheet parser OK — {len(_SHEET_CASES)} cases + the no-header negative; columns "
      f"mapped through the SAME _alias() the PDF path uses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Scanned files — `ai_parse_document`

# COMMAND ----------

try:
    print("version:", spark.sql("SELECT current_version()").collect()[0][0])
except Exception:
    print("spark:", spark.version)

try:
    spark.sql("SELECT ai_parse_document(CAST('x' AS BINARY))").collect()
    AI_PARSE_OK = True
except Exception as e:
    msg = str(e)
    AI_PARSE_OK = not ("UNRESOLVED_ROUTINE" in msg or "not found" in msg.lower())
    print(("available (rejected dummy input, which is fine)" if AI_PARSE_OK
           else "NOT AVAILABLE"), "-", msg[:200])

print("\nai_parse_document usable:", AI_PARSE_OK)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8a. HTML table → rows of cells

# COMMAND ----------

from html.parser import HTMLParser


class _TableGrab(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables, self._t, self._r, self._c, self._in = [], None, None, [], False

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._t = []
        elif tag == "tr" and self._t is not None:
            self._r = []
        elif tag in ("td", "th") and self._r is not None:
            self._in, self._c = True, []
        elif tag == "br" and self._in:
            self._c.append(" ")

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._r is not None:
            self._r.append("".join(self._c).strip())
            self._in = False
        elif tag == "tr" and self._t is not None and self._r is not None:
            self._t.append(self._r)
            self._r = None
        elif tag == "table" and self._t is not None:
            self.tables.append(self._t)
            self._t = None

    def handle_data(self, d):
        if self._in:
            self._c.append(d)


def html_tables(html):
    if not html:
        return []
    p = _TableGrab()
    try:
        p.feed(html)
        p.close()
    except Exception:
        return []
    return [t for t in p.tables if t]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8b. Inspect the raw parse for one scanned file

# COMMAND ----------

_scanned_ct = probe.filter("is_scanned").count()
print("scanned files in the sample:", _scanned_ct)

if _scanned_ct and AI_PARSE_OK:
    display(probe.filter("is_scanned").limit(1)
        .withColumn("ai", F.expr("ai_parse_document(content)"))
        .selectExpr(
            "file_name",
            "cast(ai:error_status as string) AS error_status",
            "explode(cast(ai:document.elements as array<variant>)) AS el")
        .selectExpr(
            "file_name", "error_status",
            "cast(el:type as string)       AS el_type",
            "cast(el:confidence as double) AS confidence",
            "cast(el:content as string)    AS content"))
else:
    print("Skipping probe.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8c. Parsed elements → lesion rows

# COMMAND ----------

@F.udf(returnType=ArrayType(LESION_ROW))
def rows_from_ai(elements):
    """elements: array<struct<el_type, el_content>> for one document."""
    if not elements:
        return []
    out = []
    try:
        text_blob = "\n".join(e["el_content"] for e in elements
                              if e["el_type"] != "table" and e["el_content"])
        fac = facility_from_header(text_blob)

        for e in elements:
            if e["el_type"] == "table" and e["el_content"]:
                for tbl in html_tables(e["el_content"]):
                    out.extend(parse_rows(tbl, 1, fac, layout="AI_PARSE"))

        if not out:                                  # not tabular — try narrative
            out = parse_narrative(text_blob, 1, fac)
            for r in out:
                r["layout"] = "AI_PARSE"
    except Exception as ex:
        out.append({**_blank_row(),
                    "pathology": f"__AI_PARSE_ERROR__ {type(ex).__name__}: {ex}",
                    "page_no": -1, "layout": "ERROR"})
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Run extraction and write the bronze table

# COMMAND ----------

META = ["file_hash", PATH_COL, "account_name", "file_name", "is_scanned"]


def process_batch(batch, tag):
    """Extract one batch and commit it. Rows first, then the manifest.

    That order matters. If the process dies between the two writes the rows are
    already in bronze but the files are not marked done, so the next run re-extracts
    them and appends duplicates. Duplicate lesion rows are absorbed by the GROUP BY
    in patient_days (`bool_or(x) = bool_or(x, x)`), so the cost is a wrong n_lesions
    and nothing else. The opposite order would mark files done whose rows never
    landed -- silently missing patients, which is not recoverable by re-running.

    Slow order, wrong-in-a-harmless-way. Not an accident.
    """
    # THREE branches now, and the order of these two columns matters. `is_scanned`
    # opens the bytes with pdfplumber; handed a CSV it raises or returns garbage, so a
    # sheet must never reach it. is_sheet is decided from the FILE NAME and is_scanned
    # is only evaluated where is_sheet is false.
    b = (batch
         .withColumn("is_sheet", F.expr(
             "lower(file_name) RLIKE '\\.(csv|tsv|xlsx|xlsm|xls)$'"))
         .withColumn("is_scanned", F.when(F.col("is_sheet"), F.lit(False))
                                    .otherwise(is_scanned(F.col(CONTENT_COL)))))

    # .cache() is unsupported on serverless; materialising to Delta is the equivalent.
    # Without it Spark recomputes `b` on every reference and re-opens every PDF.
    (b.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TODO_TABLE))
    b = spark.table(TODO_TABLE)
    n_scan  = b.filter("is_scanned").count()
    n_sheet = b.filter("is_sheet").count()

    text_rows = (b.filter("NOT is_scanned AND NOT is_sheet")
                  .withColumn("rows", extract_text_layer(F.col(CONTENT_COL)))
                  .select(*META, F.explode_outer("rows").alias("r")))

    if n_sheet:
        sheet_rows = (b.filter("is_sheet")
                       .withColumn("rows", extract_sheet(F.col(CONTENT_COL),
                                                         F.col("file_name")))
                       .select(*META, F.explode_outer("rows").alias("r")))
        text_rows = text_rows.unionByName(sheet_rows)

    if n_scan and AI_PARSE_OK:
        _ai = (b.filter("is_scanned")
                .withColumn("ai", F.expr("ai_parse_document(content)"))
                .withColumn("elements", F.expr("cast(ai:document.elements as array<variant>)")))

        _ai_flat = (_ai.select(*META, F.explode_outer("elements").alias("el"))
                       .withColumn("el_type",    F.expr("cast(el:type as string)"))
                       .withColumn("el_content", F.expr("cast(el:content as string)"))
                       .groupBy(*META)
                       .agg(F.collect_list(F.struct("el_type", "el_content")).alias("elements")))

        ocr_rows = (_ai_flat.withColumn("rows", rows_from_ai(F.col("elements")))
                            .select(*META, F.explode_outer("rows").alias("r")))
    else:
        ocr_rows = text_rows.limit(0)      # same schema, no rows

    all_rows = (text_rows.unionByName(ocr_rows)
                .select(*META, "r.*")
                .withColumn("run_id", F.lit(tag))
                .withColumn("extracted_at", F.current_timestamp())
                .filter(F.col("patient_name").isNotNull() | (F.col("layout") == "ERROR")))

    (all_rows.write.mode("append").option("mergeSchema", "true").saveAsTable(ROWS_TABLE))

    # --- manifest, built from the batch's FULL file list, not from the rows written. See wiki.
    _counts = (spark.table(ROWS_TABLE).filter(F.col("run_id") == tag)
                    .groupBy("file_hash")
                    .agg(F.count("*").cast("int").alias("row_count")))

    _man = (b.select("file_hash", F.col(PATH_COL).alias("source_path"),
                     "account_name", "file_name", "is_scanned")
             .join(_counts, on="file_hash", how="left")
             .withColumn("row_count", F.coalesce(F.col("row_count"), F.lit(0)))
             .withColumn("status", F.when(F.col("row_count") > 0, F.lit("OK"))
                                    .otherwise(F.lit("EMPTY")))
             .withColumn("processed_at", F.current_timestamp())
             .select("file_hash", "source_path", "account_name", "file_name",
                     "is_scanned", "row_count", "status", "processed_at"))

    _man.createOrReplaceTempView("_new_manifest")
    spark.sql(f"""
    MERGE INTO {MANIFEST_TABLE} t
    USING _new_manifest s ON t.file_hash = s.file_hash
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    _w = _man.filter("status = 'OK'").count()
    return {"files": b.count(), "scanned": n_scan, "ok": _w,
            "empty": _man.count() - _w,
            "rows": spark.table(ROWS_TABLE).filter(F.col("run_id") == tag).count()}


# --------------------------------------------------------------------------- loop
import time
_t0     = time.time()
_i      = 0
_totals = {"files": 0, "scanned": 0, "ok": 0, "empty": 0, "rows": 0}
_prev_remaining = None

while True:
    _rem_df = remaining_files()
    _rem    = _rem_df.count()
    if _rem == 0:
        print("\nnothing left to process")
        break

    # A batch that commits nothing would re-select the same files forever. The
    # manifest write is what shrinks this set, so if it does not shrink, stop.
    if _prev_remaining is not None and _rem >= _prev_remaining:
        raise RuntimeError(
            f"remaining did not shrink ({_prev_remaining:,} -> {_rem:,}). The manifest "
            f"MERGE is not committing; stopping rather than looping.")
    _prev_remaining = _rem

    _size   = BATCH_SIZE if BATCH_SIZE else _rem
    _hashes = [r["file_hash"] for r in
               _rem_df.select("file_hash").orderBy("file_hash").limit(_size).collect()]
    _batch  = src.filter(F.col("file_hash").isin(_hashes))

    _i += 1
    _tag = f"{RUN_ID}-b{_i:04d}"
    _bt  = time.time()
    _r   = process_batch(_batch, _tag)
    _el  = time.time() - _bt

    for k in _totals:
        _totals[k] += _r[k]
    _mins = (time.time() - _t0) / 60
    _left = _rem - _r["files"]
    _eta  = (_mins / _totals["files"] * _left) if _totals["files"] else 0
    print(f"batch {_i:>3}  {_r['files']:>4} files ({_r['scanned']} scanned)  "
          f"{_r['rows']:>6} rows  OK {_r['ok']:>4} EMPTY {_r['empty']:>3}  "
          f"{_el/60:5.1f} min   |  done {_totals['files']:>5}, "
          f"{_left:>5} left, ETA {_eta:5.1f} min")

    if MAX_MINUTES and _mins >= MAX_MINUTES:
        print(f"\nhit the {MAX_MINUTES:g}-minute budget with {_left:,} files left.")
        print("Everything processed so far is committed. Re-run this notebook to resume.")
        break

print(f"\n{_totals['files']:,} files, {_totals['rows']:,} rows, "
      f"{_totals['empty']:,} EMPTY, in {(time.time()-_t0)/60:.1f} min "
      f"across {_i} batch(es)")
print("wrote rows to", ROWS_TABLE)

# Every tag this run used, so the QA gates below can scope to this run.
RUN_TAGS = [f"{RUN_ID}-b{n:04d}" for n in range(1, _i + 1)]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9b. Update the manifest

# COMMAND ----------

# The manifest is written per batch inside process_batch, so there is nothing to
# commit here. This cell reconciles it and clears the staging tables.

spark.sql(f"DROP TABLE IF EXISTS {TODO_TABLE}")    # held raw PDF bytes for one batch
spark.sql(f"DROP TABLE IF EXISTS {PROBE_TABLE}")   # ditto, for the section 6 sample

# Counts pulled out into variables first: an f-string expression cannot contain a
# backslash, so an escaped quote inside {...} is a SyntaxError, not a runtime error.
_m      = spark.table(MANIFEST_TABLE)
_src_n  = src.count()
_m_n    = _m.count()
_m_ok   = _m.filter(_m.status == "OK").count()
_m_empt = _m.filter(_m.status == "EMPTY").count()

print(f"source files            : {_src_n:,}")
print(f"manifest entries        : {_m_n:,}")
print(f"  OK                    : {_m_ok:,}")
print(f"  EMPTY                 : {_m_empt:,}")

_gap = remaining_files().count()
if _gap:
    print(f"\n*** {_gap:,} source files still have no manifest entry ***")
    print("Expected only if you stopped on the time budget. Re-run to resume.")
    display(remaining_files().select("account_name", "file_name", "length")
                             .orderBy("account_name"))
else:
    print("\nevery source file is accounted for")

display(_m.groupBy("account_name")
          .agg(F.count("*").alias("files"),
               F.sum(F.when(F.col("status") == "EMPTY", 1).otherwise(0)).alias("empty_files"),
               F.sum("row_count").alias("rows"))
          .orderBy("account_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. QA gates

# COMMAND ----------

rows = spark.table(ROWS_TABLE)

# These gates read the WHOLE bronze table, not just this run -- that is deliberate,
# because what ships is the portfolio, not the increment. To scope a gate to this run
# instead: rows.filter(F.col("run_id").isin(RUN_TAGS))

# --- which layout did each file parse as? A NULL here means nothing parsed.
display(rows.groupBy("account_name", "file_name", "layout").count()
            .orderBy("account_name", "file_name"))

# COMMAND ----------

# --- Gate 0: the SAME export filed under two folder names. See wiki.
_pairs = spark.sql(f"""
WITH k AS (
  SELECT DISTINCT account_name,
         concat_ws('|', coalesce(accession, ''), coalesce(collected_date, ''),
                        lower(coalesce(body_site, '')), lower(coalesce(pathology, ''))) AS lk
  FROM {ROWS_TABLE}
  WHERE coalesce(accession, '') <> '' OR coalesce(pathology, '') <> ''
),
n AS (SELECT account_name, count(*) AS keys FROM k GROUP BY account_name),
j AS (
  SELECT a.account_name AS acct_a, b.account_name AS acct_b, count(*) AS shared
  FROM k a JOIN k b ON a.lk = b.lk AND a.account_name < b.account_name
  GROUP BY 1, 2
)
SELECT j.acct_a, j.acct_b, j.shared, na.keys AS keys_a, nb.keys AS keys_b,
       round(100.0 * j.shared / least(na.keys, nb.keys), 1) AS pct_of_smaller
FROM j JOIN n na ON na.account_name = j.acct_a
       JOIN n nb ON nb.account_name = j.acct_b
WHERE j.shared >= 20
  AND 100.0 * j.shared / least(na.keys, nb.keys) >= 60
ORDER BY pct_of_smaller DESC, shared DESC
""")
_np = _pairs.count()
print("cross-folder duplicate exports:", _np)
if _np:
    display(_pairs)
    print("Two folders sharing most of their lesions are ONE practice filed twice.")
    print("Every shared patient is counted twice. Merge them onto one account_name")
    print("before reporting -- patient_days then absorbs the overlap and keeps the extras.")

# --- Gate 1: files that produced zero rows
empty = spark.table(MANIFEST_TABLE).filter("status = 'EMPTY'").select("account_name", "file_name")
print("files with zero rows:", empty.count())
if empty.count():
    display(empty)

# --- Gate 2: extraction errors
errs = rows.filter("layout = 'ERROR'")
print("extraction errors:", errs.count())
if errs.count():
    display(errs.select("account_name", "file_name", "pathology"))

# --- Gate 3: names under 4 letters (possible OCR loss — or a real short surname)
short = rows.filter(F.length(F.trim(F.col("patient_name"))) < 4) \
            .select("account_name", "file_name", "patient_name")
print("short names:", short.count())
if short.count():
    display(short)

# --- Gate 4: null dates. A whole account with null collected_date almost always
#     means an unrecognised header — check _ALIASES first.
display(rows.groupBy("account_name").agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("collected_date").isNull(), 1).otherwise(0)).alias("null_collected"),
    F.sum(F.when(F.col("result_date").isNull(), 1).otherwise(0)).alias("null_result"),
    F.sum(F.when(F.col("actions").isNull() | (F.trim(F.col("actions")) == ""), 1)
           .otherwise(0)).alias("blank_actions")
).orderBy("account_name"))

# --- Gate 5: TRUNCATED action cells. The EMA action vocabulary is structured —. See wiki.
_TRUNC = r"^\s*(schedule|refer|reassure|discuss|observe|monitor|recheck|treat)\s*:?\s*$"

_trunc = rows.filter(
    F.col("actions").rlike(f"(?i){_TRUNC}") | F.col("actions").rlike(r":\s*$"))

print(f"Gate 5 — suspected truncated action cells: {_trunc.count()}")
if _trunc.count():
    print("  Each of these may be a LOST SRT MARK. Open the source page for the row and")
    print("  compare. Do NOT change extract_ruled_rows() without re-extracting every")
    print("  account and comparing row counts — 02_Methodology 2.2 records that a")
    print("  previous tolerance change halved several accounts.")
    display(_trunc.select("account_name", "file_name", "page_no", "collected_date",
                          "pathology", "body_site", "actions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10b. Facility check

# COMMAND ----------

display(rows.groupBy("account_name", "facility").count().orderBy("account_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Silver view — one view, every derived column

# COMMAND ----------

# MAGIC %run ./00_vocab

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.staged")

# The view definition lives in 00_vocab so the classifier's regression
# harness can build the identical view over probe rows. A regression that tests a
# copy of the logic tests nothing.
spark.sql(classified_sql(ROWS_TABLE, f"{CATALOG}.staged.lesion_classified"))

print("created", f"{CATALOG}.staged.lesion_classified")

# ---- IDENTIFIER-IN-THE-KEY GATE -------------------------------------------. See wiki.
_ctl = spark.sql("""
SELECT
  sum(CASE WHEN nm LIKE '%|%'
            OR nm RLIKE '[(][^)]*[0-9]{4}[^)]*[)]' THEN 1 ELSE 0 END) AS clean_flagged,
  sum(CASE WHEN dirty LIKE '%|%'
            OR dirty RLIKE '[(][^)]*[0-9]{4}[^)]*[)]' THEN 1 ELSE 0 END) AS dirty_flagged
FROM VALUES
  ('Wood, Mary',      'Doe, Jane (F 56 | June 17, 1970 | DABA0008)'),
  ('Smith, John (Jr)','Salazar, Maria (F, 61 | May 12, 1965 | SALO0001)'),
  ('Massey, Thomas',  'Lee, Ann (F, 40 | Mar 3, 1986 | LEEA0003)')
  AS t(nm, dirty)
""").first()
assert _ctl["clean_flagged"] == 0, (
    f"the identifier gate flags {_ctl['clean_flagged']} of 3 KNOWN-CLEAN names. The "
    "predicate is broken, not the data -- almost certainly backslash escaping: a "
    "bare `|` reaching the regex engine is (empty|empty) and matches every string.")
assert _ctl["dirty_flagged"] == 3, (
    f"the identifier gate catches only {_ctl['dirty_flagged']} of 3 known-dirty "
    "names, so it would pass a corrupt extract silently.")

_leak = spark.sql(f"""
SELECT count(*)                                                   AS rows,
       count(DISTINCT account_name)                               AS accounts,
       min(account_name)                                          AS example_account
FROM {CATALOG}.staged.lesion_classified
WHERE patient_name LIKE '%|%'
   OR patient_name RLIKE '[(][^)]*[0-9]{{4}}[^)]*[)]'
""").first()

if _leak["rows"]:
    raise AssertionError(
        f"{_leak['rows']:,} rows across {_leak['accounts']} accounts carry an "
        f"identifier inside patient_name (e.g. {_leak['example_account']}). "
        "patient_name becomes patient_id wherever there is no accession, so a "
        "date of birth would ship in a key column and its month would pollute "
        "the VOB name-matching tokens. Extend split_patient() for this layout "
        "-- see the parenthetical branch added 26 Aug -- then re-run.")
print("patient_name carries no embedded identifiers")

display(spark.sql(f"""
SELECT account_name,
       count(*)                                                  AS rows,
       sum(CASE WHEN is_nmsc_type THEN 1 ELSE 0 END)    AS is_nmsc_type,
       sum(CASE WHEN path_class = 'excluded' THEN 1 ELSE 0 END)  AS excluded,
       sum(CASE WHEN path_class = 'unknown'  THEN 1 ELSE 0 END)  AS unknown_path,
       sum(CASE WHEN srt_marked     THEN 1 ELSE 0 END)           AS marked,
       sum(CASE WHEN vob_mentioned  THEN 1 ELSE 0 END)           AS vob_mentioned,
       sum(CASE WHEN is_no_residual THEN 1 ELSE 0 END)           AS no_residual,
       round(100.0 * avg(CASE WHEN has_actions THEN 1 ELSE 0 END), 0) AS actions_coverage_pct
FROM {CATALOG}.staged.lesion_classified
GROUP BY account_name ORDER BY account_name
"""))

# The three path_class values partition the rows — this must always be zero.
_chk = spark.sql(f"""
SELECT count(*) AS n FROM {CATALOG}.staged.lesion_classified
WHERE path_class NOT IN ('acceptable','excluded','unknown')
   OR (is_nmsc_type <> (path_class = 'acceptable'))
""").first()["n"]
assert _chk == 0, f"{_chk} rows where is_nmsc_type disagrees with path_class"
print("is_nmsc_type agrees with path_class on every row")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Score against the answer keys

# COMMAND ----------

# Practice-corpus answer keys. OPTIONAL — on real SharePoint exports there are none. See wiki.

import os

_cands = [f"{SRC_VOL}/expected_results.csv",
          f"{SRC_VOL}/expected_results_ema.csv",
          f"{SRC_VOL}/practice_pathlogs/expected_results.csv",
          f"{SRC_VOL}/practice_pathlogs/expected_results_ema.csv"]
_keys = list(dict.fromkeys(p for p in _cands if os.path.isfile(p)))   # dedupe, keep order

for p in _cands:
    if p not in _keys:
        print("  (absent)", p)

expected = None
for k in _keys:
    df = spark.read.option("header", True).csv(k)
    df.limit(1).count()                     # force the read while we can still attribute it
    expected = df if expected is None else expected.unionByName(df)
    print("loaded", k)

if expected is None:
    print("No answer key found — expected on real data.")
else:
    actual = spark.table(f"{CATALOG}.staged.lesion_classified")  # flags live in silver
    score = (expected.groupBy("account").agg(
                F.count("*").alias("expected_rows"),
                F.sum(F.expr("CASE WHEN srt_marked='Y' THEN 1 ELSE 0 END")).alias("exp_marked"),
                F.sum(F.expr("CASE WHEN vob_mentioned='Y' THEN 1 ELSE 0 END")).alias("exp_vob"))
             .join(
                actual.groupBy("account_name").agg(
                    F.count("*").alias("actual_rows"),
                    F.sum(F.col("srt_marked").cast("int")).alias("act_marked"),
                    F.sum(F.col("vob_mentioned").cast("int")).alias("act_vob")),
                F.col("account") == F.col("account_name"), "left")
             .withColumn("status", F.when(
                 (F.col("expected_rows") == F.col("actual_rows")) &
                 (F.col("exp_marked") == F.col("act_marked")) &
                 (F.col("exp_vob") == F.col("act_vob")),
                 F.lit("OK")).otherwise(F.lit("CHECK")))
             .drop("account_name"))
    display(score.orderBy("account"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12b. The patient-day collapse

# COMMAND ----------

display(rows.groupBy("account_name").agg(
    F.count("*").alias("specimen_rows"),
    F.countDistinct(F.concat_ws("|", "patient_name", "collected_date")).alias("patient_days"),
    F.countDistinct("patient_name").alias("distinct_patients"),
    F.countDistinct("facility").alias("facilities"),
    F.sum(F.when(F.col("facility").isNull(), 1).otherwise(0)).alias("blank_facility")
).orderBy("account_name"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Next