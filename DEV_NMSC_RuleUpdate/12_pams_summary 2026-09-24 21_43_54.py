# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "openpyxl",
# ]
# ///
# MAGIC %md
# MAGIC # 12 · Pam's Summary — the combined workbook
# MAGIC
# MAGIC Seven sheets, project instructions v6.2 §5. Built from the **export folder**
# MAGIC written by notebook 09, not from the tables, so the workbook and the CSVs can
# MAGIC never disagree.
# MAGIC
# MAGIC ### PHI
# MAGIC Three sheets carry patient identifiers — **Raw-Unfiltered**, **Appropriate
# MAGIC Patients**, **VOB mentioned - no VOB found**. The finished workbook is a PHI
# MAGIC file. Keep it in the volume, download only to a controlled machine, and never
# MAGIC attach it to a message, an email or a ticket.
# MAGIC
# MAGIC `PHI_SHEETS = False` builds the four account-level sheets only — use that for
# MAGIC anything that has to leave the volume.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# openpyxl is not on the cluster by default. 01_ingest installs it for its own run,
# but each notebook gets a fresh Python environment, so 12 has to ask for it too.
# restartPython() is what makes the install visible to the interpreter -- without it
# the import still fails in this session.
dbutils.library.restartPython()

# COMMAND ----------

import openpyxl
print("openpyxl", openpyxl.__version__)

# COMMAND ----------

dbutils.widgets.text("export_dir", "/Volumes/dev_pathology_data/exports/2026-05-01_to_2026-09-01", "Export folder")
dbutils.widgets.text("out_path",   "/Volumes/dev_pathology_data/exports/MasterClaude_May-Aug_2026.xlsx", "Output .xlsx")
dbutils.widgets.dropdown("phi_sheets", "yes", ["yes", "no"], "Include PHI sheets?")
dbutils.widgets.text("months", "May,Jun,Jul,Aug", "Months in window")

EXPORT = dbutils.widgets.get("export_dir").rstrip("/")   # kept for readability; rd() reads the widget itself
OUT    = dbutils.widgets.get("out_path")
PHI    = dbutils.widgets.get("phi_sheets") == "yes"
M      = [x.strip() for x in dbutils.widgets.get("months").split(",") if x.strip()]
print(f"export : {EXPORT}\noutput : {OUT}\nPHI    : {'YES — keep in the volume' if PHI else 'no — account level only'}\nmonths : {M}")

# COMMAND ----------

# `dbutils.library.restartPython()` above wipes every Python variable. Running a cell
# below this point without re-running the widgets cell gives a bare NameError that
# does not say why. Fail with the reason instead.
for _v in ("EXPORT", "OUT", "PHI", "M"):
    if _v not in dir():
        raise NameError(
            f"{_v} is not defined. The %pip install restarted Python, which cleared "
            f"every variable. Re-run this notebook from the top (Run All), or at "
            f"minimum re-run the widgets cell above before this one.")
print("widgets are live — EXPORT, OUT, PHI, M all set")

# COMMAND ----------

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter as L

ARI  = Font(name="Arial", sz=10)
ARIB = Font(name="Arial", sz=10, b=True)
HDR  = Font(name="Arial", sz=10, b=True, color="FFFFFF")
BAND = Font(name="Arial", sz=11, b=True)
NOTE = Font(name="Arial", sz=9, i=True, color="5A6B75")
NAVY = PatternFill("solid", fgColor="1F4E78")
CTR  = Alignment(horizontal="center", vertical="center")
CTRW = Alignment(horizontal="center", vertical="center", wrap_text=True)

# [24 Sep] Read the widget rather than closing over a module-level EXPORT.
# `%pip install` + restartPython() clears every variable, so a helper that depended
# on a global failed with a bare NameError whenever a cell ran out of order. Reading
# the widget costs nothing and cannot go stale.
def _cfg(key, default=""):
    try:
        return dbutils.widgets.get(key)
    except Exception:
        return default

# Re-establish all four from the widgets in THIS cell, so running it is enough to
# recover from the restart no matter what order the cells were run in.
EXPORT = _cfg("export_dir", "").rstrip("/")
OUT    = _cfg("out_path", "")
PHI    = _cfg("phi_sheets", "yes") == "yes"
M      = [x.strip() for x in _cfg("months", "May,Jun,Jul,Aug").split(",") if x.strip()]
print(f"config · export={EXPORT or '(EMPTY — set the widget)'} · out={OUT} · PHI={PHI} · months={M}")

def rd(name):
    root = _cfg("export_dir", "").rstrip("/")
    if not root:
        raise ValueError("The export_dir widget is empty. Set it to the folder "
                         "notebook 09 wrote, e.g. "
                         "/Volumes/dev_pathology_data/exports/2026-05-01_to_2026-09-01")
    return pd.read_csv(f"{root}/{name}", low_memory=False)

# LibreOffice evaluates a leading = + @ as a formula, and OCR marks produce them (§6).
def sanitize(v):
    if isinstance(v, str) and v[:1] in ("=", "+", "@"):
        return "'" + v
    return v

def add_frame(wb, title, df, index=None, hidden=False, width_a=46):
    ws = wb.create_sheet(title) if index is None else wb.create_sheet(title, index)
    ws.append(list(df.columns))
    for c in ws[1]:
        c.font, c.fill, c.alignment = HDR, NAVY, CTRW
    for row in df.itertuples(index=False):
        ws.append([sanitize(v) for v in row])
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.font = ARI
    ws.column_dimensions["A"].width = width_a
    ws.freeze_panes = "B2"
    ws.auto_filter.ref = f"A1:{L(ws.max_column)}{ws.max_row}"
    if hidden:
        ws.sheet_state = "hidden"
    return ws

# COMMAND ----------

s  = rd("account_summary.csv").sort_values("account_name").reset_index(drop=True)
vd = rd("vob_disposition.csv")
sr = rd("sf_rollup.csv")
rv = rd("review.csv")
print(f"{len(s)} accounts · {len(vd)} disposition rows · {len(sr)} sf_rollup · {len(rv)} review")

# The gates that must hold before a workbook is worth building (§7).
assert (s.nmsc_total > 0).all(), \
    f"{(s.nmsc_total==0).sum()} accounts produced no rows: {list(s[s.nmsc_total==0].account_name)}"
assert (s.marked_total <= s.nmsc_total).all(), "Marked exceeds Appropriate on some account"
assert s.account_id.notna().all(), \
    f"account(s) with no AccountId — they score zero VOBs: {list(s[s.account_id.isna()].account_name)}"
_blocking = rv[rv.severity == "blocking"] if "severity" in rv.columns else rv.iloc[0:0]
assert len(_blocking) == 0, f"{len(_blocking)} BLOCKING review rows:\n{_blocking.to_string(index=False)}"
print("pre-build gates pass")

# COMMAND ----------

wb = Workbook()

# ---- 7 · AllAccounts (hidden) — keeps Pam's Summary col Y a live lookup
aa_cols = ["account_name", "account_id", "accounts_pooled"] + \
          [f"sf_vob_{m}" for m in M] + [f"sf_sim_{m}" for m in M]
aa_cols = [c for c in aa_cols if c in sr.columns]
aa = wb.active; aa.title = "AllAccounts"
aa.append(aa_cols)
for c in aa[1]:
    c.font, c.fill, c.alignment = HDR, NAVY, CTRW
for _, r_ in sr.iterrows():
    aa.append([sanitize(r_.get(c)) for c in aa_cols])
for row in aa.iter_rows(min_row=2):
    for c in row:
        c.font = ARI
aa.column_dimensions["A"].width = 46
aa.sheet_state = "hidden"
AA_LAST = len(sr) + 1

# ---- 3 · VOB BreakDown
vb = add_frame(wb, "VOB BreakDown", vd)
VB_LAST = len(vd) + 1
VD = {c: L(i) for i, c in enumerate(vd.columns, 1)}

# COMMAND ----------

# ---- 1 · Pam's Summary --------------------------------------------------------
ws = wb.create_sheet("Pam's Summary", 0)
BANDS = [("", 1, 9), ("NMSC", 10, 14), ("MARKED", 15, 19), ("VOB", 20, 25),
         ("SIM", 26, 30), ("CONVERSION", 31, 34), ("", 35, 36),
         ("TIMING", 37, 39), ("", 40, 42)]
HEADERS = (
    ["Account", "Regional", "Senior", "Mohs", "VOB pattern",
     "NMSC process 1", "NMSC process 2", "NMSC process 3", "NMSC process 4"] +
    [f"{m} Appropriate" for m in M] + ["Total Appropriate"] +
    [f"{m} SRT-Marked" for m in M] + ["Total SRT-Marked PT days"] +
    [f"VOB {m}" for m in M] + ["TOTALVOB Matched", "Total VOB from Salesforce"] +
    [f"Converted {m}" for m in M] + ["TotalVOB Converted"] +
    ["Marked / Appropriate", "Appropriate Patient Days → VOB", "Marked → VOB", "VOB → Sim"] +
    ["VOB pattern (analysis)", "Actions/Plans coverage"] +
    ["Avg days Path Result → VOB", "Avg days Biopsy → VOB", "Avg days Phone Call → VOB"] +
    ["Unknown VOB", "Unknown VOB / mth", "Phase"])
assert len(HEADERS) == 42, len(HEADERS)

for label, a0, b0 in BANDS:
    if label:
        ws.merge_cells(start_row=1, start_column=a0, end_row=1, end_column=b0)
        c = ws.cell(1, a0, label); c.font, c.alignment = BAND, CTR
for i, h in enumerate(HEADERS, 1):
    c = ws.cell(2, i, h); c.font, c.fill, c.alignment = HDR, NAVY, CTRW

MONTHS_IN_PERIOD = len(M)
r = 3
for _, a in s.iterrows():
    ws.cell(r, 1, a.account_name).font = ARI
    ws.cell(r, 2, sanitize(a.get("regional"))).font = ARI
    ws.cell(r, 3, sanitize(a.get("senior"))).font = ARI
    ws.cell(r, 4, None).font = ARI                       # Mohs — hand-maintained
    ws.cell(r, 5, sanitize(a.get("vob_pattern_band"))).font = ARI
    for i in range(6, 10):
        ws.cell(r, i, None).font = ARI                   # qualitative — hand-maintained
    for j, m in enumerate(M):
        ws.cell(r, 10 + j, int(a[f"nmsc_{m}"])).font = ARI
    ws.cell(r, 14, f"=SUM(J{r}:M{r})").font = ARIB
    for j, m in enumerate(M):
        ws.cell(r, 15 + j, int(a[f"marked_{m}"])).font = ARI
    ws.cell(r, 19, f"=SUM(O{r}:R{r})").font = ARIB
    for j, m in enumerate(M):
        ws.cell(r, 20 + j, int(a[f"vobm_{m}"])).font = ARI
    # X = SUM of the monthly matched columns, NOT account_summary.vob_total.
    # vob_total is gated on `marked` in 08_gold, so the zero-marked accounts report 0
    # there while their monthly columns carry real matched VOBs. Summing keeps the row
    # internally consistent and keeps those accounts honest.
    ws.cell(r, 24, f"=SUM(T{r}:W{r})").font = ARIB
    ws.cell(r, 25, f"=IFERROR(" + "+".join(
        f"SUMIF(AllAccounts!$A$2:$A${AA_LAST},$A{r},AllAccounts!${L(aa_cols.index(f'sf_vob_{m}')+1)}$2:"
        f"${L(aa_cols.index(f'sf_vob_{m}')+1)}${AA_LAST})" for m in M) + ",0)").font = ARI
    for j, m in enumerate(M):
        ws.cell(r, 26 + j, int(a[f"simm_{m}"])).font = ARI
    ws.cell(r, 30, f"=SUM(Z{r}:AC{r})").font = ARIB
    for col, f in ((31, f'=IFERROR(S{r}/N{r},"")'), (32, f'=IFERROR(X{r}/N{r},"")'),
                   (33, f'=IFERROR(X{r}/S{r},"")'),  (34, f'=IFERROR(AD{r}/X{r},"")')):
        c = ws.cell(r, col, f); c.font, c.number_format = ARI, "0%"
    ws.cell(r, 35, None).font = ARI
    c = ws.cell(r, 36, float(a.get("actions_coverage") or 0)); c.font, c.number_format = ARI, "0%"
    for col, src in ((37, "avg_days_result_to_vob"), (38, "avg_days_biopsy_to_vob"),
                     (39, "avg_days_phone_to_vob")):
        v = a.get(src)
        c = ws.cell(r, col, None if pd.isna(v) else float(v)); c.font, c.number_format = ARI, "0.0"
    ws.cell(r, 40, "=IFERROR(" + "+".join(
        f"SUMIF('VOB BreakDown'!$A$2:$A${VB_LAST},$A{r},"
        f"'VOB BreakDown'!${VD[b]}$2:${VD[b]}${VB_LAST})"
        for b in ("name_found_no_biopsy_in_range", "no_patient_of_that_name",
                  "ambiguous_namesake") if b in VD) + ",0)").font = ARI
    c = ws.cell(r, 41, f'=IFERROR(AN{r}/{MONTHS_IN_PERIOD},"")'); c.font, c.number_format = ARI, "0.0"
    ws.cell(r, 42, None).font = ARI                      # Phase — hand-maintained
    r += 1
DATA_LO, DATA_HI = 3, r - 1

# ---- TOTAL row: sums the account rows ONLY, never itself (§7)
ws.cell(r, 1, "TOTAL").font = ARIB
for col in list(range(10, 31)) + [40]:
    ws.cell(r, col, f"=SUM({L(col)}{DATA_LO}:{L(col)}{DATA_HI})").font = ARIB
for col, f in ((31, f'=IFERROR(S{r}/N{r},"")'), (32, f'=IFERROR(X{r}/N{r},"")'),
               (33, f'=IFERROR(X{r}/S{r},"")'),  (34, f'=IFERROR(AD{r}/X{r},"")')):
    c = ws.cell(r, col, f); c.font, c.number_format = ARIB, "0%"
c = ws.cell(r, 41, f'=IFERROR(AN{r}/{MONTHS_IN_PERIOD},"")'); c.font, c.number_format = ARIB, "0.0"
TOTROW = r
med = Side(style="medium", color="1F4E78")
for col in range(1, 43):
    ws.cell(TOTROW, col).border = Border(top=med)

# COMMAND ----------

# ---- the recommended conversion is the BOLD one, §5.1 -------------------------
# [v6.2] Blanket -> AG (Marked -> VOB inverted, S/X), NOT AF. The note in the
# project instructions says build_combined.py still bolds AF for Blanket; this
# notebook does the v6.2 thing.
#
# ONE DEPARTURE, deliberate: an account with ZERO marked patient-days takes AF,
# not AH. §5.1 sends `Insufficient data` to AH, but AH divides by X and those
# accounts have no X, so the rule as written bolds a blank cell.
BOLDF = Font(name="Arial", sz=10, b=True)
_rec = {}
for i, a in s.iterrows():
    rr = 3 + i
    band = str(a.get("vob_pattern_band") or "")
    cov  = float(a.get("actions_coverage") or 0)
    mk   = int(a.get("marked_total") or 0)
    if mk == 0:                       col = 32   # AF — see note above
    elif band == "Insufficient data":  col = 34  # AH
    elif band == "Blanket":            col = 33  # AG inverted  [v6.2]
    elif cov < 0.40:                   col = 32  # AF
    elif mk < 10:                      col = 32  # AF
    elif band == "Mixed":              col = 32  # AF
    else:                              col = 33  # AG
    ws.cell(rr, col).font = BOLDF
    _rec[a.account_name] = HEADERS[col - 1]
print("recommended-conversion column counts:", pd.Series(list(_rec.values())).value_counts().to_dict())

ws.freeze_panes = "B3"
ws.column_dimensions["A"].width = 46
for col in range(2, 43):
    ws.column_dimensions[L(col)].width = 13
for col in list(range(10, 14)) + list(range(15, 19)) + list(range(20, 24)) + list(range(26, 30)):
    ws.column_dimensions[L(col)].hidden = True          # monthly detail hidden by default
ws.row_dimensions[2].height = 44

# COMMAND ----------



# ---- 2 / 4 / 5 · the PHI sheets
if PHI:
    add_frame(wb, "Raw-Unfiltered",            rd("raw_unfiltered.csv"))
    add_frame(wb, "Appropriate Patients",      rd("appropriate_patients.csv"))
    add_frame(wb, "VOB mentioned - no VOB found", rd("vob_mentioned_no_vob.csv"))
    print("PHI sheets included — this workbook stays in the volume")
else:
    print("PHI sheets SKIPPED — safe to move off the volume")

order = ["Pam's Summary", "Raw-Unfiltered", "VOB BreakDown", "Appropriate Patients",
         "VOB mentioned - no VOB found", "FlagsReview", "AllAccounts"]
wb._sheets.sort(key=lambda x: order.index(x.title) if x.title in order else 99)

# [24 Sep] openpyxl writes through the local filesystem. On serverless the UC Volumes
# FUSE mount does not accept a create from zipfile, which surfaces as a
# FileNotFoundError on a path whose parent plainly exists. Save to local disk first,
# then copy into the volume with dbutils.fs, which speaks to Volumes properly.
import os, shutil, tempfile

_dest = OUT
_dir  = os.path.dirname(_dest)

# Do NOT hard-code a scratch path. /local_disk0 exists on classic clusters and not on
# serverless; /tmp is the reverse on some runtimes. tempfile picks one that is
# actually writable for whatever this notebook is running on.
_scratch = tempfile.mkdtemp(prefix="pams_")
_tmp     = os.path.join(_scratch, os.path.basename(_dest))
wb.save(_tmp)
print(f"built locally: {_tmp} ({os.path.getsize(_tmp):,} bytes)")

# make sure the destination folder exists before copying into it
try:
    dbutils.fs.mkdirs(_dir)
except Exception as e:
    print(f"  note: could not mkdirs {_dir} — {e}")

try:
    dbutils.fs.cp(f"file:{_tmp}", _dest)
    print(f"wrote {_dest} · sheets: {wb.sheetnames}")
except Exception as e:
    # last resort: a plain copy, which works on clusters where the mount is writable
    try:
        shutil.copyfile(_tmp, _dest)
        print(f"wrote {_dest} (shutil) · sheets: {wb.sheetnames}")
    except Exception as e2:
        raise RuntimeError(
            f"Built the workbook at {_tmp} — it is complete, only the copy failed.\n"
            f"Could not place it at {_dest}.\n"
            f"  dbutils.fs.cp said: {e}\n"
            f"  shutil said       : {e2}\n"
            f"Check the out_path widget points at a volume you can write to, e.g. "
            f"/Volumes/dev_pathology_data/exports/ , and that the folder exists.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Now recalculate — the QA gate (§7.1)
# MAGIC ```
# MAGIC python3 /mnt/skills/public/xlsx/scripts/recalc.py <out_path> 270
# MAGIC ```
# MAGIC Zero formula errors, or the gate has not passed.