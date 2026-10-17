#!/usr/bin/env python3
"""Pam's Summary, in Pam's own layout.

This rebuilds `Pathology Data w. KPIs` exactly as she keeps it: ten sheets, 53 columns,
band labels on row 1, headers on row 2, data from row 3, and the goal engine driven by
the CONVERSION GOAL ASSUMPTIONS block at AX/AY rather than by anything hardcoded here.

    python3 build_pam_v2.py <export_dir> --prior <last month's xlsx> \\
                            --from-month Apr -o Pathology_Data_w_KPIs.xlsx

WHY EVERY RATE IS STILL A FORMULA
    AF, AI and AL are the goal columns, and each one reads the assumptions block --
    row 2 the base goal, row 3 the stretch, row 7 the outside-referral floor, row 8 the
    IPM threshold, row 12 the month count. The COLUMN is derived, never typed, so the
    references follow the block if a column ahead of it is added or removed. Pam changes
    a goal by editing that block, not by asking for a rebuild. Freezing those as values would take that away.

WHAT IS CARRIED FROM LAST MONTH'S WORKBOOK
    D  Mohs                          - practice attribute, not in the export
    F-I the four process columns     - Pam maintains these by hand
    Goals sheet                      - the documented target logic
    Anything carried is keyed on the account name and left blank when the name is new,
    never fuzzy-matched: handing one practice another's clinical workflow would be
    worse than an empty cell.

MONTHS
    The window moved from Mar-Jun to Apr-Jul, so J-M, O-R, T-W and Y-AB carry the new
    month names. `Months in data period` at AY12 follows the count, so the per-month
    columns stay right when the window is a different length.
"""
import argparse, os, re, sys, warnings
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.formatting.rule import DataBarRule
from openpyxl.utils import get_column_letter as L
import patient_access as PA

warnings.filterwarnings("ignore")
ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

HDR_FILL = PatternFill("solid", fgColor="1F4E78")
HDR_FONT = Font(name="Arial", size=9, bold=True, color="FFFFFF")
# Pam's band labels are bold AND underlined, on no fill.
BAND_FONT = Font(name="Arial", size=9, bold=True, underline="single")
# Her header row is not one colour. Four groups, resolved from the theme her file
# carries (accent1 4F81BD, accent5 4BACC6) at the tints the cells specify:
#   navy   1F4E78  white ink  - account info, the monthly count columns, the tail
#   pale   DCE6F2  black ink  - the four band TOTALS
#   teal   388195  black ink  - the conversion RATES and Current Avg Sims
#   aqua   B7DEE8  black ink  - the goal and yield columns beside each rate
#   note   1F497D  white ink  - Notes
# The body carries one fill only, DCE6F2, on the totals and the rate/yield columns.
# Totals and conversions therefore share a body colour in her file; what separates
# them to the eye is the header above them and the bold on the totals.
HF_PALE = PatternFill("solid", fgColor="DCE6F2")
HF_TEAL = PatternFill("solid", fgColor="388195")
HF_AQUA = PatternFill("solid", fgColor="B7DEE8")
HF_NOTE = PatternFill("solid", fgColor="1F497D")
HDR_DARK = Font(name="Arial", size=9, bold=True)          # black ink on a light header
BODY_FILL = PatternFill("solid", fgColor="DCE6F2")
BODY = Font(name="Arial", size=9)
# Pam bolds the four band totals, the four conversion rates and the SIM YIELD
# trio, greens the VOB-close-to-Sim column and blues the assumption values.
# Read off the 8.5.26 backup's own cell styles, not chosen here.
BOLD  = Font(name="Arial", size=9, bold=True)
GREEN = Font(name="Arial", size=9, color="FF008000")
BLUE  = Font(name="Arial", size=9, color="FF0000FF")
THIN = Side(style="thin", color="D9D9D9")
# Pam's sheet has no cell borders. BOX is kept as an empty Border so the
# twenty-odd call sites that assign it stay untouched and a future change of
# mind is one line here, not a sweep.
BOX = Border()
BANDS = {"ACCOUNT INFORMATION":"DDEBF7", "CLINCAL WORKFLOW":"E2EFDA", "NMSC":"DDEBF7",
         "MARKED":"E2EFDA", "VOB":"FCE4D6", "SIM":"FFF2CC", "NMSC→ Marked":"EDEDED",
         "Marked → VOB":"EDEDED", "VOB → Sim":"EDEDED", "SIM YIELD":"E4DFEC",
         "CONVERSION GOAL ASSUMPTIONS":"F2F2F2"}

ASSUMPTIONS = [
    ("NMSC → Marked: base goal", 0.75),
    ("NMSC → Marked: stretch add (if above base)", 0.03),
    ("Marked → VOB: base goal", 0.85),
    ("Marked → VOB: stretch threshold", 0.80),
    ("Marked → VOB: stretch add (cap 100%)", 0.05),
    ("Marked → VOB: Lead / outside-referral band starts", 1.0),
    ("Marked → VOB: IPM threshold", 1.35),
    ("VOB → Sim: base goal", 0.70),
    ("VOB → Sim: stretch threshold", 0.66),
    ("VOB → Sim: stretch add (cap 100%)", 0.05),
    ("Months in data period", None),          # filled from the window
]


def sanitize(v):
    """LibreOffice evaluates a leading =, + or @ as a formula."""
    if isinstance(v, str) and v[:1] in "=+@-":
        return "'" + v
    return v


def months_of(df):
    pat = re.compile(r"^nmsc_([A-Z][a-z]{2})$")
    return sorted([pat.match(c).group(1) for c in df.columns if pat.match(c)],
                  key=ORDER.index)


# ------------------------------------------------------------------ prior workbook
def carry(prior_path):
    """The columns only Pam has: Mohs and the four clinical-workflow answers."""
    if not prior_path or not os.path.exists(prior_path):
        return {}, {}
    ws = load_workbook(prior_path, data_only=True)["Account Summary"]
    hdr = {ws.cell(2, c).value: c for c in range(1, ws.max_column + 1)
           if ws.cell(2, c).value}
    want = ["Mohs", "NMSC Result Presentation", "NMSC Results Delivered By",
            "Treatment Option Presentation", "GentleCure Consult Script", "Notes"]
    out = {}
    for r in range(3, ws.max_row + 1):
        a = ws.cell(r, 1).value
        if not a:
            continue
        out[a] = {k: (ws.cell(r, hdr[k]).value if k in hdr else None) for k in want}
    goals = []
    if "Goals" in load_workbook(prior_path, data_only=True).sheetnames:
        g = load_workbook(prior_path, data_only=True)["Goals"]
        goals = [[g.cell(r, c).value for c in range(1, 6)]
                 for r in range(1, g.max_row + 1)]
    return out, goals


def resolve(name, keys):
    """Exact, then the pooled-folder variants. Never fuzzy — see the module docstring."""
    if name in keys:
        return name
    for cand in (name.replace(" #1 & 2", " #1"), name.replace(" #1 & 2", " #2"),
                 name.replace(" & 2", "")):
        if cand in keys:
            return cand
    return None


# ------------------------------------------------------------------ derived sheets
def vob_to_sim_90d(export):
    """Avg days from VOB close to simulation, capped at 90.

    Column AW VLOOKUPs this sheet, so it has to exist and be keyed on the account name
    exactly as the summary spells it. Built from gold.vob_scope, which carries
    vob_created and simulation_scheduled and no patient identifiers.
    """
    v = pd.read_csv(os.path.join(export, "vob_scope.csv"), low_memory=False)
    vc = pd.to_datetime(v["vob_created"], errors="coerce")
    ss = pd.to_datetime(v["simulation_scheduled"], errors="coerce")
    d  = (ss - vc).dt.days
    ok = v.assign(days=d)[(d >= 0) & (d <= 90)]
    g  = ok.groupby("account")["days"].agg(["count", "sum", "mean"]).reset_index()
    g.columns = ["Account Name", "Records", "Total Days", "Avg Days VOB Close → Sim"]
    g["Avg Days VOB Close → Sim"] = g["Avg Days VOB Close → Sim"].round(1)
    return g.sort_values("Account Name")


def vob_to_sim_report(export, report_csv, month, derived):
    """VOB-close-to-Sim taken verbatim from the Salesforce YTD report.

    The YTD extract carries AVG_Days_VOB_Closed-Sim_<yyyy-mm> per AccountId, which is the
    number the business already publishes. Until 08b extracts it natively this reads the
    single requested month straight off the report.

    The report's value is taken as it stands for the 178 accounts that are one AccountId.
    The six pooled folders are the exception: one shared path log, two AccountIds, and no
    way to attribute a verification to one machine rather than the other, so the row
    carries the STRAIGHT mean of the two reported figures. Straight, not weighted by that
    month's simulations -- the two ids are being treated as one site doing one body of
    work, which is the same reason the folder is pooled in the first place.

    The one thing that is not taken verbatim is a zero. 168 of the report's 483 rows read
    0.0 for the month, and every one of them has zero simulations that month, so zero
    means "nothing closed", not "closed same day". Those carry the literal "n/a" so AW
    reads n/a rather than a fabricated 0. A pooled folder where only one id closed
    anything averages the one real figure rather than dragging it toward zero.
    """
    # [3 Sep] When the export carries `avg_days_vob_close_to_sim`, notebook 10 already
    # sourced this from the warehouse -- same metric, same filters, and a month with
    # nothing closed is ABSENT rather than a zero. Prefer it and skip the CSV entirely.
    _s = pd.read_csv(os.path.join(export, "account_summary.csv"), low_memory=False)
    if "avg_days_vob_close_to_sim" in _s.columns:
        der = dict(zip(derived["Account Name"], derived["Avg Days VOB Close → Sim"]))
        rows = []
        for _, a in _s.iterrows():
            v = a["avg_days_vob_close_to_sim"]
            val = round(float(v), 1) if pd.notna(v) else "n/a"
            d = der.get(a["account_name"])
            rows.append([a["account_name"], 1, None, val, None,
                         None if d is None or pd.isna(d) else round(float(d), 1),
                         round(val - d, 1) if isinstance(val, float) and d is not None
                         and pd.notna(d) else None])
        g = pd.DataFrame(rows, columns=[
            "Account Name", "AccountIds", f"Sims {month}", "Avg Days VOB Close → Sim",
            "Pooled ids averaged", "Pipeline 90d (vob_scope)", "Delta vs pipeline"])
        print(f"  VOB→Sim taken from the deck feed ({len(rows)} accounts, "
              f"{sum(1 for r in rows if r[3] == 'n/a')} n/a) — the report CSV was not read")
        return g.sort_values("Account Name")

    y = pd.read_csv(report_csv, low_memory=False)
    col, sim = f"AVG_Days_VOB_Closed-Sim_{month}", f"SIM_{month}"
    for c in (col, sim):
        if c not in y.columns:
            raise SystemExit(f"{report_csv} has no column {c}")
    y = y.set_index("account_id")

    s = pd.read_csv(os.path.join(export, "account_summary.csv"), low_memory=False)
    # the pooled id sets come from vob_scope, the same place the VOB/SIM rollup pools them
    vs = pd.read_csv(os.path.join(export, "vob_scope.csv"), low_memory=False)
    ids = vs.groupby("account")["account_id"].unique().to_dict()
    der = dict(zip(derived["Account Name"], derived["Avg Days VOB Close → Sim"]))

    rows, unmatched = [], []
    for _, a in s.iterrows():
        acct, aid = a["account_name"], a["account_id"]
        id_list = [i for i in ids.get(acct, [aid]) if i in y.index] or []
        if not id_list:
            unmatched.append(acct)
        vals = [round(float(y.at[i, col]), 1) for i in id_list
                if float(y.at[i, col]) > 0 and float(y.at[i, sim]) > 0]
        sims = sum(int(y.at[i, sim]) for i in id_list) if id_list else None
        val = round(sum(vals) / len(vals), 1) if vals else "n/a"
        d = der.get(acct)
        # the value stays in column D -- AW VLOOKUPs $A:$D,4 and so may Pam's own
        # references, so audit columns go AFTER it, never in front of it
        rows.append([acct, len(id_list), sims, val,
                     " + ".join(f"{v:g}" for v in vals) if len(vals) > 1 else None,
                     None if d is None or pd.isna(d) else round(float(d), 1),
                     round(val - d, 1) if isinstance(val, float) and d is not None
                     and pd.notna(d) else None])
    if unmatched:
        print(f"  ** {len(unmatched)} account(s) not in {os.path.basename(report_csv)} "
              f"— VOB→Sim reads n/a: {', '.join(unmatched[:5])}")
    g = pd.DataFrame(rows, columns=[
        "Account Name", "AccountIds", f"Sims {month}", "Avg Days VOB Close → Sim",
        "Pooled ids averaged", "Pipeline 90d (vob_scope)", "Delta vs pipeline"])
    return g.sort_values("Account Name")


def timing_sheet(s, v2s):
    """Results → VOB open, VOB open → Sim, and the two added together."""
    m = dict(zip(v2s["Account Name"], v2s["Avg Days VOB Close → Sim"]))
    rows = []
    for _, a in s.iterrows():
        r2v = a.get("avg_days_result_to_vob")
        v2s_d = m.get(a["account_name"])
        # v2s_d is "n/a" for the accounts with no simulation in the reported month
        num = isinstance(v2s_d, (int, float)) and not pd.isna(v2s_d)
        tot = round(r2v + v2s_d, 1) if pd.notna(r2v) and num else None
        rows.append([a["account_name"],
                     None if pd.isna(r2v) else round(float(r2v), 1),
                     v2s_d, tot])
    return pd.DataFrame(rows, columns=["Account", "Results to VOB Open",
                                       "VOB Open to Sim", "Results to Sim"])


def growth_month(export, last_window_month):
    """VOB and SIM for the month AFTER the window closes, from trend.csv.

    The window is exclusive of its end date, so August has no column in the summary and
    none in sf_rollup either. trend.csv carries it because it keys on Salesforce
    vob_created rather than on the biopsy window. Returns (label, {acct: (vob, sim)})
    or (None, None) when the month after the window is not in the file.
    """
    p = os.path.join(export, "trend.csv")
    if not os.path.exists(p):
        return None, None
    t = pd.read_csv(p, low_memory=False)
    if not {"month", "sf_vob", "sf_sim"} <= set(t.columns):
        return None, None
    cur = max(t["month"].astype(str))
    y, m = int(cur[:4]), int(cur[5:7])
    prior = f"{y-1}-12" if m == 1 else f"{y}-{m-1:02d}"
    if ORDER[int(prior[5:7]) - 1] != last_window_month:
        return None, None
    g = (t[t["month"].astype(str) == cur]
         .groupby("account_name")[["sf_vob", "sf_sim"]].sum())
    return ORDER[m - 1], {a: (int(r.sf_vob), int(r.sf_sim)) for a, r in g.iterrows()}


def vob_pattern(export, MS):
    """Columns E and AR — Pam's VOB-pattern taxonomy, not the pipeline's band.

    These are two different classifications and only one of them belongs on her sheet.
    The pipeline's `vob_pattern_band` (Standard / IPM / Lead / Insufficient) tests the
    RATIO of matched VOBs to marked patient-days. Pam's column answers a different
    question -- does the VOB come before the mark or after it -- and is documented in
    01_Counting_Rules.md §10:

        nm_pct = VOB rate among NEVER-MARKED appropriate patients
        mk_pct = VOB rate among MARKED appropriate patients
        index  = nm_pct / mk_pct

        < 5 never-marked or < 5 marked, or a zero control -> Insufficient data
        nm_pct >= 0.40                                    -> Blanket - VOB at biopsy
        nm_pct < 0.15 and index < 0.35                    -> SRT-triggered - VOB after interest
        anything between                                  -> Mixed / transitional

    Per PATIENT, not per patient-day: "never SRT-marked" is a property of the patient,
    and a patient with one VOB across several biopsy days cannot have it attributed to
    one day without bias.

    Reproduces 168 of her 177 labels (95%); the nine that move are all borderline and
    the window shifted a month, which is the expected amount of drift.
    """
    p = os.path.join(export, "appropriate_patients.csv")
    if not os.path.exists(p):
        return {}
    ap = pd.read_csv(p, low_memory=False)
    keep = {f"2026-{ORDER.index(m)+1:02d}" for m in MS}
    ap = ap[ap["biopsy_date"].astype(str).str[:7].isin(keep)]
    g = (ap.groupby(["account_name", "patient_id"])
           .agg(marked=("marked", "any"), vob=("has_vob", "any")).reset_index())
    out = {}
    for acct, sub in g.groupby("account_name"):
        nm, mk = sub[~sub.marked], sub[sub.marked]
        nm_n, mk_n = len(nm), len(mk)
        nm_pct = int(nm.vob.sum()) / nm_n if nm_n else None
        mk_pct = int(mk.vob.sum()) / mk_n if mk_n else None
        if nm_n < 5 or mk_n < 5 or not mk_pct or nm_pct is None:
            out[acct] = "Insufficient data"
        elif nm_pct >= 0.40:
            out[acct] = "Blanket - VOB at biopsy"
        elif nm_pct < 0.15 and (nm_pct / mk_pct) < 0.35:
            out[acct] = "SRT-triggered - VOB after interest"
        else:
            out[acct] = "Mixed / transitional"
    return out


def timing_lag(s):
    """The mean biopsy-to-pathology-result interval, for rescaling AV onto AU's basis.

    AU is result -> VOB. AV is biopsy -> VOB, and the biopsy always precedes the result,
    so AV runs longer than AU by the reporting lag. Averaging the two, or falling back
    from one to the other without adjustment, silently mixes two different clocks.

    Measured on the 167 accounts carrying both: mean 5.7 days, median 5.0, IQR 3.7-6.9.
    The median is used -- four accounts have AV below AU, which cannot be true of a real
    interval and drags a mean.
    """
    both = s[s.avg_days_result_to_vob.notna() & s.avg_days_biopsy_to_vob.notna()]
    if not len(both):
        return 0.0
    return float((both.avg_days_biopsy_to_vob - both.avg_days_result_to_vob).median())


def outside_referral(export, MS):
    """The share of a clinic's verifications whose BIOPSY sits somewhere else.

    Pam's VOB-pattern column says WHICH WAY the process runs (does the VOB precede the
    mark or follow it). It cannot say how much of a site's volume is referred in. The
    pipeline's own band could — it put Kim Ross La Vernia in `Lead / outside referral`
    — but that band is a ratio test (VOBs over marked patient-days) and reads as a
    conversion rate, which is exactly how it gets misread: La Vernia's Marked → VOB is
    168%, and nobody converts 168% of anything.

    So this measures it directly. A VOB is an OUTSIDE REFERRAL when its biopsy matched
    at any other folder we hold -- a practice-group sibling OR a practice outside the
    group. Not just sisters: the question is whether the biopsy happened here.

        outside = sister site + enterprise
        scoped  = own site + sister site + enterprise   (ATTRIBUTABLE only)

    Worked example, Apr-Jul: La Vernia raises 36 VOBs, 15 with a biopsy in its own log
    and 12 at a sibling -- Seguin 8, San Antonio 4. That is 33% referred in. Restricted
    to verifications it owns both ends of, its Marked → VOB is 68%, under the 85% goal
    rather than double it. Seguin, the group's biopsy site, reads 7%.

    [2 Sep, Becky] `no biopsy found` and `ambiguous namesake` are OUT of the denominator.
    Neither is evidence about where a biopsy happened -- one found no biopsy at all, the
    other found a name match nowhere trustworthy -- so including them answered a question
    nobody asked ("what share of all verifications are known referrals") instead of the
    one that matters ("of the verifications we can place, how many came from elsewhere").
    It reads higher now, and the rate is only computed over VOBs that actually matched.
    """
    p = os.path.join(export, "vob_scope.csv")
    if not os.path.exists(p):
        return {}
    v = pd.read_csv(p, low_memory=False)
    keep = {f"2026-{ORDER.index(m)+1:02d}" for m in MS}
    cm = v["cohort_month"].fillna(v["vob_created"].astype(str).str[:7])
    w  = v[cm.isin(keep)]
    att = w[w.match_scope.isin(["own site", "sister site", "enterprise"])]
    tot = att.groupby("account")["vob_id"].nunique()
    out = (att[att.match_scope.isin(["sister site", "enterprise"])]
           .groupby("account")["vob_id"].nunique())
    return {a: (int(tot[a]), int(out.get(a, 0))) for a in tot.index}


def notes_for(a, MS):
    """The short diagnosis in column AQ, in last month's vocabulary.

    Regenerated rather than carried: a note that says "low appropriate volume" about a
    practice that has since doubled is worse than no note.
    """
    n   = len(MS)
    NM  = sum(int(a[f"nmsc_{m}"]) for m in MS)
    MK  = sum(int(a[f"marked_{m}"]) for m in MS)
    VB  = sum(int(a[f"sf_vob_{m}"]) for m in MS)
    SM  = sum(int(a[f"sf_sim_{m}"]) for m in MS)
    band = a.get("_pattern") or a.get("vob_pattern_band")
    if str(a.get("status", "")).lower().startswith("terminat"):
        return "Terminated - retained for reconciliation"
    if str(band).startswith("Blanket"):
        return "Blanket process - VOB at biopsy, marking happens after"
    if str(band).startswith("Mixed"):
        return "Mid-transition between blanket and SRT-triggered"
    if NM / n < 25:
        return "Low appropriate volume - build top of funnel"
    if MK and MK / NM >= 0.90:
        return "Marks nearly everyone - marking not a useful lever"
    if MK and VB / MK < 0.85:
        return "Marked patients not reaching a VOB - open the VOB step"
    if VB and SM / VB < 0.70:
        return "VOBs not closing to simulation - work the bedside"
    if str(a.get("_script")) == "Clinic Own Wording":
        return "On own wording - move to GentleCure approved script"
    return "Every stage at or near goal - hold the pace"


# ------------------------------------------------------------------ the workbook
# Fairview matched 3 patients by hand against the latest path-log pull that the
# pipeline did not match. Held here as an explicit, dated override rather than edited
# into the sheet, so it is visible, reversible, and disappears the moment the matcher
# is fixed. See Fairview_Match_Gap_02Sep2026.
MATCH_OVERRIDE = {
    "Fairview Dermatology, PLLC - Fairview": {
        "own_site_matched": 3,
        "note": "REVIEW AFTER MONTH-END — review the VOB date window and the data pull "
                "for this account. 3 own-site matches found by hand against the latest "
                "pull, 0 found by the pipeline; 5 more log rows read 'name appears on a "
                "VOB out of window'. Path log is CSV. Attributable and outside-referral % "
                "adjusted here; no other column is affected.",
    },
}


def terminated(report_csv, keep=()):
    """Account names to leave out of the summary, from the YTD report's status column.

    Sourced rather than inferred -- the report is the system of record for whether an
    account is still live. Anything in `keep` stays in despite being terminated; Anchorage
    is kept because it may be reinstated and its path-log numbers are wanted.
    """
    if not report_csv or not os.path.exists(report_csv):
        return set(), set()
    y = pd.read_csv(report_csv, low_memory=False)
    if "status" not in y.columns:
        return set(), set()
    dead = set(y.loc[y.status.astype(str).str.strip() != "Active", "account_id"])
    kept = {k.lower() for k in keep}
    return dead, kept


def build(export, out, prior=None, from_month=None, status_csv=None,
          keep_terminated=("anchorage",), marked_csv=None, pa_xlsx=None):
    s  = pd.read_csv(os.path.join(export, "account_summary.csv"), low_memory=False)
    sf = pd.read_csv(os.path.join(export, "sf_rollup.csv"), low_memory=False)
    MS = months_of(s)
    if from_month:
        MS = MS[MS.index(from_month):]
    n = len(MS)
    s = s.sort_values("account_name").reset_index(drop=True)

    # terminated accounts leave the SUMMARY only. Their rows stay in Raw-Unfiltered and
    # Appropriate Patients so the extract still reconciles against the watchlist and no
    # path-log evidence is lost -- only the pages that present an account as an
    # opportunity drop them.
    dead, kept = terminated(status_csv, keep_terminated)
    if dead:
        drop = s.account_id.isin(dead) & ~s.account_name.str.lower().apply(
            lambda nm: any(k in nm for k in kept))
        if drop.any():
            print(f"  terminated, dropped from the summary ({int(drop.sum())}):")
            for nm in s.loc[drop, "account_name"]:
                print(f"        {nm}")
            keptn = s.account_id.isin(dead) & ~drop
            for nm in s.loc[keptn, "account_name"]:
                print(f"  terminated, KEPT by request: {nm}")
            s = s[~drop].reset_index(drop=True)

    # markings that sit in a practice-group sibling's log, recovered by marked_elsewhere.py
    MKE = {}
    if marked_csv and os.path.exists(marked_csv):
        me = pd.read_csv(marked_csv)
        MKE = dict(zip(me.account_name, me.marked_elsewhere.fillna(0).astype(int)))
    CARRY, GOALS = carry(prior)
    ck = set(CARRY)
    # August from the All-Accounts report when one is supplied -- trend.csv was pulled
    # mid-month and is 773 verifications short across 166 accounts. July is untouched.
    GM, GVAL = (None, None)
    if status_csv:
        GM, GVAL = PA.august(status_csv, export)
    if not GM:
        GM, GVAL = growth_month(export, MS[-1])
    PAX = PA.attributes(pa_xlsx)
    LAG = timing_lag(s)
    PATTERN = vob_pattern(export, MS)
    OUTREF  = outside_referral(export, MS)
    if not PATTERN:
        print("  ** appropriate_patients.csv missing — columns E/AR fall back to the "
              "pipeline band, which is NOT Pam's taxonomy")

    wb = Workbook(); ws = wb.active; ws.title = "Account Summary"

    # ---- the column plan, in Pam's order. (band, header)
    cols  = [("ACCOUNT INFORMATION","Account"), (None,"Regional"), (None,"Senior"),
             (None,"Mohs"), (None,"VOB pattern (analysis)"),
             # [2 Sep, Becky] from the Patient Access file, inside the account band
             (None,"Data Discussion Complete"), (None,"Account Transition"),
             (None,"Lever")]
    cols += [("CLINCAL WORKFLOW","NMSC Result Presentation"),
             (None,"NMSC Results Delivered By"), (None,"Treatment Option Presentation"),
             (None,"GentleCure Consult Script")]
    # J is where the monthly counts start. DERIVED, never typed: three columns went
    # into the account band and every letter after them moves, so a hardcoded 10 would
    # have written the months over the clinical-workflow answers.
    _J = len(cols) + 1
    cols += [("NMSC", f"{MS[0]} NMSC")] + [(None, f"{m} NMSC") for m in MS[1:]] + [(None,"Total NMSC")]
    cols += [("MARKED", f"{MS[0]} SRT-Marked")] + [(None, f"{m} SRT-Marked") for m in MS[1:]] + [(None,"Total Marked")]
    cols += [("VOB", f"{MS[0]} VOB")] + [(None, f"{m} VOB") for m in MS[1:]] + [(None,"TOTAL VOB")]
    cols += [("SIM", f"{MS[0]} SIM")] + [(None, f"{m} SIM") for m in MS[1:]] + [(None,"Total SIM")]
    cols += [(None,"NMSC→ VOB")]
    cols += [("NMSC→ Marked","NMSC→ Marked"), (None,"Goal: NMSC → Marked"),
             (None," +Sims/mo if NMSC → Marked hits goal")]
    cols += [("Marked → VOB","Marked → VOB"), (None,"Goal: Marked → VOB"),
             (None," +Sims/mo if Marked → VOB hits goal")]
    cols += [("VOB → Sim","VOB → Sim"), (None,"Goal: VOB → Sim"),
             (None,"Yield: +Sims/mo if VOB → Sim hits goal")]
    cols += [("SIM YIELD","Current Avg Sims/mo"), (None,"Total Yield: +Sims/mo @ all goals"),
             (None,"Avg Sims/mo @ all goals"), (None,"Notes"),
             (None,"VOB pattern (analysis)"), (None,"Pathlog utilization"),
             (None,"Actions/Plans coverage"),
             # AU and AV collapsed into ONE column on ONE clock. 167 accounts carry a
             # measured result-to-VOB figure; the 16 that do not get their biopsy-to-VOB
             # figure less the 5-day median reporting lag, so the column is comparable
             # end to end. 16 of 184 are adjusted rather than measured -- the split is
             # recorded here because the column itself no longer says so.
             (None,"Avg Days Path Result → VOB"),
             (None,"Average Days from VOB Close to Sim")]

    # column letters, derived so nothing is counted by hand
    J = _J;  N_ = J + n
    O = N_ + 1; S_ = O + n
    T = S_ + 1; X = T + n
    Y = X + 1;  AC = Y + n
    AD = AC + 1                                   # NMSC→VOB
    AE, AF, AG = AD+1, AD+2, AD+3                 # NMSC→Marked block
    AH, AI, AJ = AD+4, AD+5, AD+6                 # Marked→VOB block
    AK, AL, AM = AD+7, AD+8, AD+9                 # VOB→Sim block
    AN, AO, AP = AD+10, AD+11, AD+12              # SIM YIELD
    AQ, AR, AS_, AT, AU, AW = AD+13, AD+14, AD+15, AD+16, AD+17, AD+18
    ASSUM = AW + 1
    # The goal columns read the assumptions by ABSOLUTE reference, so the letter has to
    # be derived, not typed. Dropping the timing-basis column moved this block from AY
    # to AX; a hardcoded reference would have pointed every band test at an empty cell and
    # AF/AI/AL would have returned the base goal for all 184 accounts, silently.
    AY = f"${L(ASSUM + 1)}$"
    # The growth block sits PAST the assumptions, at BB, so A..AW keep the letters
    # Pam's own formulas and filters already point at. Parked here on purpose until
    # its home is decided -- moving it into the VOB band later shifts every column
    # from X onward, which is a rewire, not a move.
    GB = ASSUM + 4                                # BB: Aug VOB
    GS, GVN, GVP, GSN, GSP = GB+1, GB+2, GB+3, GB+4, GB+5
    # BI-BK: the referral mix. The count and its denominator sit beside the percentage
    # so the ratio can be checked without leaving the row -- the same rule every other
    # rate on this sheet follows.
    OR_T, OR_N, OR_P = GSP+2, GSP+3, GSP+4
    # BM-BN: the sister-site marking recovered from the sibling's own log. Kept at the far
    # right so A..AW keep the letters Pam's formulas already point at -- but note that the
    # Marked -> VOB ratio at AH now divides by BN, not by Total Marked at S. The marked
    # COUNT at S is deliberately left alone: a marking credited here is also counted at the
    # site that made it, so summing an effective-marked column would double count the
    # enterprise total.
    MK_S, MK_E = OR_P+2, OR_P+3
    MKCOL = L(MK_E) if MKE else L(S_)

    if GM:
        cols += [(None,"")] * (GB - len(cols) - 1)          # AX..BA stay empty
        cols += [(f"{GM.upper()} — SALESFORCE (OUTSIDE THE WINDOW)", f"{GM} VOB"),
                 (None, f"{GM} SIM"),
                 (None, f"VOB Growth {MS[-1]} → {GM} (#)"),
                 (None, f"VOB Growth {MS[-1]} → {GM} (%)"),
                 (None, f"SIM Growth {MS[-1]} → {GM} (#)"),
                 (None, f"SIM Growth {MS[-1]} → {GM} (%)")]
        BANDS[f"{GM.upper()} — SALESFORCE (OUTSIDE THE WINDOW)"] = "FCE4D6"
    if OUTREF:
        cols += [(None, "")] * (OR_T - len(cols) - 1)
        cols += [("OUTSIDE REFERRAL", "Matched VOB (attributable)"),
                 (None, "Outside referral VOB"),
                 (None, "Outside referral %")]
        BANDS["OUTSIDE REFERRAL"] = "E4DFEC"
    if MKE:
        cols += [(None, "")] * (MK_S - len(cols) - 1)
        cols += [("MARKED AT A SISTER SITE", "Marked at sister site"),
                 (None, "Marked incl. sister (AH denominator)")]
        BANDS["MARKED AT A SISTER SITE"] = "DDEBF7"

    # ---- rows 1 and 2
    _HDRMAP = {}
    for _c in (N_, S_, X, AC):                     _HDRMAP[_c] = HF_PALE
    for _c in (AD, AE, AH, AK, AN):                _HDRMAP[_c] = HF_TEAL
    for _c in (AF, AG, AI, AJ, AL, AM, AO, AP):    _HDRMAP[_c] = HF_AQUA
    _HDRMAP[AQ] = HF_NOTE
    _BODY_FILL_COLS = {N_, S_, X, AC, AD, AE, AH, AK, AN, AO, AP}
    for i,(band,h) in enumerate(cols, 1):
        if not h and not band: continue
        if band:
            c = ws.cell(1, i); c.value = band; c.font = BAND_FONT
            c.alignment = Alignment(horizontal="center", vertical="center")
        c = ws.cell(2, i); c.value = h
        _f = _HDRMAP.get(i)
        if _f is None:
            c.font, c.fill = HDR_FONT, HDR_FILL
        elif _f is HF_NOTE:
            c.font, c.fill = HDR_FONT, HF_NOTE
        else:
            c.font, c.fill = HDR_DARK, _f
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = BOX
    # measured off the 8.5.26 backup
    ws.row_dimensions[1].height = 35.25
    ws.row_dimensions[2].height = 69
    # band merges are listed, not inferred. Deriving "this band runs to the column
    # before the next band" gets AD wrong -- NMSC -> VOB sits between the SIM and
    # NMSC -> Marked bands and belongs to neither, exactly as in Pam's file -- and it
    # would also swallow the assumptions column, whose cell is written later and would
    # then be a read-only MergedCell.
    BAND_SPANS = [(1, 8), (9, 12), (J, N_), (O, S_), (T, X), (Y, AC),
                  (AE, AG), (AH, AJ), (AK, AM), (AN, AP)]
    if GM:     BAND_SPANS.append((GB, GSP))
    if OUTREF: BAND_SPANS.append((OR_T, OR_P))
    if MKE:    BAND_SPANS.append((MK_S, MK_E))

    sfx = sf.set_index("account_name")
    miss_carry = []
    for r,(_,a) in enumerate(s.iterrows(), start=3):
        acct = a["account_name"]
        k = resolve(acct, ck)
        C = CARRY.get(k, {}) if k else {}
        if not k: miss_carry.append(acct)
        a = a.copy(); a["_script"] = C.get("GentleCure Consult Script")
        a["_pattern"] = PATTERN.get(acct)

        ws.cell(r, 1, sanitize(acct))
        ws.cell(r, 2, sanitize(a.get("regional")))
        ws.cell(r, 3, sanitize(a.get("senior")))
        ws.cell(r, 4, sanitize(C.get("Mohs")))
        ws.cell(r, 5, sanitize(PATTERN.get(acct, a.get("vob_pattern_band"))))
        # the three Patient Access facts, then the four workflow answers
        _pa = PAX.get(acct, {})
        ws.cell(r, 6, sanitize(_pa.get("discussion") or ("N/A" if PAX else None)))
        ws.cell(r, 7, sanitize(_pa.get("transition") or ("N/A" if PAX else None)))
        ws.cell(r, 8, sanitize(_pa.get("lever")))
        for j,h in enumerate(["NMSC Result Presentation","NMSC Results Delivered By",
                              "Treatment Option Presentation","GentleCure Consult Script"]):
            ws.cell(r, 9+j, sanitize(C.get(h)))
        for i,m in enumerate(MS):
            ws.cell(r, J+i, int(a[f"nmsc_{m}"]))
            ws.cell(r, O+i, int(a[f"marked_{m}"]))
            ws.cell(r, T+i, int(sfx.loc[acct, f"sf_vob_{m}"]) if acct in sfx.index else 0)
            ws.cell(r, Y+i, int(sfx.loc[acct, f"sf_sim_{m}"]) if acct in sfx.index else 0)
        ws.cell(r, N_, f"=SUM({L(J)}{r}:{L(J+n-1)}{r})")
        ws.cell(r, S_, f"=SUM({L(O)}{r}:{L(O+n-1)}{r})")
        ws.cell(r, X,  f"=SUM({L(T)}{r}:{L(T+n-1)}{r})")
        ws.cell(r, AC, f"=SUM({L(Y)}{r}:{L(Y+n-1)}{r})")
        # the goal engine, verbatim from last month's workbook
        ws.cell(r, AD, f"=IFERROR({L(X)}{r}/{L(N_)}{r},0)")
        ws.cell(r, AE, f"=IFERROR({L(S_)}{r}/{L(N_)}{r},0)")
        ws.cell(r, AF, f'=IF(OR({L(N_)}{r}=0,{L(S_)}{r}=0,{L(AH)}{r}>{AY}8),"n/a",'
                       f'IF({L(AE)}{r}>{AY}2,MIN(1,{L(AE)}{r}+{AY}3),{AY}2))')
        ws.cell(r, AG, f"=IF(ISNUMBER({L(AF)}{r}),MAX(0,({L(N_)}{r}*{L(AF)}{r}*"
                       f"MIN(1,{L(AH)}{r})*MIN(1,{L(AK)}{r})-{L(AC)}{r})/{AY}12),0)")
        # Marked -> VOB divides by own marked PLUS any marking made at the sibling that
        # biopsied the patient, so a treatment site is measured against the work it is
        # actually converting. MKCOL is S when no marked file was supplied.
        ws.cell(r, AH, f"=IFERROR({L(X)}{r}/{MKCOL}{r},0)")
        ws.cell(r, AI, f'=IF({L(S_)}{r}=0,"n/a",IF({L(AH)}{r}>{AY}8,"IPM",'
                       f'IF({L(AH)}{r}>={AY}7,"Lead / outside referral",'
                       f'IF({L(AH)}{r}>{AY}5,MIN(1,{L(AH)}{r}+{AY}6),{AY}4))))')
        ws.cell(r, AJ, f"=IF(ISNUMBER({L(AI)}{r}),MAX(0,({L(S_)}{r}*{L(AI)}{r}*"
                       f"MIN(1,{L(AK)}{r})-{L(AC)}{r})/{AY}12),0)")
        ws.cell(r, AK, f"=IFERROR({L(AC)}{r}/{L(X)}{r},0)")
        ws.cell(r, AL, f'=IF({L(X)}{r}=0,"n/a",IF({L(AK)}{r}>{AY}10,'
                       f'MIN(1,{L(AK)}{r}+{AY}11),{AY}9))')
        ws.cell(r, AM, f"=IF(ISNUMBER({L(AL)}{r}),MAX(0,({L(X)}{r}*{L(AL)}{r}-"
                       f"{L(AC)}{r})/{AY}12),0)")
        ws.cell(r, AN, f"={L(AC)}{r}/{AY}12")
        ws.cell(r, AO, f"={L(AG)}{r}+{L(AJ)}{r}+{L(AM)}{r}")
        ws.cell(r, AP, f"={L(AN)}{r}+{L(AO)}{r}")
        _note = MATCH_OVERRIDE.get(acct, {}).get("note")
        _base = notes_for(a, MS)
        ws.cell(r, AQ, sanitize(f"{_note} {_base}".strip() if _note else _base))
        ws.cell(r, AR, sanitize(PATTERN.get(acct, a.get("vob_pattern_band"))))
        # AS keeps the pipeline's read: does this account annotate marking at all
        ws.cell(r, AS_, "IPM" if a.get("vob_pattern_band") == "IPM" else "Marks Patients")
        cov = a.get("actions_coverage")
        ws.cell(r, AT, None if pd.isna(cov) else float(cov))
        # AU first when it exists; otherwise AV brought onto AU's clock by removing the
        # biopsy-to-result lag. Never both, never averaged.
        _au, _av = a.get("avg_days_result_to_vob"), a.get("avg_days_biopsy_to_vob")
        if pd.notna(_au):
            ws.cell(r, AU, round(float(_au), 0))
        elif pd.notna(_av):
            ws.cell(r, AU, max(round(float(_av) - LAG, 0), 0))
        else:
            ws.cell(r, AU, None)
        ws.cell(r, AW, f"=IFERROR(VLOOKUP($A{r},'VOB to Sim (90d)'!$A:$D,4,FALSE),\"n/a\")")
        if GM:
            gv, gs = GVAL.get(acct, (0, 0))
            ws.cell(r, GB, gv); ws.cell(r, GS, gs)
            jv, js = L(T+n-1), L(Y+n-1)          # the window's last month
            ws.cell(r, GVN, f"={L(GB)}{r}-{jv}{r}")
            ws.cell(r, GVP, f'=IFERROR({L(GVN)}{r}/{jv}{r},"")')
            ws.cell(r, GSN, f"={L(GS)}{r}-{js}{r}")
            ws.cell(r, GSP, f'=IFERROR({L(GSN)}{r}/{js}{r},"")')
        if OUTREF:
            t, o = OUTREF.get(acct, (0, 0))
            # a hand-found own-site match raises the attributable denominator, never the
            # outside count -- the point of the override is that the VOB was NOT outside
            t += int(MATCH_OVERRIDE.get(acct, {}).get("own_site_matched", 0))
            ws.cell(r, OR_T, t); ws.cell(r, OR_N, o)
            ws.cell(r, OR_P, f'=IFERROR({L(OR_N)}{r}/{L(OR_T)}{r},"")')
        if MKE:
            mk = int(MKE.get(acct, 0))
            ws.cell(r, MK_S, mk)
            ws.cell(r, MK_E, f"={L(S_)}{r}+{L(MK_S)}{r}")

    last = 2 + len(s)
    for a0, b0 in BAND_SPANS:
        if b0 > a0:
            ws.merge_cells(start_row=1, start_column=a0, end_row=1, end_column=b0)
    # ---- the assumptions block, which the goal columns read
    ws.cell(1, ASSUM, "CONVERSION GOAL ASSUMPTIONS").font = BAND_FONT
    for i,(lbl,val) in enumerate(ASSUMPTIONS, start=2):
        ws.cell(i, ASSUM, lbl).font = BODY
        c = ws.cell(i, ASSUM+1, n if val is None else val)
        c.font = BLUE
        c.number_format = "0%" if isinstance(val, float) and val <= 1.5 else "General"

    # ---- formats
    _BOLD_COLS = {N_, S_, X, AC, AD, AE, AH, AK, AN, AO, AP}
    for r in range(3, last+1):
        for ci in range(1, AW+1):
            c = ws.cell(r, ci)
            c.font = BOLD if ci in _BOLD_COLS else (GREEN if ci == AW else BODY)
            c.border = BOX
            if ci in _BODY_FILL_COLS:
                c.fill = BODY_FILL
            if ci in (AD,AE,AF,AH,AI,AK,AL,AT): c.number_format = "0%"
            elif ci in (AG,AJ,AM,AN,AO,AP):     c.number_format = "0.0"
            elif J <= ci <= AC:                 c.number_format = "#,##0"
            # AU and AW are the two day-count columns (AW is the VOB-close-to-Sim
            # variable; it lands at spreadsheet column AV since the basis column went)
            elif ci in (AU,AW):                 c.number_format = "0.0"
    # Pam's own column widths, read off the 8.5.26 backup. Columns absent from her
    # file keep Excel's default rather than being given one.
    widths = {1:36.14, 2:10.0, 3:9.71, 4:6.14, 5:19.57,
              6:18.0, 7:26.0, 8:16.0, 9:12.86,
              J:6.29, N_:6.29, O:6.29, S_:6.29, T:6.29,
              AD:7.86, AE:7.43, AF:7.86, AH:7.14, AI:7.86, AK:7.86, AL:7.29,
              AN:8.71, AO:7.29, AQ:25.14, AR:14.29, AS_:12.86, AT:6.29,
              AU:12.43, AW:6.29, ASSUM:41.29, ASSUM+1:4.86}
    if GM:
        for r in range(3, last+1):
            for ci in (GB, GS, GVN, GSN):
                ws.cell(r, ci).number_format = ("#,##0" if ci in (GB, GS)
                                                else "+#,##0;-#,##0;0")
                ws.cell(r, ci).font = BODY; ws.cell(r, ci).border = BOX
            for ci in (GVP, GSP):
                ws.cell(r, ci).number_format = "+0%;-0%;0%"
                ws.cell(r, ci).font = BODY; ws.cell(r, ci).border = BOX
        for ci in range(GB, GSP+1):
            widths[ci] = 15
    if OUTREF:
        for r in range(3, last+1):
            for ci in (OR_T, OR_N):
                c = ws.cell(r, ci); c.number_format = "#,##0"
                c.font = BODY; c.border = BOX
            c = ws.cell(r, OR_P); c.number_format = "0%"; c.font = BODY; c.border = BOX
        for ci in (OR_T, OR_N, OR_P):
            widths[ci] = 16
    if MKE:
        for r in range(3, last+1):
            for ci in (MK_S, MK_E):
                c = ws.cell(r, ci); c.number_format = "#,##0"
                c.font = BODY; c.border = BOX
        widths[MK_S], widths[MK_E] = 16, 22
    _lastcol = MK_E if MKE else ((OR_P if OUTREF else GSP) if GM else ASSUM+1)
    for ci in range(1, _lastcol+1):
        if ci in widths:
            ws.column_dimensions[L(ci)].width = widths[ci]
    # The average-days column carries Pam's data bar: solid FFF16A6A, min to max, value
    # still shown. She has one on each of her two timing columns; this file collapsed
    # them into one, so there is one bar.
    ws.conditional_formatting.add(
        f"{L(AU)}3:{L(AU)}{last}",
        DataBarRule(start_type="min", end_type="max", color="F16A6A", showValue=True))
    ws.freeze_panes = "B3"
    ws.auto_filter.ref = f"A2:{L(AW)}{last}"
    return wb, s, MS, n, GOALS, miss_carry


# ------------------------------------------------------------------ support sheets
SUPPORT = [
    # (sheet name, export csv, hidden, header rename map)
    ("AVGDayResulttoVOBNoAuth", None, False, None),
    ("Goals",                   None, False, None),
    ("Raw-Unfiltered",          "raw_unfiltered.csv", False, {
        "account":"Account","source_file":"Source File","page":"Page","month":"Month",
        "date":"Date","patient":"Patient","mrn":"MRN","facility":"Facility",
        "location":"Location","result_pathology":"Result (Pathology)",
        "actions_and_plans":"Actions and Plans","comments_assigned_to":"Comments / Assigned To",
        "flags":"Flags","notifications":"Notifications","notes":"Notes","extra":"Extra",
        "appropriate_lesion":"Appropriate (lesion)","srt_signal_lesion":"SRT signal (lesion)",
        "match_type":"Match type",
        "any_vob_for_this_patient":"Any VOB for this patient (name only, ignores date)"}),
    ("VOB BreakDown(VOB Disposition)", "vob_disposition.csv", True, None),
    ("Appropriate Patients",    "appropriate_patients.csv", True, None),
    ("VOB mentioned - no VOB found", "vob_mentioned_no_vob.csv", True, None),
    ("FlagsReview",             "review.csv", True, None),
    ("AllAccounts",             "sf_rollup.csv", True, None),
    ("VOB to Sim (90d)",        None, False, None),
]


def add_frame(wb, name, df, hidden=False):
    w = wb.create_sheet(name[:31])
    w.append([str(c) for c in df.columns])
    for c in w[1]:
        c.font = HDR_FONT; c.fill = HDR_FILL
    for row in df.itertuples(index=False):
        w.append([sanitize(v) if isinstance(v, str) else (None if pd.isna(v) else v)
                  for v in row])
    w.freeze_panes = "A2"
    if hidden:
        w.sheet_state = "hidden"
    return w


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("export_dir")
    ap.add_argument("--prior", help="last month's workbook — Mohs, the four clinical "
                                    "workflow columns, and the Goals sheet")
    ap.add_argument("--from-month", metavar="Mmm")
    ap.add_argument("--v2s-report", metavar="CSV",
                    help="All_Account_YTD export — takes VOB-close-to-Sim from the "
                         "Salesforce report instead of deriving it from vob_scope")
    ap.add_argument("--v2s-month", default="2026-07", metavar="yyyy-mm",
                    help="which month of the report to use (default 2026-07)")
    ap.add_argument("--marked-elsewhere", metavar="CSV",
                    help="output of marked_elsewhere.py — adds the sibling's marking to "
                         "the Marked → VOB denominator")
    ap.add_argument("--patient-access", metavar="XLSX",
                    help="Patient Access & Growth Projections — Data Discussion, "
                         "Account Transition and Lever")
    ap.add_argument("--keep-terminated", default="anchorage,vincennes", metavar="SUBSTR",
                    help="comma-separated name fragments of terminated accounts to keep")
    ap.add_argument("-o", "--out", default="Pathology_Data_w_KPIs.xlsx")
    A = ap.parse_args()

    wb, s, MS, n, GOALS, miss = build(
        A.export_dir, A.out, A.prior, A.from_month,
        status_csv=A.v2s_report,
        keep_terminated=tuple(x.strip().lower() for x in A.keep_terminated.split(",") if x.strip()),
        marked_csv=A.marked_elsewhere, pa_xlsx=A.patient_access)

    v2s = vob_to_sim_90d(A.export_dir)
    if A.v2s_report:
        v2s = vob_to_sim_report(A.export_dir, A.v2s_report, A.v2s_month, v2s)
        _num = [v for v in v2s["Avg Days VOB Close → Sim"] if isinstance(v, float)]
        _dl  = v2s["Delta vs pipeline"].dropna()
        print(f"  VOB→Sim from report {os.path.basename(A.v2s_report)} "
              f"month {A.v2s_month}: {len(_num)} accounts valued, "
              f"{len(v2s) - len(_num)} n/a (no sim that month)")
        print(f"    median {pd.Series(_num).median():.1f} d · vs pipeline 90d: "
              f"median delta {_dl.median():+.1f} d, "
              f"{int((_dl.abs() > 10).sum())} account(s) more than 10 d apart")
    add_frame(wb, "AVGDayResulttoVOBNoAuth", timing_sheet(s, v2s))
    g = wb.create_sheet("Goals")
    for row in (GOALS or [["Conversion Stage","Target","Adjustment Logic",
                           "Additional Rules","Output"]]):
        g.append(row)
    for c in g[1]:
        c.font = HDR_FONT; c.fill = HDR_FILL
    for name, csvf, hidden, ren in SUPPORT:
        if csvf is None:
            continue
        p = os.path.join(A.export_dir, csvf)
        if not os.path.exists(p):
            print(f"  ** {csvf} missing — {name} left out")
            continue
        df = pd.read_csv(p, low_memory=False)
        if ren:
            df = df.rename(columns=ren)
        add_frame(wb, name, df, hidden)
    add_frame(wb, "VOB to Sim (90d)", v2s)

    wb.save(A.out)
    print(f"wrote {A.out}")
    print(f"  {len(s)} accounts · months {', '.join(MS)} · {len(wb.sheetnames)} sheets")
    print(f"  sheets: {', '.join(wb.sheetnames)}")
    if miss:
        print(f"  *** {len(miss)} account(s) not in last month's workbook — Mohs and the "
              f"four clinical-workflow columns are blank for them:")
        for x in miss:
            print(f"        {x}")


if __name__ == "__main__":
    main()
