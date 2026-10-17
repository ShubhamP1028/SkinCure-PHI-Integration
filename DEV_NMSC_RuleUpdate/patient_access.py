#!/usr/bin/env python3
"""The three Patient Access attributes, and August from the All-Accounts report.

Both the workbook and the deck need the same three facts about an account and the same
August figures, so they are loaded here once rather than twice with two chances to
disagree.

WHY AUGUST MOVED SOURCE
    The pipeline's August came from `trend.csv`, pulled while the month was still
    landing: 2,511 verifications against the All-Accounts file's 3,284, short by 773
    across 166 accounts. July agrees to within one on the same basis (2,691 vs 2,692),
    so this is a cut-off, not a definition. August is therefore read from the report and
    July is left alone.
"""
import os, re
import pandas as pd

ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

# The lever column is free text and has drifted into near-duplicates -- "NMSC to Marked"
# and "NMSC to Mark", "Marked to VOB" and "Mark to VOB", "Both top half" and "both top
# half". Collapse to the five real answers; a blank stays blank rather than being given
# a value it was never assigned.
LEVER_CANON = {
    "nmsc to marked": "NMSC → Marked", "nmsc to mark": "NMSC → Marked",
    "marked to vob": "Marked → VOB",   "mark to vob": "Marked → VOB",
    "vob to sim": "VOB → Sim",
    "both top half": "Both top half",
    "flat": "Flat", "flat on nmsc to marked": "Flat",
}


def normalise_lever(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    t = re.sub(r"\s+", " ", str(v)).strip()
    if not t or t.lower() in ("n/a", "na", "none"):
        return None
    return LEVER_CANON.get(t.lower(), t)


def attributes(xlsx, sheet="Account Summary", header=1):
    """{account_name: {"transition":…, "discussion":…, "lever":…}}.

    Account Transition is taken from THIS file, not the All-Accounts export: the two
    disagree in wording ("Normal Operations - PCA Program" against "Normal Operations,
    CS") and this is the more recent of the two.
    """
    if not xlsx or not os.path.exists(xlsx):
        return {}
    d = pd.read_excel(xlsx, sheet_name=sheet, header=header)
    cols = {c.strip().lower(): c for c in d.columns if isinstance(c, str)}
    c_acct = cols.get("account")
    c_tr   = cols.get("account transition")
    c_dd   = cols.get("data discussion complete")
    c_lv   = cols.get("projected growth?") or cols.get("lever")
    out = {}
    for _, r in d.iterrows():
        a = r.get(c_acct)
        if not isinstance(a, str) or not a.strip():
            continue
        dd = r.get(c_dd) if c_dd else None
        out[a.strip()] = dict(
            transition=(None if c_tr is None or pd.isna(r.get(c_tr)) else str(r[c_tr]).strip()),
            discussion=(None if dd is None or (isinstance(dd, float) and pd.isna(dd))
                        else ("Complete" if bool(dd) else "Not complete")),
            lever=normalise_lever(r.get(c_lv) if c_lv else None))
    return out


def august(report_csv, export_dir, month="2026-08"):
    """{account_name: (vob, sim)} for the month after the window, from the report.

    Pooled folders hold more than one AccountId and the report is one row per id, so the
    ids are summed -- the same rule the VOB/SIM rollup uses. The id sets come from
    vob_scope, which is where the rollup pools them.
    """
    if not report_csv or not os.path.exists(report_csv):
        return None, None
    y = pd.read_csv(report_csv, low_memory=False)
    cv, cs = f"VOB_{month}", f"SIM_{month}"
    if cv not in y.columns or cs not in y.columns:
        return None, None
    y = y.set_index("account_id")
    vs = pd.read_csv(os.path.join(export_dir, "vob_scope.csv"), low_memory=False)
    ids = vs.groupby("account")["account_id"].unique().to_dict()
    s = pd.read_csv(os.path.join(export_dir, "account_summary.csv"), low_memory=False)
    out = {}
    for _, a in s.iterrows():
        n = a["account_name"]
        idl = [i for i in ids.get(n, [a["account_id"]]) if i in y.index]
        if not idl:
            continue
        out[n] = (int(y.loc[idl, cv].sum()), int(y.loc[idl, cs].sum()))
    return ORDER[int(month[5:7]) - 1], out
