#!/usr/bin/env python3
"""Turn notebook 11's `deck_feed` workbook into the CSVs the deck scripts already read.

The point of the feed is that Databricks states the contract once, in one file, with the
as-of date of every source attached. The point of THIS adapter is that adopting it costs
nothing downstream: `prep_scorecards.py` and `gen_copy.py` keep the exact shape they have
today, so the feed can be trialled for a month without a rewrite that would have to be
un-picked if it turned out to be the wrong seam.

What it writes, into an export-shaped folder:

    account_summary.csv   the deck's own columns, month names intact
    sf_rollup.csv         the same monthly VOB/SIM at folder level
    trend.csv             the window months PLUS the growth month, long-form

What it deliberately does NOT write: `vob_scope.csv`, `appropriate_patients.csv`,
`raw_unfiltered.csv`. Those carry patient identifiers and the deck does not need them --
only the workbook build and the sister-site checks do, and those keep reading notebook
10's exports. Keeping the two paths separate is what stops the deck's feed from quietly
becoming a PHI file.

    python3 feed_to_export.py deck_feed_<run>.xlsx -o export_dir/
"""
import argparse, os
import pandas as pd

ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("feed", help="deck_feed workbook from notebook 11")
    ap.add_argument("-o", "--out", required=True, help="export directory to write into")
    A = ap.parse_args()

    f = pd.read_excel(A.feed, sheet_name="accounts")
    try:
        prov = pd.read_excel(A.feed, sheet_name="sources")
        print("SOURCES")
        print(prov.to_string(index=False))
        print()
    except Exception:
        print("** this feed has no `sources` sheet — it predates the provenance change, "
              "so there is no as-of date on the growth month. Check it by hand.\n")

    MS = [m for m in str(f.window_months.iloc[0]).split(",") if m]
    GM = str(f.growth_month.iloc[0])
    n = len(MS)
    os.makedirs(A.out, exist_ok=True)

    # ---- account_summary
    keep = ["account_name", "account_id", "regional", "senior", "practice_group",
            "vob_pattern_band", "actions_coverage",
            "avg_days_result_to_vob", "avg_days_biopsy_to_vob",
            # column AY on Pam's sheet. Sourced in the notebook now, so the workbook
            # build no longer needs the All-Accounts CSV passed to it separately.
            "avg_days_vob_close_to_sim"]
    s = f[[c for c in keep if c in f.columns]].copy()
    for pre, tot in (("nmsc", "appropriate_total"), ("marked", "marked_total"),
                     ("sf_vob", "sf_vob_total"), ("sf_sim", "sf_sim_total")):
        for m in MS:
            s[f"{pre}_{m}"] = f[f"{pre}_{m}"]
        s[f"{pre}_total"] = f[tot]
    s.to_csv(os.path.join(A.out, "account_summary.csv"), index=False)

    # ---- sf_rollup
    r = f[["account_name", "account_id", "accounts_pooled"]].copy()
    for m in MS:
        r[f"sf_vob_{m}"] = f[f"sf_vob_{m}"]
    for m in MS:
        r[f"sf_sim_{m}"] = f[f"sf_sim_{m}"]
    r.to_csv(os.path.join(A.out, "sf_rollup.csv"), index=False)

    # ---- trend, long form, window months plus the growth month.
    # The growth month is whatever the FEED says it is -- which is the whole point. In
    # September it was 773 verifications short because it came from a mid-month pull;
    # taking it from here means the deck and the workbook cannot disagree about it.
    rows = []
    for _, a in f.iterrows():
        for m in MS:
            rows.append(dict(month=f"2026-{ORDER.index(m)+1:02d}", account_name=a.account_name,
                             sf_vob=int(a[f"sf_vob_{m}"]), sf_sim=int(a[f"sf_sim_{m}"])))
        rows.append(dict(month=f"2026-{ORDER.index(GM)+1:02d}", account_name=a.account_name,
                         sf_vob=int(a.growth_vob), sf_sim=int(a.growth_sim)))
    pd.DataFrame(rows).to_csv(os.path.join(A.out, "trend.csv"), index=False)

    print(f"wrote 3 CSVs to {A.out}")
    print(f"  {len(f)} accounts · window {', '.join(MS)} · growth month {GM}")
    print(f"  growth source: {f.growth_source.iloc[0]}")
    print(f"  terminated but kept: {int(f.kept_despite_terminated.sum())}")
    print("\nNEXT:")
    print(f"  python3 tpl/prep_scorecards.py {A.out} --copy tpl/seed_copy.csv "
          f"--from-month {MS[0]} -o sc_pass1.json")
    print("  python3 tpl/gen_copy.py sc_pass1.json --master <last month's workbook> \\")
    print(f"      --account-summary {A.out}/account_summary.csv -o copy_gen.csv")
    print(f"  python3 tpl/prep_scorecards.py {A.out} --copy copy_gen.csv "
          f"--from-month {MS[0]} -o sc_final.json")
    print("  node tpl/build_deck.js sc_final.json Volume_Bottleneck_<Month>.pptx")


if __name__ == "__main__":
    main()
