# Pathology Focus — pipeline wiki

For consultants joining the analysis. Read §1 and §2 before touching anything; the rest
is reference.

Pipeline `v7`, September 2026. Catalog `dev_pathology_data` (or `prod_pathology_data`),
schemas `raw` → `staged` → `gold`.

---

## 1. What this analysis answers

For each dermatology practice, four questions in sequence:

```
NMSC APPROPRIATE  →  SRT-MARKED  →  VOB  →  SIMULATION
```

1. How many patients had a biopsy that makes them appropriate for IG-SRT?
2. How many were marked as interested?
3. How many produced a verification of benefits?
4. How many converted to a simulation?

**The gap between those stages is the commercial opportunity**, and the therapist owns
exactly one transition: turning a candidate into a completed VOB.

### The two rules that explain most confusion

**Everything is a patient-day, never a lesion.** The grain is
`(account, patient_id, biopsy_date)`. One patient counted once per date of service no
matter how many lesions were taken. If a number looks too big, check whether something
has been counted per lesion.

**Four different clocks sit on one row.** This is the single most common source of
"these numbers don't tie":

| column | keyed on |
|---|---|
| appropriate, marked | **biopsy date** |
| Salesforce VOB / SIM | **`vob_created` date** |
| matched VOB (`vobm_<M>`) | the **biopsy** month |
| scope buckets | biopsy month when matched, `vob_created` month otherwise |

So the same account can show 50 VOBs in July on one column and 44 on another and both be
right. They are answering different questions.

---

## 2. Running a month

```
04b  →  01, 02, 02b, 03  →  04  →  05, 06  →  07, 07b  →  08  →  99_qa  →  09, 10
```

| # | notebook | what it does |
|---|---|---|
| 00 | `00_config` | window, catalog, goals, file paths — the only place to edit them |
| 00 | `00_vocab` | date parsing, folder canonicalisation, facility rules, aliases |
| 01 | `01_ingest` | manifest, extract text-layer PDFs / CSV / XLSX, then scanned via OCR at 300 DPI |
| 02 | `02_classify` | NMSC appropriate + SRT signal, per lesion |
| 02b | `02b_rule_d_validation` | RULE D gates — read-only, run it after any edit to the cleared vocabulary |
| 03 | `03_patient_days` | lesion dedup, facility reroute, folder canon, the patient-day grain |
| 04 | `04_account_map` | folder name → Salesforce AccountId |
| 04b | `04b_new_account_preflight` | **run this when folders are added** — see §5 |
| 05 | `05_vob_source` | the VOB candidate set |
| 06 | `06_match` | VOB ↔ patient-day matching, and the scope of each match |
| 07 | `07_review_views` | review and flag views |
| 07b | `07b_allaccounts_rollup` | Salesforce monthly rollup, pooled ids summed |
| 08 | `08_gold` | `account_summary`, `sf_rollup`, `trend`, `vob_scope`, disposition |
| 99 | `99_qa` | **every quality check, in one place** — see §4 |
| 09 | `09_export` | the CSVs the workbook build reads. **PHI — see §6** |
| 10 | `10_deck_feed` | one workbook feeding the PowerPoint. No patient data |

Then, locally: `feed_to_export.py` → `prep_scorecards.py` → `gen_copy.py` →
`prep_scorecards.py` → `build_deck.js`, and `build_pam_v2.py` for the workbook.

**The workbook's own gate runs outside Databricks.** `recalc.py` must report zero formula
errors, and it needs LibreOffice, which serverless compute does not have.

---

## 3. Where a match can land

`match_scope` describes **where the biopsy was found**, not where the VOB was raised. A
VOB belongs permanently to the site that raised it in Salesforce; a sister match moves the
biopsy attribution and nothing else.

| scope | meaning |
|---|---|
| `own site` | the biopsy is in this account's own path log |
| `sister site` | in a practice-group sibling's log |
| `enterprise` | in a practice outside the group |
| `ambiguous namesake` | a name matched somewhere untrustworthy — **not credited** |
| `no biopsy found` | no biopsy anywhere |
| `duplicate_patient` | exclusive: a duplicate-patient VOB leaves its scope bucket |

### Reading a site that takes referrals

Some sites treat patients other sites biopsied. La Vernia holds 36 VOBs against 29
appropriate patient-days, because 12 were biopsied at a sibling. Its Marked → VOB reads
**168%**, which is not a conversion rate. On work it owns both ends of it is **68%**.

When a site is over 100%, look at `sister site` and `enterprise` before drawing a
conclusion. The workbook carries an **outside-referral %** column for exactly this.

### Pooled folders

Six folders serve more than one Salesforce AccountId — one shared path log, two machines
at one address. **Anything joining to a per-AccountId file must sum across the ids.**
Joining on the primary alone halves the account; that was the Dunedin defect of 1 Sep.

---

## 4. QA

`99_qa` runs every check and writes `gold.qa_results`. Three severities: **BLOCK** stops
the run, **WARN** is worth a look, **INFO** is a number worth seeing.

The checks that exist because something went wrong:

| check | what it caught |
|---|---|
| orphaned manifest rows | `OK`/`EMPTY` are terminal, so 01 skips forever. 6 accounts, ~4,000 rows |
| `biopsy_date` parsed | dropping the bare `try_to_date` arm failed 156,217 of 157,000 rows |
| grain is one row per patient-day | a duplicate here multiplies every published number |
| no VOB at two accounts | 17,510 distinct `vob_id` across exactly 17,510 pairs |
| month columns are NAMED | prod emits `nmsc_m1`; nothing downstream reads it |
| in-window span per account | 91 days against a 121-day window is a trailing-window export |
| **last window month vs the report** | **the August lesson — see below** |

### The August lesson

`gold.trend` is only as complete as the Salesforce pull behind it, and **a month pulled
before it closes is short without saying so.** In September the growth month read 2,511
verifications against the report's 3,284 — **773 short across 166 accounts** — and it
moved 172 of 182 published growth figures once corrected.

The tell: **the last window month should already agree with the report.** July did, to
within one record. August did not. `99_qa` now checks that automatically, and
`10_deck_feed` takes the growth month from the report rather than from `trend`.

### What is deliberately NOT gated

**Do not reject a match because the biopsy is dated after the VOB.** Some practices verify
benefits at the visit, ahead of pathology — 66 of 182 accounts show it in own-site data,
six above 10%. A date guard was proposed and rejected; it would have deleted real matches
at exactly the referral-taking sites the analysis is trying to get right. The check
reports the distribution and does not act on it.

### RULE D — cleared margins plus a topical agent · 18 Sep 2026

A lesion whose **Actions or Comments** say the margins came back clear **and** name a
topical agent — imiquimod, Aldara, Zyclara, 5-FU, Efudex, Carac, Fluoroplex, Tolak — is
not appropriate. The lesion was excised, the margins are clear, the topical is
belt-and-braces; there is nothing left to irradiate.

**The defect it fixed was the field list, not the vocabulary.** `is_cleared` read
`pathology` and `actions` only. `comments` has always been in `SEARCH_COLS`, so SRT
marking saw it — but `is_cleared` never did, so a practice recording its disposition in
Comments / Assigned To never had its cleared lesions suppressed.

Measured on 161,215 lesion rows: **25 hits, 16 of them Appropriate, 1 SRT-marked** —
0.013% of appropriate lesions. It moves nothing; it is in for correctness. It is folded
into `is_cleared`, so it sits inside the `OR srt_marked` and the marked hit survives,
the same call Rule A made.

**The rule is a conjunction, and that is the whole of its safety.** 5,893 appropriate
lesions name a topical agent *without* clearance language — 4.6% of appropriate lesions,
367× this rule. Those are superficial BCC and SCCis being field-treated **instead of**
definitively: the IG-SRT opportunity, not an exclusion. Widening RULE D to "any topical
agent" would delete 4.6% of the denominator and flatter the mark rate in the same move.
`02b_rule_d_validation` gate 4 exists to catch exactly that edit.

---

## 5. Adding an account

**Usually nothing to do.** Folder names are copied character-for-character from Salesforce,
so `04_account_map` is an exact-name join — all 54 Phase-1 accounts matched with zero
fuzzy matching.

Three exceptions, and **none of them crash**:

1. **A folder that matches nothing gets no AccountId, and then scores zero matched VOBs.**
   On a scorecard that is indistinguishable from a practice that is not converting.
2. **A pooled second AccountId cannot be derived** — nothing in the data says two ids are
   one facility. It has to be seeded in `POOLED` in `04_account_map`.
3. **Seven columns come from outside the path-log pipeline** — Mohs and four
   clinical-workflow answers from last month's workbook, data discussion and lever from
   Patient Access, and the account transition from the warehouse. A new account renders
   blank or `N/A` in all seven.

**`04b_new_account_preflight` reports all three by name** and refuses to bless a run with
an unmapped folder. Run it after `04`.

---

### Where the account attributes come from

| field | source | why |
|---|---|---|
| **Account Transition** | **warehouse**, `dim_account_dtl` via `10_deck_feed` | live, and normalised by the same CASE the monthly query uses: `Hold` → `Hold - Amended Program`, `Normal Operations, Accelerator` → `Normal Operations, CS`. Current by construction |
| Data Discussion, Lever | Patient Access workbook | maintained by the business; nothing else holds them |
| Mohs, the four workflow answers | last month's delivered workbook | Pam maintains them by hand |
| **AVG days VOB close → Sim** (column AY) | **`raw.account_rollup`**, built in `07b` | one definition, asserted by metric name, reaching both the workbook and the deck |

Two things about the AVG-days metric that are not obvious from its column header:

* **The month is the VOB case CLOSED month**, not the simulation month. A case closed in
  July whose sim lands in August is a July row.
* **A month with nothing closed produces no row, not a zero.** The pivoted CSV writes
  `0.0` for those — 168 of its 483 July rows — which reads as "closed same day". Nothing
  in the pipeline coalesces it: absent stays `NULL`, and `99_qa` warns on an exact `0`
  because that is the tell a `coalesce` crept back in.
* **It is carried as a sum and a count, not as an average.** `all_metric_src` sums every
  metric over the month, and summing per-case day counts gives a total rather than an
  average. `07b` emits `V2S_DaysSum` and `V2S_Cases`, and divides them — a true
  case-weighted average that survives that sum. `v2s_cases` travels with the value so a
  one-case month is visible.
* **Pooled folders take a straight average of the ids, not a weighted one** — the 2 Sep
  decision. `sf_v2s_cases_<Mon>` is on `gold.sf_rollup` if that is ever revisited.

Where it flows:

```
distinct_ptl → avg_vob_close_sim (07b, in the FINAL UNION)
             → raw.allaccounts_src   V2S_DaysSum / V2S_Cases
             → _rollup_new           avg_days_vob_close_to_sim, v2s_cases
             → raw.account_rollup    (07b §6 write)
             → gold.sf_rollup        sf_v2s_days_<Mon>, sf_v2s_cases_<Mon>   (08)
             → gold.account_summary  avg_days_vob_close_to_sim  (last window month)
             → 09_export CSV → build_pam_v2.py → column AY
             → 10_deck_feed → the deck
```

The warehouse and Patient Access word the same transition states differently — `Normal
Operations, CS` against `Normal Operations - PCA Program`. **The warehouse wins**;
Patient Access fills only accounts the warehouse has no row for, and
`account_transition_source` records which was used.

---

## 6. Patient data

`raw_unfiltered`, `appropriate_patients` and `paths_marked_no_match` carry patient names,
and `mrn` where the account keys on one. **`accession` IS the EMA MRN and `dob` is a date
of birth — both are identifiers, not metadata.**

Those CSVs are PHI. Keep them in the volume, download to a controlled machine, and **do
not attach them to a message or a ticket.**

`gold.vob_scope` was deliberately built with no patient identifiers — account, `vob_id`
and dates only — and `10_deck_feed` is account-level aggregates throughout. Those two are
safe to share.


