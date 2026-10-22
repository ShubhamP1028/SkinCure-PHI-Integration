"""
Extract the Pathology Results Log PDF into a pandas DataFrame.

Pandas replacement for the PySpark notebook: the PDF parsing logic (PyMuPDF
word coordinates -> column bands) is unchanged; only the DataFrame creation,
date conversion and validation checks were converted from PySpark to pandas.

Requirements:  pip install pymupdf pandas
Usage:         python pathology_pdf_to_pandas.py [path/to/report.pdf]
               (or import extract_pdf_records / build_dataframe in a notebook)
"""

import re
import sys
from pathlib import Path

import pandas as pd
import pymupdf

# Default: the PDF produced by the Playwright automation (adjust as needed).
DEFAULT_PDF = Path(__file__).resolve().parent / "downloads" / "path_labs_report.pdf" \
    if "__file__" in globals() else Path("downloads/path_labs_report.pdf")

X_BOUNDS = [
    0, 65, 140, 210, 280, 350, 420,
    480, 545, 635, 660, 700, 755, 792,
]

COLUMN_NAMES = [
    "result_date",
    "patient_raw",
    "facility",
    "location",
    "result",
    "actions_and_plans",
    "comments_assigned_to",
    "flags",
    "notifications",
    "notes",
    "patient_portal",
    "results_processed",
    "photos",
]

# Output column order (equivalent to the Spark StructType schema).
OUTPUT_COLUMNS = [
    "result_date",
    "patient_name",
    "dob",
    "mrn",
    "facility",
    "location",
    "result",
    "actions_and_plans",
    "comments_assigned_to",
    "flags",
    "notifications",
    "notes",
    "patient_portal",
    "results_processed",
    "photos",
    "source_page",
]

# Set to numbers only if the PDFs contain Chrome's printed header/footer text.
BODY_Y_MIN = None
BODY_Y_MAX = None

DATE_PATTERN = re.compile(r"^\d{2}/\d{2}/\d{4}$")
DOB_PATTERN = re.compile(r"\bDOB\s+(\d{2}/\d{2}/\d{4})\b")
MRN_PATTERN = re.compile(r"\bMRN\s+([A-Z]{2,}-\d+)\b")


# --------------------------------------------------
# TEXT HELPERS (unchanged from the PySpark notebook)
# --------------------------------------------------

def clean_text(value):
    """Normalize whitespace without removing meaningful punctuation."""
    if not value:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def unique_join(values, separator=" "):
    """Join multiline values while removing consecutive duplicates."""
    cleaned = [clean_text(v) for v in values]
    cleaned = [v for v in cleaned if v]

    output = []
    for value in cleaned:
        if not output or output[-1] != value:
            output.append(value)

    return separator.join(output) if output else None


def words_to_lines(words, tolerance=2.5):
    """Group PDF words by approximate vertical position."""
    lines = []

    for word in sorted(words, key=lambda w: (w[1], w[0])):
        x0, y0, x1, y1, text = word[:5]

        target = None
        for line in lines:
            if abs(line["y"] - y0) <= tolerance:
                target = line
                break

        if target is None:
            target = {"y": y0, "words": []}
            lines.append(target)

        target["words"].append(word)

    for line in lines:
        line["words"].sort(key=lambda w: w[0])

    return sorted(lines, key=lambda line: line["y"])


def extract_column_text(words, x_left, x_right):
    """Extract words that fall inside a specified column."""
    selected = [w for w in words if x_left <= w[0] < x_right]
    lines = words_to_lines(selected)

    return unique_join(
        [" ".join(word[4] for word in line["words"]) for line in lines]
    )


def extract_pdf_records(pdf_path):
    records = []

    with pymupdf.open(str(pdf_path)) as pdf:
        for page_number, page in enumerate(pdf, start=1):
            words = page.get_text("words", sort=True)

            # Optional fixed y-band (only needed for PDFs printed with Chrome's own
            # date/URL header+footer). By default NO y filtering is applied: a
            # hard-coded 65-570 band silently dropped every row near the top or
            # bottom of each PDF page (e.g. 86 of 100 rows extracted). Page
            # headings and the repeated table header sit above the first date
            # anchor, so they are ignored automatically.
            if BODY_Y_MIN is not None:
                words = [w for w in words if w[1] >= BODY_Y_MIN]
            if BODY_Y_MAX is not None:
                words = [w for w in words if w[1] < BODY_Y_MAX]
            page_bottom = BODY_Y_MAX if BODY_Y_MAX is not None else page.rect.height + 1

            # Each record begins with a date in the first column.
            anchors = []
            for word in words:
                x0, y0, x1, y1, text = word[:5]
                if x0 < 65 and DATE_PATTERN.fullmatch(text.strip()):
                    anchors.append((y0, text.strip()))
            anchors.sort(key=lambda item: item[0])

            for index, (start_y, date_text) in enumerate(anchors):
                # Stop at the next record's date; the last record runs to the page bottom.
                end_y = anchors[index + 1][0] if index + 1 < len(anchors) else page_bottom

                record_words = [w for w in words if start_y - 1 <= w[1] < end_y]

                row = {}
                for i, column in enumerate(COLUMN_NAMES):
                    row[column] = extract_column_text(
                        record_words, X_BOUNDS[i], X_BOUNDS[i + 1]
                    )

                row["result_date"] = date_text
                row["source_page"] = str(page_number)

                # Extract patient identifiers from multiline details.
                patient_raw = row.get("patient_raw") or ""
                dob_match = DOB_PATTERN.search(patient_raw)
                mrn_match = MRN_PATTERN.search(patient_raw)

                row["dob"] = dob_match.group(1) if dob_match else None
                row["mrn"] = mrn_match.group(1) if mrn_match else None

                # Remove DOB/MRN text to isolate the patient name.
                patient_name = DOB_PATTERN.sub("", patient_raw)
                patient_name = MRN_PATTERN.sub("", patient_name)
                row["patient_name"] = clean_text(patient_name)

                records.append(row)

    return records


def pdf_diagnostics(pdf_path):
    """Print per-page date-anchor counts (= records detected) to explain row counts."""
    total = 0
    with pymupdf.open(str(pdf_path)) as pdf:
        for n, page in enumerate(pdf, start=1):
            words = page.get_text("words", sort=True)
            ys = [w[1] for w in words if w[0] < 65 and DATE_PATTERN.fullmatch(w[4].strip())]
            total += len(ys)
            span = f"y {min(ys):.0f}-{max(ys):.0f}" if ys else "no records"
            print(f"  [pdf] page {n}: size {page.rect.width:.0f}x{page.rect.height:.0f}, "
                  f"{len(ys)} date anchors ({span})")
    print(f"  [pdf] total date anchors: {total}")
    return total


# --------------------------------------------------
# CREATE PANDAS DATAFRAME  (replaces spark.createDataFrame + F.to_date)
# --------------------------------------------------

def build_dataframe(pdf_path):
    records = extract_pdf_records(pdf_path)

    # Select fields in schema order; every field starts as a string / None.
    df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    df = df.astype("string")  # nullable string dtype, keeps missing as <NA>

    # Convert dates after extraction (invalid values become NaT, like Spark's null).
    for col in ("result_date", "dob"):
        df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")

    return df


# --------------------------------------------------
# VALIDATION CHECKS  (replaces the PySpark checks)
# --------------------------------------------------

def run_checks(df):
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_colwidth", 60)

    # 1. Expected record count
    print("Extracted rows:", len(df))

    # 2. Check for missing patient identifiers
    print("\nRows with missing patient_name or mrn:")
    missing_ids = df[df["patient_name"].isna() | df["mrn"].isna()]
    print(
        missing_ids[["result_date", "patient_name", "dob", "mrn", "source_page"]]
        .to_string(index=False)
        if not missing_ids.empty else "  none"
    )

    # 3. Check duplicate medical record numbers
    #    (dropna=False so null MRNs are grouped, matching Spark's groupBy)
    print("\nDuplicate MRNs:")
    mrn_counts = df.groupby("mrn", dropna=False).size().reset_index(name="count")
    dupes = mrn_counts[mrn_counts["count"] > 1]
    print(dupes.to_string(index=False) if not dupes.empty else "  none")

    # 4. Inspect all extracted fields (newest first, top 25)
    print("\nTop 25 rows by result_date (desc):")
    inspect_cols = [
        "result_date", "patient_name", "dob", "mrn", "location", "result",
        "actions_and_plans", "comments_assigned_to", "flags", "notifications",
        "notes", "patient_portal", "results_processed",
    ]
    print(
        df.sort_values("result_date", ascending=False, na_position="last")[inspect_cols]
        .head(25)
        .to_string(index=False)
    )

    # 5. Check required fields (null or blank counts as missing)
    print("\nRequired field check:")
    required_columns = ["result_date", "patient_name", "mrn", "result", "flags"]
    for column in required_columns:
        series = df[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            missing = int(series.isna().sum())
        else:
            missing = int((series.isna() | (series.str.strip() == "")).sum())
        print(f"  {column}: {missing} missing values")


if __name__ == "__main__":
    pdf_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PDF
    if not pdf_path.exists():
        sys.exit(f"PDF not found: {pdf_path}")

    pathology_df = build_dataframe(pdf_path)
    print(pathology_df.head(10).to_string(index=False))
    print()
    run_checks(pathology_df)

    # Optional: persist the result
    out_csv = pdf_path.with_name("pathology_results.csv")
    pathology_df.to_csv(out_csv, index=False)
    print(f"\nSaved: {out_csv}")