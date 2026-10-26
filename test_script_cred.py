"""
Northfield Dermatology (mock EHR) - multi-user login -> Path / Labs report -> PDF -> pandas

Run all users in credentials.csv:
    python -m pytest -v -s test_script.py
Run selected users (exact usernames, comma separated) or the first N users:
    set RPA_USERS=user_a0,user_a3        (PowerShell: $env:RPA_USERS="user_a0,user_a3")
    set RPA_LIMIT=5

credentials.csv columns: Path, Username, Password   (kept next to this script)

Output file names:  <suggested_name>_<username>_<YYYYMMDD_HHMMSS>.pdf
  * suggested_name = the name the site/Chrome would suggest: the download's
    suggested filename, or the printed page's <title> (what Chrome's "Save as
    PDF" proposes). Falls back to DEFAULT_REPORT_NAME. Set REPORT_NAME_OVERRIDE
    to force a fixed name.
  * timestamp = the moment the report was captured/downloaded.

How "Print Table" is handled
----------------------------
Chrome's native print dialog is browser UI and cannot be driven by Playwright.
We inject a window.print() stub into every page/frame before site code runs;
when Print Table calls it, we render the page (in the exact state the site
prepared for printing) to PDF with Chromium's print engine. Popup reports and
real file downloads are handled too. page.pdf() requires headless Chromium, so
this script launches its own headless browser.
"""

import csv
import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pytest
from playwright.sync_api import expect, sync_playwright

# pathology_pdf_to_pandas.py must sit in the same folder as this script
from pathology_pdf_to_pyspark import build_dataframe, run_checks

URL = "https://musical-biscuit-f1d2b1.netlify.app/"

BASE_DIR = Path('/Workspace/Users/shubham.pandey1@procdna.com/HANDSON SANDBOX').resolve()
DOWNLOAD_DIR = BASE_DIR / "downloads"
SCREENSHOT_DIR = BASE_DIR / "screenshots"
CREDENTIALS_FILE = BASE_DIR / "credentials.csv"   # or credentials.xlsx (needs openpyxl)

DEFAULT_REPORT_NAME = "path_labs_report"
REPORT_NAME_OVERRIDE = None   # e.g. "path_labs_report" to ignore the suggested name

NAV_TIMEOUT_MS = 30_000
LOGIN_TIMEOUT_MS = 30_000
FILTER_TIMEOUT_MS = 20_000
PRINT_ENABLED_TIMEOUT_MS = 120_000  # site needs ~30s; allow generous headroom
PRINT_REACTION_TIMEOUT_S = 20

# Filter form values
PROVIDER_VALUE = "test"
PATIENT_VALUE = "test"
LOCATION_VALUE = "test"
ENTRY_DATE_DISPLAY = "01-10-2026"  # dd-mm-yyyy as shown in the UI (1 Oct 2026)
ENTRY_DATE_ISO = "2026-10-01"      # <input type=date> always takes yyyy-mm-dd

# Injected into every frame of every page before any site script runs.
INIT_JS = """
(() => {
  window.__printCalls = 0;
  window.__printSnapshot = null;
  window.print = function () {
    window.__printCalls += 1;
    try { window.__printSnapshot = document.documentElement.outerHTML; } catch (e) {}
  };
})();
"""


# ----------------------------------------------------------------------------
# credentials
# ----------------------------------------------------------------------------
class Secret(str):
    """A str that never reveals itself in reprs / pytest failure output."""

    def __repr__(self):
        return "'****'"


def load_credentials():
    """Return [{'path','username','password'}], honouring RPA_USERS / RPA_LIMIT."""
    if CREDENTIALS_FILE.exists():
        with open(CREDENTIALS_FILE, newline="", encoding="utf-8-sig") as fh:
            rows = list(csv.DictReader(fh))
    elif CREDENTIALS_FILE.with_suffix(".xlsx").exists():
        import pandas as pd
        rows = pd.read_excel(
            CREDENTIALS_FILE.with_suffix(".xlsx"), dtype=str
        ).fillna("").to_dict("records")
    else:
        return []

    creds = []
    for row in rows:
        low = {(k or "").strip().lower(): (v or "") for k, v in row.items()}
        username = low.get("username", "").strip()
        if not username:
            continue
        creds.append({
            "path": low.get("path", "").strip() or "/",
            "username": username,
            "password": Secret(low.get("password", "")),
        })

    wanted = os.environ.get("RPA_USERS")
    if wanted:
        names = {n.strip() for n in wanted.split(",") if n.strip()}
        creds = [c for c in creds if c["username"] in names]
    limit = os.environ.get("RPA_LIMIT")
    if limit and limit.isdigit():
        creds = creds[: int(limit)]
    return creds


CREDENTIALS = load_credentials()


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def log(msg):
    print(f"[rpa] {msg}", flush=True)


def snap(page, name):
    """Best-effort screenshot; never raises."""
    try:
        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        path = SCREENSHOT_DIR / f"{name}.png"
        page.screenshot(path=str(path), full_page=True)
        log(f"screenshot saved: {path}")
    except Exception as exc:  # noqa: BLE001
        log(f"could not take screenshot '{name}': {exc}")


def fill_field(page, label, fallback, value):
    """Fill a text input by label, else by fallback locator; verify the value."""
    target = page.get_by_label(label, exact=True)
    try:
        use_label = target.count() == 1 and target.is_visible()
    except Exception:  # noqa: BLE001
        use_label = False
    field = target if use_label else fallback
    expect(field).to_be_visible()
    field.fill(value)
    expect(field).to_have_value(value)
    log(f"filled {label!r} = {value!r} ({'label' if use_label else 'fallback'} locator)")


def find_button_any_frame(page, name, timeout_ms):
    """Wait for a visible button with this exact name in ANY frame; None on timeout."""
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        for frame in page.frames:
            loc = frame.get_by_role("button", name=name, exact=True)
            try:
                if loc.count() > 0 and loc.first.is_visible():
                    return loc.first
            except Exception:  # frame may be navigating/detached
                pass
        page.wait_for_timeout(250)
    return None


def safe_name(text, fallback):
    """Make a string safe for use in a Windows file name."""
    text = re.sub(r"[^\w\-. ]+", "", text or "").strip().replace(" ", "_")
    return text[:80] or fallback


def make_pdf_path(suggested, username):
    base = REPORT_NAME_OVERRIDE or safe_name(suggested, DEFAULT_REPORT_NAME)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DOWNLOAD_DIR / f"{base}_{safe_name(username, 'user')}_{stamp}.pdf"


def validate_pdf(path: Path):
    assert path.exists(), f"PDF not found at {path}"
    size = path.stat().st_size
    assert size > 0, "PDF file is empty"
    with open(path, "rb") as fh:
        assert fh.read(5) == b"%PDF-", "File does not start with %PDF- signature"
    log(f"PDF signature OK, size = {size:,} bytes")

    try:  # optional text check: pip install pypdf
        from pypdf import PdfReader
    except ImportError:
        log("pypdf not installed - skipping text-content check")
        return
    reader = PdfReader(str(path))
    text = "\n".join((p.extract_text() or "") for p in reader.pages)
    log(f"PDF pages: {len(reader.pages)}, extracted characters: {len(text)}")
    assert len(reader.pages) >= 1, "PDF has no pages"
    assert text.strip(), (
        "PDF contains no extractable text - the print view may be blank. "
        "Check the .html snapshot in downloads and the screenshots folder."
    )


def render_pdf(page, path: Path):
    page.emulate_media(media="print")
    page.pdf(path=str(path), prefer_css_page_size=True, print_background=True, landscape=True)


def login(page, cred, tag):
    """Open the user's login page, sign in, and wait for the app shell."""
    login_url = urljoin(URL, cred["path"])
    log(f"[{cred['username']}] opening {login_url}")
    page.goto(login_url, wait_until="domcontentloaded")

    password = page.locator("input[type='password']")
    expect(password.first).to_be_visible(timeout=NAV_TIMEOUT_MS)

    user_by_label = page.get_by_label("Username", exact=True)
    username_field = (
        user_by_label if user_by_label.count() == 1
        else page.locator("input[type='text'], input:not([type])").first
    )
    username_field.fill(cred["username"])
    password.first.fill(str(cred["password"]))   # never logged

    log(f"[{cred['username']}] clicking 'Sign In'")
    page.get_by_role("button", name="Sign In", exact=True).click()

    nav = page.get_by_text("Path / Labs", exact=True).first
    try:
        expect(nav).to_be_visible(timeout=LOGIN_TIMEOUT_MS)
    except AssertionError:
        snap(page, f"{tag}_login_failed")
        still_on_login = page.locator("input[type='password']").count() > 0
        pytest.fail(
            f"Login failed for {cred['username']}: "
            + ("sign-in form is still displayed (bad credentials or wrong path "
               f"'{cred['path']}')." if still_on_login
               else "'Path / Labs' navigation did not appear after sign-in.")
        )
    log(f"[{cred['username']}] logged in")


# ----------------------------------------------------------------------------
# test
# ----------------------------------------------------------------------------
@pytest.fixture(scope="module")
def browser():
    with sync_playwright() as pw:
        b = pw.chromium.launch(headless=True)  # headless is required for pdf()
        yield b
        b.close()


@pytest.mark.skipif(not CREDENTIALS, reason=f"No credentials found in {CREDENTIALS_FILE.name}")
@pytest.mark.parametrize("cred", CREDENTIALS, ids=[c["username"] for c in CREDENTIALS])
def test_download_path_labs_report(browser, cred):
    username = cred["username"]
    tag = safe_name(username, "user")
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    # Fresh, isolated session per user (no shared cookies/storage).
    context = browser.new_context(
        accept_downloads=True, viewport={"width": 1600, "height": 1000}
    )
    context.add_init_script(INIT_JS)
    page = context.new_page()
    page.set_default_timeout(NAV_TIMEOUT_MS)

    popups, downloads = [], []
    context.on("page", lambda p: popups.append(p) if p != page else None)
    page.on("download", lambda d: downloads.append(d))
    page.on("dialog", lambda d: d.accept())  # alert/confirm safety net

    pdf_path = None
    try:
        # 1. Login ---------------------------------------------------------
        login(page, cred, tag)
        snap(page, f"{tag}_after_login")

        # 2. Path / Labs ---------------------------------------------------
        log(f"[{username}] clicking 'Path / Labs'")
        page.get_by_text("Path / Labs", exact=True).first.click()

        # 3. Path (it is a button, not a tab) -------------------------------
        path_btn = page.get_by_role("button", name="Path", exact=True)
        expect(path_btn).to_be_visible()
        log("clicking 'Path'")
        path_btn.click()

        # 3b. Pending Plan Completion sub-tab (Apply Filter only appears here)
        plan_tab = page.get_by_role("button", name="Pending Plan Completion", exact=True)
        expect(plan_tab).to_be_visible()
        log("clicking 'Pending Plan Completion'")
        plan_tab.click()
        snap(page, f"{tag}_after_pending_plan_completion")

        # 3c. Fill the filter form -----------------------------------------
        last_first = page.get_by_placeholder("Last, First")
        expect(last_first.first).to_be_visible()
        fill_field(page, "Provider", last_first.nth(0), PROVIDER_VALUE)
        fill_field(page, "Patient", last_first.nth(1), PATIENT_VALUE)
        fill_field(page, "Location", page.get_by_placeholder("Any"), LOCATION_VALUE)

        date_input = page.locator("input[type='date']")
        expect(date_input).to_be_visible()
        date_input.fill(ENTRY_DATE_ISO)
        expect(date_input).to_have_value(ENTRY_DATE_ISO)
        log(f"filled 'Entry Date' = {ENTRY_DATE_DISPLAY} ({ENTRY_DATE_ISO})")
        snap(page, f"{tag}_form_filled")

        # 4. Apply Filter ---------------------------------------------------
        apply_btn = find_button_any_frame(page, "Apply Filter", FILTER_TIMEOUT_MS)
        if apply_btn is None:
            snap(page, f"{tag}_apply_filter_not_found")
            buttons = [
                b.inner_text().strip()
                for f in page.frames for b in f.locator("button").all()
            ]
            pytest.fail(f"'Apply Filter' never appeared. Buttons seen: {buttons}")
        expect(apply_btn).to_be_enabled()
        log("clicking 'Apply Filter'")
        apply_btn.click()
        snap(page, f"{tag}_after_apply_filter")

        # 5. Wait for Print Table to become enabled -------------------------
        print_btn = find_button_any_frame(page, "Print Table", 10_000)
        assert print_btn is not None, "'Print Table' button not found"
        log("waiting for 'Print Table' to become enabled (up to 120s)...")
        started = time.time()
        try:
            expect(print_btn).to_be_enabled(timeout=PRINT_ENABLED_TIMEOUT_MS)
        except AssertionError:
            snap(page, f"{tag}_print_table_never_enabled")
            pytest.fail("'Print Table' did not become enabled in time")
        log(f"'Print Table' enabled after {time.time() - started:.1f}s")

        try:
            log("Print Table outerHTML: " + print_btn.evaluate("e => e.outerHTML"))
        except Exception:  # noqa: BLE001
            pass
        snap(page, f"{tag}_before_print_click")

        # 6. Click Print Table and see how the site reacts ------------------
        log("clicking 'Print Table'")
        print_btn.click()

        outcome = None  # ("download"|"popup"|"print", object)
        deadline = time.time() + PRINT_REACTION_TIMEOUT_S
        while time.time() < deadline and outcome is None:
            if downloads:
                outcome = ("download", downloads[0])
            elif popups:
                outcome = ("popup", popups[0])
            else:
                for frame in page.frames:
                    try:
                        if frame.evaluate("window.__printCalls || 0") > 0:
                            outcome = ("print", frame)
                            break
                    except Exception:  # noqa: BLE001
                        pass
            page.wait_for_timeout(200)

        if outcome is None:
            snap(page, f"{tag}_print_no_reaction")
            pytest.fail(
                f"Clicking Print Table produced no download, popup or "
                f"window.print() call within {PRINT_REACTION_TIMEOUT_S}s."
            )

        kind, obj = outcome
        log(f"site reaction: {kind}")

        # 7. Produce the PDF: <suggested>_<username>_<timestamp>.pdf --------
        if kind == "download":
            suggested = Path(obj.suggested_filename).stem
            pdf_path = make_pdf_path(suggested, username)
            log(f"saving download '{obj.suggested_filename}'")
            obj.save_as(str(pdf_path))

        elif kind == "popup":
            obj.wait_for_load_state("load")
            obj.wait_for_timeout(500)
            snap(obj, f"{tag}_popup_report")
            pdf_path = make_pdf_path(obj.title(), username)
            render_pdf(obj, pdf_path)

        else:  # window.print() was called
            frame = obj
            snapshot = frame.evaluate("window.__printSnapshot") or ""
            pdf_path = make_pdf_path(page.title(), username)
            pdf_path.with_suffix(".html").write_text(snapshot, encoding="utf-8")
            if frame == page.main_frame:
                render_pdf(page, pdf_path)   # live page, exactly as the site left it
            else:
                log("print came from a child frame - rendering that frame")
                tmp = context.new_page()
                tmp.set_content(snapshot, wait_until="load")
                render_pdf(tmp, pdf_path)
                tmp.close()

        log(f"PDF written to: {pdf_path.resolve()}")

        # 8. Validate -------------------------------------------------------
        validate_pdf(pdf_path)
        log("PDF download validated")

    except BaseException:
        snap(page, f"{tag}_failure")
        raise
    finally:
        context.close()

    # 9. Parse the saved PDF with the pandas extractor ---------------------
    log(f"[{username}] parsing {pdf_path.name} with pathology_pdf_to_pandas")
    pathology_df = build_dataframe(pdf_path)
    run_checks(pathology_df)

    assert len(pathology_df) > 0, (
        "No pathology records were extracted from the PDF - the layout may "
        "differ from the X_BOUNDS / y-range assumptions in pathology_pdf_to_pandas_subscript.py"
    )

    csv_path = pdf_path.with_suffix(".csv")   # same stem as the PDF
    pathology_df.to_csv(csv_path, index=False)
    log(f"extracted {len(pathology_df)} records -> {csv_path.resolve()}")
    log(f"[{username}] SUCCESS")