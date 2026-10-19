"""
Northfield Dermatology (mock EHR) - Path / Labs report -> PDF

Run:  python -m pytest -v -s test_script.py

How "Print Table" is handled
----------------------------
Chrome's native print dialog is browser UI, not a web page, so Playwright cannot
click it. Instead we inject a stub for window.print() into every page/frame
BEFORE the site's own JS runs. When the site's Print Table handler calls
window.print(), the stub records the call (and leaves the DOM exactly as the site
prepared it for printing). We then render that state to PDF with Chromium's
print engine under print-media CSS, which is what Chrome's "Save as PDF" does.

The script also handles the other ways a "print" button may behave:
  * opens a popup/new tab with the report      -> PDF is made from the popup
  * triggers a normal file download            -> the download is saved
  * prints from a hidden iframe                -> the iframe content is rendered

NOTE: page.pdf() only works in headless Chromium, so this script launches its
own headless browser instead of using the pytest-playwright `page` fixture.
"""

import re
import time
from pathlib import Path

import pytest
from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import expect, sync_playwright
from pathology_pdf_to_pandas_subscript import build_dataframe, run_checks

URL = "https://musical-biscuit-f1d2b1.netlify.app/"

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
SCREENSHOT_DIR = BASE_DIR / "screenshots"
PDF_PATH = DOWNLOAD_DIR / "path_labs_report.pdf"

NAV_TIMEOUT_MS = 30_000
FILTER_TIMEOUT_MS = 20_000
PRINT_ENABLED_TIMEOUT_MS = 120_000  # site needs ~30s; allow generous headroom
PRINT_REACTION_TIMEOUT_S = 20

# Filter form values
PROVIDER_VALUE = "test"
PATIENT_VALUE = "test"
LOCATION_VALUE = "test"
ENTRY_DATE_DISPLAY = "01-10-2026"  # dd-mm-yyyy as shown in the UI (1 Oct 2026)
ENTRY_DATE_ISO = "2026-10-01"      # <input type=date> always takes yyyy-mm-dd


def fill_field(page, label, fallback, value):
    """
    Fill a text input. Prefer the accessible label; if the label isn't linked
    to the input, use the fallback locator derived from the diagnostic output.
    Verifies the value afterwards.
    """
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


def find_button_any_frame(page, name, timeout_ms):
    """
    Wait until a button with this exact accessible name exists in ANY frame
    (main frame first) and return its locator. Returns None on timeout.
    """
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


def validate_pdf(path: Path):
    assert path.exists(), f"PDF not found at {path}"
    size = path.stat().st_size
    assert size > 0, "PDF file is empty"
    with open(path, "rb") as fh:
        assert fh.read(5) == b"%PDF-", "File does not start with %PDF- signature"
    log(f"PDF signature OK, size = {size:,} bytes")

    # Optional content check (only if pypdf is installed: pip install pypdf)
    try:
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
        "Check downloads/print_snapshot.html and the screenshots folder."
    )


def render_pdf(page, path: Path):
    page.emulate_media(media="print")
    page.pdf(
        path=str(path),
        prefer_css_page_size=True,
        print_background=True,
        landscape=True,
    )


# ----------------------------------------------------------------------------
# test
# ----------------------------------------------------------------------------
def test_download_path_labs_report():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    if PDF_PATH.exists():
        PDF_PATH.unlink()  # make sure we never validate a stale file

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)  # headless is required for pdf()
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

        try:
            # 1. Open site -----------------------------------------------------
            log(f"opening {URL}")
            page.goto(URL, wait_until="domcontentloaded")
            expect(page.get_by_text("Path / Labs", exact=True).first).to_be_visible()

            # 2. Path / Labs ---------------------------------------------------
            log("clicking 'Path / Labs'")
            page.get_by_text("Path / Labs", exact=True).first.click()

            # 3. Path (it is a button, not a tab) -------------------------------
            path_btn = page.get_by_role("button", name="Path", exact=True)
            expect(path_btn).to_be_visible()
            log("clicking 'Path'")
            path_btn.click()

            # 3b. Pending Plan Completion sub-tab (Apply Filter only appears here)
            plan_tab = page.get_by_role(
                "button", name="Pending Plan Completion", exact=True
            )
            expect(plan_tab).to_be_visible()
            log("clicking 'Pending Plan Completion'")
            plan_tab.click()
            snap(page, "after_pending_plan_completion")

            # 3c. Fill the filter form -----------------------------------------
            # Fallbacks follow the order seen in the diagnostic: the two
            # 'Last, First' inputs are Provider then Patient; the 'Any' input
            # is Location; the only <input type=date> is Entry Date.
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
            snap(page, "form_filled")

            # 4. Apply Filter ---------------------------------------------------
            # Wait on the observable condition (button present) instead of a
            # sleep; search every frame in case the UI re-renders.
            apply_btn = find_button_any_frame(page, "Apply Filter", FILTER_TIMEOUT_MS)
            if apply_btn is None:
                snap(page, "apply_filter_not_found")
                buttons = [
                    b.inner_text().strip()
                    for f in page.frames
                    for b in f.locator("button").all()
                ]
                pytest.fail(f"'Apply Filter' never appeared. Buttons seen: {buttons}")
            expect(apply_btn).to_be_enabled()
            log("clicking 'Apply Filter'")
            apply_btn.click()
            snap(page, "after_apply_filter")

            # 5. Wait for Print Table to become enabled -------------------------
            print_btn = find_button_any_frame(page, "Print Table", 10_000)
            assert print_btn is not None, "'Print Table' button not found"
            log("waiting for 'Print Table' to become enabled (up to 120s)...")
            started = time.time()
            try:
                expect(print_btn).to_be_enabled(timeout=PRINT_ENABLED_TIMEOUT_MS)
            except AssertionError:
                snap(page, "print_table_never_enabled")
                pytest.fail("'Print Table' did not become enabled in time")
            log(f"'Print Table' enabled after {time.time() - started:.1f}s")

            # Diagnostics: what does the button actually do?
            try:
                log("Print Table outerHTML: " + print_btn.evaluate("e => e.outerHTML"))
            except Exception:  # noqa: BLE001
                pass
            snap(page, "before_print_click")

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
                snap(page, "print_no_reaction")
                pytest.fail(
                    f"Clicking Print Table produced no download, popup or "
                    f"window.print() call within {PRINT_REACTION_TIMEOUT_S}s."
                )

            kind, obj = outcome
            log(f"site reaction: {kind}")

            # 7. Produce the PDF -----------------------------------------------
            if kind == "download":
                log(f"saving download '{obj.suggested_filename}'")
                obj.save_as(str(PDF_PATH))

            elif kind == "popup":
                obj.wait_for_load_state("load")
                obj.wait_for_timeout(500)
                snap(obj, "popup_report")
                render_pdf(obj, PDF_PATH)

            else:  # window.print() was called
                frame = obj
                snapshot = frame.evaluate("window.__printSnapshot") or ""
                (DOWNLOAD_DIR / "print_snapshot.html").write_text(
                    snapshot, encoding="utf-8"
                )
                if frame == page.main_frame:
                    # Render the live page in the exact state the site left it.
                    render_pdf(page, PDF_PATH)
                else:
                    # Print came from a hidden iframe: render only its content.
                    log("print came from a child frame - rendering that frame")
                    tmp = context.new_page()
                    tmp.set_content(snapshot, wait_until="load")
                    render_pdf(tmp, PDF_PATH)
                    tmp.close()

            log(f"PDF written to: {PDF_PATH.resolve()}")

            # 8. Validate -------------------------------------------------------
            validate_pdf(PDF_PATH)
            log("PDF download validated")

        except BaseException:
            snap(page, "failure")
            raise
        finally:
            context.close()
            browser.close()

    # 9. Parse the saved PDF with the pandas extractor ---------------------
    # Runs after the browser is closed; only reached if the download succeeded.
    log(f"parsing {PDF_PATH.name} with pathology_pdf_to_pandas")
    pathology_df = build_dataframe(PDF_PATH)
    run_checks(pathology_df)

    assert len(pathology_df) > 0, (
        "No pathology records were extracted from the PDF - the layout may "
        "differ from the X_BOUNDS / y-range assumptions in pathology_pdf_to_pandas_subscript.py"
    )

    csv_path = DOWNLOAD_DIR / "pathology_results.csv"
    pathology_df.to_csv(csv_path, index=False)
    log(f"extracted {len(pathology_df)} records -> {csv_path.resolve()}")
    log("SUCCESS")
