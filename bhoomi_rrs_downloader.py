#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║        POWER-BHOOMI RRS DOWNLOADER v2.0 — Bhu Suraksha Record Room (Service4)        ║
║          Search + download scanned revenue documents by SURVEY NUMBER from:          ║
║              https://recordroom.karnataka.gov.in/Service4/RRS/RRSRequest             ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  MAPPED PORTAL FLOW (verified against the live logged-in page, Sept 2026):           ║
║                                                                                      ║
║   The page is a 4-step jQuery-Steps wizard:                                          ║
║     Step 0  Applicant details   #txt_mobile_no #txt_name ... (#btn_save_apln)        ║
║     Step 1  Create request      #txt_file_desc (>=20 chars) + #rd_lang_en/kn         ║
║                                 → #btn_create_new_request → SweetAlert confirm       ║
║                                 → POST /Service4/RRS/SaveRequest                     ║
║     Step 2  Document search     4 cards; ours = fnShowSurveyModalPopUp() opens       ║
║                                 #PopSurveySearch:                                    ║
║                                   #ddl_dist → #ddl_taluk → #ddl_hobli → #ddl_village ║
║                                   (cascade via GetTaluka/GetHobli/GetVillage AJAX)   ║
║                                   #ddl_file (ALL/File/Register)                      ║
║                                   #ddl_category_reg_type_SN (type)                   ║
║                                   #txt_survey_no  ("12" or "12/*/3")                 ║
║                                   → fnSearchBySurveyNo()                             ║
║                                 → POST /Service4/RRS/GetFilesBySurveyNumber          ║
║                                 → server-rendered rows injected in #divSearchedData  ║
║     Step 3  Forward-to-keeper fallback (not automated)                               ║
║                                                                                      ║
║   Downloading: pick a document from results (popup viewer) → add pages to            ║
║   wishlist → pay Rs 10/page → file appears on /Service4/RRS/Dashboard.               ║
║                                                                                      ║
║  WHAT THIS TOOL AUTOMATES                                                            ║
║   • session reuse (persistent profile; you do mobile+OTP login once, by hand)        ║
║   • request bootstrap (description + language + create, incl. SweetAlert)            ║
║   • the whole survey-number search cascade, for many survey numbers in one run       ║
║   • harvesting the result list per survey → CSV + saved HTML                         ║
║   • clicking download links and saving PDFs where the portal offers them             ║
║   • PAUSING at the wishlist/payment step — payment is ALWAYS done by you             ║
║                                                                                      ║
║  District names in dropdowns are Kannada; pass English names (built-in map),         ║
║  Kannada text, or raw census codes for any of district/taluk/hobli/village.          ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

Usage:
    python bhoomi_rrs_downloader.py login
    python bhoomi_rrs_downloader.py map
    python bhoomi_rrs_downloader.py download \
        --district "Bangalore Rural" --taluk "Hoskote" --hobli "Kasaba" \
        --village "ದೊಡ್ಡಗಟ್ಟಿಗನಬ್ಬೆ" --surveys "1,5,10-15" \
        [--file-reg ALL|File|Register] [--desc "..."]

Codes work everywhere text does:  --district 21 --taluk 4 --hobli 1 --village 1
"""

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

# ═══════════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://recordroom.karnataka.gov.in"
RRS_REQUEST_URL = f"{BASE_URL}/Service4/RRS/RRSRequest"
DASHBOARD_URL = f"{BASE_URL}/Service4/RRS/Dashboard"
SCRIPT_DIR = Path(__file__).resolve().parent
PROFILE_DIR = SCRIPT_DIR / "rrs_profile"          # persistent Chromium profile (keeps session)
STATE_FILE = SCRIPT_DIR / "rrs_state.json"        # cookies incl. session cookie (skips OTP)
PAGE_MAP_FILE = SCRIPT_DIR / "rrs_page_map.json"
PAGE_HTML_FILE = SCRIPT_DIR / "rrs_page.html"
DEFAULT_OUT_DIR = Path.home() / "Downloads" / "BHOOMI_RRS"

AJAX_POLL_INTERVAL = 0.4    # s between dropdown-population polls (same idea as v11)
AJAX_POLL_TIMEOUT = 30      # s to wait for a dependent dropdown to fill
NAV_TIMEOUT = 60_000        # ms

# ── Exact selectors (mapped from the live page) ────────────────────────────────────────
SEL = {
    "desc":            "#txt_file_desc",
    "lang_en":         "#rd_lang_en",
    "lang_kn":         "#rd_lang_kn",
    "create_request":  "#btn_create_new_request",
    "wizard_step2":    "a[href='#wizard-h-2']",
    "survey_modal":    "#PopSurveySearch",
    "district":        "#ddl_dist",
    "taluk":           "#ddl_taluk",
    "hobli":           "#ddl_hobli",
    "village":         "#ddl_village",
    "file_reg":        "#ddl_file",
    "file_reg_type":   "#ddl_category_reg_type_SN",
    "survey_no":       "#txt_survey_no",
    "search_btn":      "#PopSurveySearch button[onclick='fnSearchBySurveyNo()']",
    "results":         "#divSearchedData",
    "swal_popup":      ".swal2-popup, .swal2-container",
    "swal_confirm":    ".swal-btn-confirm, .swal2-confirm",
    "swal_html":       ".swal2-html-container, .swal2-title",
}

# English → district census code (as listed in #ddl_dist on the live portal)
DISTRICT_CODES = {
    "belagavi": 1, "belgaum": 1, "bagalkote": 2, "bagalkot": 2, "vijayapura": 3,
    "bijapur": 3, "kalaburagi": 4, "gulbarga": 4, "bidar": 5, "raichur": 6,
    "koppal": 7, "gadag": 8, "dharwad": 9, "uttara kannada": 10, "karwar": 10,
    "haveri": 11, "ballari": 12, "bellary": 12, "chitradurga": 13, "davanagere": 14,
    "shivamogga": 15, "shimoga": 15, "udupi": 16, "chikkamagaluru": 17,
    "chikmagalur": 17, "tumakuru": 18, "tumkur": 18, "kolar": 19,
    "bengaluru urban": 20, "bangalore urban": 20, "bengaluru rural": 21,
    "bangalore rural": 21, "mandya": 22, "hassan": 23, "dakshina kannada": 24,
    "mangalore": 24, "kodagu": 25, "coorg": 25, "mysuru": 26, "mysore": 26,
    "chamarajanagar": 27, "chikkaballapur": 28, "chikballapur": 28,
    "bengaluru south": 29, "bangalore south": 29, "yadgiri": 30, "yadgir": 30,
    "vijayanagara": 31,
}

# Kannada → Latin (rough phonetic; only consonant skeletons are compared)
KN_MAP = {
    "ಕ": "k", "ಖ": "kh", "ಗ": "g", "ಘ": "gh", "ಙ": "ng",
    "ಚ": "ch", "ಛ": "ch", "ಜ": "j", "ಝ": "jh", "ಞ": "n",
    "ಟ": "t", "ಠ": "th", "ಡ": "d", "ಢ": "dh", "ಣ": "n",
    "ತ": "t", "ಥ": "th", "ದ": "d", "ಧ": "dh", "ನ": "n",
    "ಪ": "p", "ಫ": "ph", "ಬ": "b", "ಭ": "bh", "ಮ": "m",
    "ಯ": "y", "ರ": "r", "ಱ": "r", "ಲ": "l", "ಳ": "l", "ೞ": "l",
    "ವ": "v", "ಶ": "sh", "ಷ": "sh", "ಸ": "s", "ಹ": "h",
    "ಅ": "a", "ಆ": "a", "ಇ": "i", "ಈ": "i", "ಉ": "u", "ಊ": "u",
    "ಋ": "ru", "ಎ": "e", "ಏ": "e", "ಐ": "ai", "ಒ": "o", "ಓ": "o", "ಔ": "au",
    "ಂ": "n", "ಃ": "h",
    # dependent vowel signs + virama → dropped (skeleton ignores vowels anyway)
    "ಾ": "", "ಿ": "", "ೀ": "", "ು": "", "ೂ": "", "ೃ": "",
    "ೆ": "", "ೇ": "", "ೈ": "", "ೊ": "", "ೋ": "", "ೌ": "", "್": "",
}


def kn_translit(s: str) -> str:
    return "".join(KN_MAP.get(ch, ch) for ch in s)


def skeleton(s: str) -> str:
    """Consonant skeleton for fuzzy Latin/Kannada name matching:
    hoskote → hskt, ಹೊಸಕೋಟೆ → hosakote → hskt."""
    s = kn_translit(s).lower().replace("w", "v").replace("f", "p")
    s = re.sub(r"[^a-z]", "", s)
    s = re.sub(r"[aeiou]", "", s)
    return re.sub(r"(.)\1+", r"\1", s)


PAYMENT_HINT_WORDS = ["payment", "pay now", "wishlist", "ಇಚ್ಛೆಪಟ್ಟ", "ಪಾವತಿ",
                      "billdesk", "razorpay", "sbiepay", "khajane", "checkout"]
DOWNLOAD_LINK_WORDS = ["download", "ಡೌನ್"]


def log(msg: str, level: str = "INFO") -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level:5}] {msg}", flush=True)


def slug(s: str) -> str:
    return re.sub(r"[^\w-]", "_", str(s))


# ═══════════════════════════════════════════════════════════════════════════════════════
# BROWSER / SESSION
# ═══════════════════════════════════════════════════════════════════════════════════════

def launch(p):
    """Headed persistent-context Chromium — session cookies survive between runs."""
    ctx = p.chromium.launch_persistent_context(
        str(PROFILE_DIR),
        headless=False,
        accept_downloads=True,
        ignore_https_errors=True,          # govt portal SSL chain is flaky (same as v11)
        viewport={"width": 1400, "height": 900},
        args=["--disable-blink-features=AutomationControlled"],
    )
    ctx.set_default_timeout(NAV_TIMEOUT)
    # The portal's auth cookie is a session cookie, which Chromium drops on exit
    # even in a persistent profile — restore it from the last saved state.
    if STATE_FILE.exists():
        try:
            cookies = json.loads(STATE_FILE.read_text()).get("cookies", [])
            if cookies:
                ctx.add_cookies(cookies)
                log(f"Restored {len(cookies)} cookie(s) from {STATE_FILE.name}")
        except Exception as e:
            log(f"Could not restore session state: {e}", "WARN")
    return ctx


def save_session(page: Page) -> None:
    try:
        page.context.storage_state(path=str(STATE_FILE))
        log(f"Session state saved to {STATE_FILE.name}")
    except Exception as e:
        log(f"Could not save session state: {e}", "WARN")


def is_login_page(page: Page) -> bool:
    try:
        return page.locator("input[placeholder*='OTP'], input[placeholder*='ಓಟಿಪಿ']").count() > 0
    except Exception:
        return False


def ensure_logged_in(page: Page, wait_minutes: int = 10) -> bool:
    """Navigate to RRSRequest; if bounced to login, wait for the human to log in."""
    page.goto(RRS_REQUEST_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    if not is_login_page(page):
        log("Session active — RRS request page reached.")
        save_session(page)
        return True

    log("═" * 70)
    log("LOGIN REQUIRED — enter your MOBILE NUMBER and OTP in the browser")
    log("window that just opened. This tool never types credentials.")
    log(f"Waiting up to {wait_minutes} minutes...")
    log("═" * 70)
    deadline = time.time() + wait_minutes * 60
    while time.time() < deadline:
        time.sleep(3)
        try:
            if not is_login_page(page):
                if "RRSRequest" not in page.url:
                    page.goto(RRS_REQUEST_URL, wait_until="domcontentloaded")
                    page.wait_for_load_state("networkidle")
                if not is_login_page(page):
                    log("Login detected.")
                    save_session(page)
                    return True
        except Exception:
            pass  # mid-navigation; retry next tick
    log("Timed out waiting for login.", "ERROR")
    return False


def wait_for_rrs_page(page: Page, attempts: int = 3) -> bool:
    """Make sure the RRSRequest wizard DOM is actually loaded (post-login
    redirects can land on Dashboard or an interstitial first)."""
    for i in range(1, attempts + 1):
        try:
            if "RRSRequest" not in page.url:
                page.goto(RRS_REQUEST_URL, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            page.wait_for_selector(SEL["desc"], state="attached", timeout=15_000)
            return True
        except Exception as e:
            log(f"RRSRequest wizard not ready (attempt {i}/{attempts}, url={page.url}): {e}", "WARN")
            try:
                page.goto(RRS_REQUEST_URL, wait_until="domcontentloaded")
            except Exception:
                pass
            time.sleep(2)
    debug = SCRIPT_DIR / "rrs_debug_page.html"
    try:
        debug.write_text(page.content())
        log(f"Wizard never appeared — page HTML saved to {debug.name} (url={page.url})", "ERROR")
    except Exception:
        log(f"Wizard never appeared (url={page.url})", "ERROR")
    return False


# ═══════════════════════════════════════════════════════════════════════════════════════
# SWEETALERT HELPERS (portal uses Swal for confirms and error messages)
# ═══════════════════════════════════════════════════════════════════════════════════════

def swal_text(page: Page) -> str:
    try:
        loc = page.locator(SEL["swal_html"])
        if loc.count() and loc.first.is_visible():
            return loc.first.inner_text().strip()
    except Exception:
        pass
    return ""


def toast_text(page: Page) -> str:
    """Read any visible bootstrap-notify toast (alertError / alertErrorModal)."""
    try:
        loc = page.locator("[data-notify='container']")
        for i in range(loc.count()):
            el = loc.nth(i)
            if el.is_visible():
                t = re.sub(r"\s+", " ", el.inner_text()).strip(" ×x")
                if t:
                    return t.strip()
    except Exception:
        pass
    return ""


def swal_confirm_if_shown(page: Page, timeout_s: float = 6) -> str:
    """Click the confirm/OK button of any visible SweetAlert; return its message."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        msg = swal_text(page)
        if msg:
            try:
                btn = page.locator(SEL["swal_confirm"]).first
                if btn.is_visible():
                    btn.click()
                    time.sleep(0.5)
            except Exception:
                pass
            return msg
        time.sleep(0.3)
    return ""


# ═══════════════════════════════════════════════════════════════════════════════════════
# DROPDOWN CASCADE
# ═══════════════════════════════════════════════════════════════════════════════════════

def wait_for_options(page: Page, selector: str, field: str) -> bool:
    """AJAX-poll until a dependent dropdown has real options (v11 pattern)."""
    deadline = time.time() + AJAX_POLL_TIMEOUT
    while time.time() < deadline:
        try:
            n = page.eval_on_selector(selector, "e => e.options.length")
            if n > 1:
                return True
        except Exception:
            pass
        time.sleep(AJAX_POLL_INTERVAL)
    log(f"{field} dropdown never populated (>{AJAX_POLL_TIMEOUT}s)", "ERROR")
    return False


def select_smart(page: Page, selector: str, wanted: str, field: str) -> bool:
    """Select by census code (digits) or by option text (exact, then substring)."""
    wanted = str(wanted).strip()
    options = page.eval_on_selector_all(
        f"{selector} option", "os => os.map(o => ({v: o.value, t: o.text.trim()}))")
    pick = None
    if wanted.isdigit():
        pick = next((o for o in options if o["v"] == wanted), None)
    if pick is None:
        wl = wanted.lower()
        pick = next((o for o in options if o["t"].lower() == wl), None) or \
               next((o for o in options if wl in o["t"].lower() and o["v"] != "0"), None)
    if pick is None and field == "district":
        code = DISTRICT_CODES.get(wanted.lower())
        if code is not None:
            pick = next((o for o in options if o["v"] == str(code)), None)
    if pick is None:
        # transliteration fallback: English name vs Kannada option text
        want_sk = skeleton(wanted)
        if want_sk:
            hits = [o for o in options if o["v"] != "0" and skeleton(o["t"]) == want_sk]
            if not hits:
                hits = [o for o in options if o["v"] != "0" and
                        (want_sk in skeleton(o["t"]) or skeleton(o["t"]) in want_sk)]
            if len(hits) == 1:
                pick = hits[0]
                log(f"{field}: matched '{wanted}' → '{pick['t']}' by transliteration")
            elif len(hits) > 1:
                names = " | ".join(f"{o['v']}:{o['t']}" for o in hits)
                log(f"'{wanted}' is ambiguous in {field}: {names} — use the code", "ERROR")
                return False
    if pick is None:
        avail = " | ".join(f"{o['v']}:{o['t']}" for o in options if o["v"] != "0")
        log(f"'{wanted}' not found in {field}. Options: {avail}", "ERROR")
        return False
    page.select_option(selector, value=pick["v"])   # fires change → inline fnBind* cascade
    log(f"{field:8} = {pick['t']} (code {pick['v']})")
    return True


# ═══════════════════════════════════════════════════════════════════════════════════════
# WIZARD NAVIGATION + REQUEST BOOTSTRAP (wizard step 1)
# ═══════════════════════════════════════════════════════════════════════════════════════

def is_visible_js(page: Page, selector: str) -> bool:
    try:
        return bool(page.eval_on_selector(selector, "e => e.offsetParent !== null"))
    except Exception:
        return False


def wizard_goto(page: Page, step: int, probe_selector: str) -> bool:
    """Drive the jQuery-Steps wizard until `probe_selector` is visible.
    Tries the step's tab link first, then the Next button."""
    if is_visible_js(page, probe_selector):
        return True
    for attempt in range(6):
        try:
            page.eval_on_selector(f"a[href='#wizard-h-{step}']", "e => e.click()")
        except Exception:
            pass
        time.sleep(0.8)
        if is_visible_js(page, probe_selector):
            return True
        try:  # future steps may be locked for direct clicks — advance with Next
            page.eval_on_selector("a[href='#next']", "e => e.click()")
        except Exception:
            pass
        time.sleep(0.8)
        if is_visible_js(page, probe_selector):
            return True
    log(f"Could not reach wizard step {step} ({probe_selector} never became visible)", "ERROR")
    return False


def js_fill(page: Page, selector: str, value: str) -> None:
    """Set a field's value through the DOM (works even while the panel is hidden)."""
    page.eval_on_selector(selector, """(e, v) => {
        e.value = v;
        e.dispatchEvent(new Event('input',  {bubbles: true}));
        e.dispatchEvent(new Event('change', {bubbles: true}));
    }""", value)


def _create_request_once(page: Page, desc: str) -> str:
    """Fill description + language, click Create, confirm. Returns the portal's
    follow-up message ('' if none)."""
    wizard_goto(page, 1, SEL["desc"])   # best effort; JS fallbacks below work anyway
    try:
        page.fill(SEL["desc"], desc, timeout=8000)
    except Exception:
        js_fill(page, SEL["desc"], desc)
        log("Filled description via DOM (panel hidden to Playwright)")
    try:
        page.check(SEL["lang_en"], timeout=5000)
    except Exception:
        page.eval_on_selector(SEL["lang_en"], "e => { e.checked = true; "
                              "e.dispatchEvent(new Event('change', {bubbles: true})); }")
    try:
        page.click(SEL["create_request"], timeout=8000)
    except Exception:
        page.eval_on_selector(SEL["create_request"], "e => e.click()")
    # SweetAlert confirm dialog → ✔️ submit
    msg = swal_confirm_if_shown(page, timeout_s=8)
    log(f"Create-request dialog: {msg or '(none)'}")
    time.sleep(1.0)
    # a follow-up Swal or toast may report success / "Request already exist" — both fine
    deadline = time.time() + 5
    while time.time() < deadline:
        msg2 = swal_text(page) or toast_text(page)
        if msg2:
            swal_confirm_if_shown(page, timeout_s=1)
            log(f"Portal says: {msg2}")
            return msg2
        time.sleep(0.3)
    return ""


def ensure_request(page: Page, desc: str) -> bool:
    """Search requires #txt_file_desc >= 20 non-space chars + a saved request."""
    if not wait_for_rrs_page(page):
        return False
    current = page.eval_on_selector(SEL["desc"], "e => e.value") or ""
    if len(re.sub(r"\s", "", current)) >= 20:
        log("Active request already present — skipping create.")
        return True
    for attempt in (1, 2):
        msg = _create_request_once(page, desc)
        if "applicant" not in msg.lower():
            return True     # created, or already exists — either way search works
        if attempt == 2:
            log("Applicant details still missing after manual fill.", "ERROR")
            return False
        # Applicant profile isn't saved on the portal — that's the user's
        # personal data (and saving it may ask for an OTP), so hand it over.
        wizard_goto(page, 0, "#txt_name")
        log("═" * 70)
        log("APPLICANT DETAILS NOT SAVED ON THE PORTAL (one-time step).")
        log("In the browser window: complete the fields on the first wizard")
        log("step (address etc.), click ಉಳಿಸಿ / Save, finish any OTP prompt,")
        log("then press ENTER here to continue...")
        log("═" * 70)
        try:
            input()
        except EOFError:
            log("Non-interactive session: waiting 120s instead of ENTER", "WARN")
            time.sleep(120)
    return False


# ═══════════════════════════════════════════════════════════════════════════════════════
# SEARCH BY SURVEY NUMBER (wizard step 2 modal)
# ═══════════════════════════════════════════════════════════════════════════════════════

def open_survey_modal(page: Page) -> bool:
    try:
        page.evaluate("fnShowSurveyModalPopUp()")
        page.wait_for_selector(f"{SEL['survey_modal']}.show, {SEL['survey_modal']}[style*='display: block']",
                               timeout=10_000)
        return True
    except Exception as e:
        log(f"Could not open survey-search modal: {e}", "ERROR")
        return False


def fill_location_cascade(page: Page, args) -> bool:
    cascade = [("district", SEL["district"], args.district),
               ("taluk",    SEL["taluk"],    args.taluk),
               ("hobli",    SEL["hobli"],    args.hobli),
               ("village",  SEL["village"],  args.village)]
    for field, selector, wanted in cascade:
        if not wait_for_options(page, selector, field):
            return False
        if not select_smart(page, selector, wanted, field):
            return False
        time.sleep(0.8)  # let dependent AJAX fire
    if args.file_reg and args.file_reg.upper() != "ALL":
        select_smart(page, SEL["file_reg"], args.file_reg, "file_reg")
        time.sleep(0.8)
        if args.file_reg_type:
            wait_for_options(page, SEL["file_reg_type"], "file_reg_type")
            select_smart(page, SEL["file_reg_type"], args.file_reg_type, "file_reg_type")
    return True


def run_search(page: Page, survey: str) -> str:
    """Fill survey number, click Search; return 'ok', 'empty' or error text."""
    page.fill(SEL["survey_no"], str(survey))
    before = page.eval_on_selector(SEL["results"], "e => e.innerHTML.length")
    page.click(SEL["search_btn"])
    # Either the modal closes and #divSearchedData fills, or an error toast
    # (bootstrap-notify, auto-dismisses in ~5s) / Swal appears — poll fast.
    deadline = time.time() + 40
    while time.time() < deadline:
        msg = swal_text(page) or toast_text(page)
        if msg:
            swal_confirm_if_shown(page, timeout_s=1)
            if "no results" in msg.lower():
                return "empty"
            return msg
        try:
            after = page.eval_on_selector(SEL["results"], "e => e.innerHTML.length")
            visible = page.eval_on_selector(
                SEL["survey_modal"], "e => getComputedStyle(e).display !== 'none'")
            if after != before and not visible:
                return "ok"
        except Exception:
            pass
        time.sleep(0.3)
    # nothing detected — save what the page looks like for diagnosis
    debug = SCRIPT_DIR / "rrs_debug_search.html"
    try:
        debug.write_text(page.content())
        log(f"Search state dumped to {debug.name}", "WARN")
    except Exception:
        pass
    return "timeout waiting for search results"


def parse_results(page: Page):
    """Extract the document cards from the server-rendered results block.
    Each result is a .deal_box card carrying a fnViewFile(FileID, OfficeCode)
    handler and the document's descriptive text."""
    return page.eval_on_selector(SEL["results"], """div => {
        const rows = [];
        const seen = new Set();
        for (const el of div.querySelectorAll('[onclick*="fnViewFile"]')) {
            const oc = el.getAttribute('onclick');
            const m = oc.match(/fnViewFile\\((\\d+)\\s*,\\s*(\\d+)\\)/);
            if (!m || seen.has(m[1])) continue;
            seen.add(m[1]);
            const card = el.closest('.deal_box') || el.closest('.col-md-4, .col') || el;
            const text = card.innerText.replace(/\\s+/g, ' ')
                .replace(/ದಾಖಲೆಗಳನ್ನು ವೀಕ್ಷಿಸಿ\\s*\\/?\\s*View Document/g, '').trim();
            rows.push({file_id: m[1], office_code: m[2], cells: [text],
                       clicks: [oc], links: []});
        }
        // fallback: classic table rows, if the portal ever renders those
        if (!rows.length) {
            for (const tr of div.querySelectorAll('tr')) {
                const cells = [...tr.querySelectorAll('td,th')].map(c => c.innerText.trim());
                if (!cells.length) continue;
                rows.push({file_id: '', office_code: '', cells,
                           clicks: [...tr.querySelectorAll('[onclick]')].map(e => e.getAttribute('onclick')),
                           links: [...tr.querySelectorAll('a[href]')].map(a => a.getAttribute('href'))});
            }
        }
        return {rows, html_len: div.innerHTML.length};
    }""")



# ═══════════════════════════════════════════════════════════════════════════════════════
# DOWNLOAD / PAYMENT HAND-OFF (only used for --check-dashboard, i.e. already-paid docs)
# ═══════════════════════════════════════════════════════════════════════════════════════

def wait_if_payment(page: Page) -> None:
    """If the portal routed us into wishlist/payment, hand control to the human."""
    try:
        body = page.inner_text("body", timeout=5000).lower()
    except Exception:
        return
    if any(w in body for w in PAYMENT_HINT_WORDS) and ("₹" in body or "rs" in body or "amount" in body):
        log("═" * 70)
        log("WISHLIST / PAYMENT STEP — complete it YOURSELF in the browser")
        log("(Rs 10 per page). Press ENTER here once payment is done...")
        log("═" * 70)
        try:
            input()
        except EOFError:
            log("Non-interactive session: waiting 180s instead of ENTER", "WARN")
            time.sleep(180)


def harvest_downloads(page: Page, out_dir: Path, tag: str) -> int:
    """Click download links (results page or Dashboard) and save the files."""
    got = 0
    els = []
    for el in page.query_selector_all("a, button, input[type=button]"):
        try:
            if not el.is_visible():
                continue
            text = (el.inner_text() or el.get_attribute("value") or "").strip().lower()
            if any(w in text for w in DOWNLOAD_LINK_WORDS):
                els.append(el)
        except Exception:
            continue
    if not els:
        return 0
    out_dir.mkdir(parents=True, exist_ok=True)
    for i, el in enumerate(els, 1):
        try:
            with page.expect_download(timeout=45_000) as dl_info:
                el.click()
            dl = dl_info.value
            suffix = Path(dl.suggested_filename or "doc.pdf").suffix or ".pdf"
            dest = out_dir / f"{tag}_{i:02d}{suffix}"
            dl.save_as(str(dest))
            log(f"Saved {dest}")
            got += 1
        except PWTimeout:
            log(f"Download candidate {i}: no file produced (viewer link or unpaid doc)", "WARN")
        except Exception as e:
            log(f"Download candidate {i} failed: {e}", "WARN")
    return got


# ═══════════════════════════════════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════════════

def cmd_login(args) -> int:
    with sync_playwright() as p:
        ctx = launch(p)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        ok = ensure_logged_in(page, wait_minutes=args.wait)
        ctx.close()
    return 0 if ok else 1


def cmd_map(args) -> int:
    """Dump the RRSRequest page structure for selector maintenance."""
    with sync_playwright() as p:
        ctx = launch(p)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        if not ensure_logged_in(page):
            ctx.close(); return 1
        m = {
            "url": page.url, "captured": datetime.now().isoformat(),
            "selects": page.eval_on_selector_all("select", """els => els.map(e => ({
                id: e.id, options: e.options.length,
                first: e.options[0] ? e.options[0].text.trim() : ''}))"""),
            "inputs": page.eval_on_selector_all("input:not([type=hidden]), textarea",
                """els => els.map(e => ({id: e.id, type: e.type||'textarea',
                placeholder: e.placeholder||''}))"""),
            "buttons": page.eval_on_selector_all("button, input[type=button]",
                """els => els.map(e => ({id: e.id,
                text: (e.innerText||e.value||'').trim().slice(0,60),
                onclick: (e.getAttribute('onclick')||'').slice(0,80)}))"""),
        }
        PAGE_MAP_FILE.write_text(json.dumps(m, indent=2, ensure_ascii=False))
        PAGE_HTML_FILE.write_text(page.content())
        log(f"Mapped → {PAGE_MAP_FILE.name} ({len(m['selects'])} selects, "
            f"{len(m['inputs'])} inputs, {len(m['buttons'])} buttons)")
        ctx.close()
    return 0


def parse_surveys(spec: str):
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part and "/" not in part:
            a, b = part.split("-", 1)
            out.extend(str(n) for n in range(int(a), int(b) + 1))
        else:
            out.append(part)     # keeps "12/*/3" style intact
    return out


def cmd_download(args) -> int:
    surveys = parse_surveys(args.surveys)
    out_root = Path(args.out) / re.sub(r"[^\wಀ-೿-]+", "_", str(args.village))
    out_root.mkdir(parents=True, exist_ok=True)
    desc = args.desc or (
        f"Old revenue records of survey numbers {args.surveys} of village "
        f"{args.village}, {args.hobli} hobli, {args.taluk} taluk, {args.district} district")
    log(f"Surveys: {surveys}  →  {out_root}")

    index_rows, total_files = [], 0
    with sync_playwright() as p:
        ctx = launch(p)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        if not ensure_logged_in(page):
            ctx.close(); return 1
        if not ensure_request(page, desc):
            ctx.close(); return 1
        # move wizard to the search step (cards must be visible for later clicks)
        wizard_goto(page, 2, ".search-option-card")

        cascade_done = False
        for survey in surveys:
            log(f"── Survey {survey} " + "─" * 50)
            try:
                if not open_survey_modal(page):
                    break
                if not cascade_done:
                    if not fill_location_cascade(page, args):
                        break
                    cascade_done = True   # modal keeps selections between searches
                status = run_search(page, survey)
                if status == "empty":
                    log(f"Survey {survey}: no documents in the record room index")
                    index_rows.append({"survey": survey, "file_id": "", "office_code": "",
                                       "document": "NO RESULTS"})
                    continue
                if status != "ok":
                    log(f"Survey {survey}: {status}", "WARN")
                    index_rows.append({"survey": survey, "file_id": "", "office_code": "",
                                       "document": f"ERROR: {status}"})
                    continue
                res = parse_results(page)
                log(f"Survey {survey}: {len(res['rows'])} document(s) available")
                (out_root / f"Sy{slug(survey)}_results.html").write_text(
                    page.eval_on_selector(SEL["results"], "e => e.outerHTML"))
                for r in res["rows"]:
                    log(f"  • [{r.get('file_id','')}] {r['cells'][0][:110]}")
                    index_rows.append({"survey": survey,
                                       "file_id": r.get("file_id", ""),
                                       "office_code": r.get("office_code", ""),
                                       "document": " | ".join(r["cells"])})
            except Exception as e:
                log(f"Survey {survey} failed: {e}", "ERROR")
                index_rows.append({"survey": survey, "file_id": "", "office_code": "",
                                   "document": f"EXCEPTION: {e}"})

        # results index CSV
        if index_rows:
            csv_path = out_root / "rrs_search_index.csv"
            with open(csv_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["survey", "file_id", "office_code", "document"])
                w.writeheader(); w.writerows(index_rows)
            log(f"Search index → {csv_path}")

        # paid documents land on the Dashboard — sweep it for download links too
        if args.check_dashboard:
            log("Checking Dashboard for ready/paid documents...")
            page.goto(DASHBOARD_URL, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            total_files += harvest_downloads(page, out_root, "Dashboard")

        ctx.close()
    log(f"DONE — {total_files} file(s) downloaded, {len(index_rows)} index rows in {out_root}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Bhu Suraksha (Service4/RRS) document search+download by survey number")
    sub = ap.add_subparsers(dest="cmd", required=True)

    lp = sub.add_parser("login", help="Open the portal and wait for you to log in (mobile+OTP)")
    lp.add_argument("--wait", type=int, default=10, help="Minutes to wait for login (default 10)")
    lp.set_defaults(fn=cmd_login)

    mp = sub.add_parser("map", help="Dump the RRS request page structure to rrs_page_map.json")
    mp.set_defaults(fn=cmd_map)

    dp = sub.add_parser("download", help="Search by survey number(s), index results, download files")
    dp.add_argument("--district", required=True, help="English/Kannada name or census code (e.g. 'Bangalore Rural' or 21)")
    dp.add_argument("--taluk", required=True, help="Name or code (e.g. 'Hoskote' or 4)")
    dp.add_argument("--hobli", required=True, help="Name or code (e.g. 'Kasaba' or 1)")
    dp.add_argument("--village", required=True, help="Name (Kannada ok) or code")
    dp.add_argument("--surveys", required=True, help="e.g. '12', '1,5,10-15', or '12/*/3'")
    dp.add_argument("--file-reg", default="ALL", help="ALL | File | Register (default ALL)")
    dp.add_argument("--file-reg-type", default="", help="File/Register type text (optional)")
    dp.add_argument("--desc", default="", help="Request description (>=20 chars; auto-generated if omitted)")
    dp.add_argument("--out", default=str(DEFAULT_OUT_DIR), help=f"Output dir (default {DEFAULT_OUT_DIR})")
    dp.add_argument("--check-dashboard", action="store_true",
                    help="Also sweep /RRS/Dashboard for ready (paid) documents")
    dp.set_defaults(fn=cmd_download)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
