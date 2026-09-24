#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║     POWER-BHOOMI RRS CATALOG v1.0 — state-wide index of Bhu Suraksha record-room     ║
║                   files (https://recordroom.karnataka.gov.in/Service4)               ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  Builds a sortable catalog of every scanned file the record room lists, village by  ║
║  village, so documents can be shortlisted and paid for in one go.                    ║
║                                                                                      ║
║  HOW IT WORKS (verified against the live portal, Sept 2026)                          ║
║   • Location tree via the portal's own JSON cascade endpoints:                       ║
║       GetTaluka {DistrictCode} → GetHobli {+TalukCode} → GetVillage {+HobliCode}     ║
║   • One GetFilesBySurveyNumber query per village with a BLANK survey number —        ║
║     the portal then returns every file tagged to that village (a superset of any     ║
║     per-survey search). The server renders one card per survey tag, so cards are     ║
║     de-duplicated per file and the tag count is kept.                                ║
║   • Card fields: office, file no., subject, year, File/Register. Survey numbers are  ║
║     not a card field; they are extracted from the subject where it names them        ║
║     (e.g. "ಸ. ನಂ 7/3") into survey_hint. For exact per-survey tags on a shortlisted  ║
║     village, run bhoomi_rrs_downloader.py download --surveys ... on it.              ║
║                                                                                      ║
║  POLITENESS / SAFETY                                                                 ║
║   • Throttled (default ~1 request/s with jitter), resumable, checkpointed to SQLite   ║
║     after every village. Ctrl-C anytime; re-run the same command to continue.        ║
║   • Read-only: search + listing only. Never opens a viewer, wishlist or payment.     ║
║   • Login is always manual (mobile + OTP in the browser window); if the session      ║
║     drops mid-crawl the tool pauses and waits for you to log in again.               ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

Usage:
    python bhoomi_rrs_catalog.py crawl                          # whole state, phased
    python bhoomi_rrs_catalog.py crawl --district "Bangalore Rural" --taluk 4
    python bhoomi_rrs_catalog.py status
    python bhoomi_rrs_catalog.py export [--district 21] [--out file.csv]
    python bhoomi_rrs_catalog.py export --split          # one Excel-sized CSV per district
"""

import argparse
import csv
import json
import random
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page

import bhoomi_rrs_downloader as rrs

# ═══════════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════════════

DB_PATH = Path.home() / "Documents" / "POWER-BHOOMI" / "rrs_catalog.db"
EXPORT_DIR = Path.home() / "Downloads" / "BHOOMI_RRS"
REQUEST_DESC = ("Catalog of old revenue records (RTC, mutation and survey files) "
                "for village-level land record research")

DEFAULT_DELAY = 1.0        # s between village queries (jittered ±30%)
TREE_DELAY = 0.3           # s between location-tree calls
MAX_ATTEMPTS = 4           # per village before marking it 'error'
FETCH_TIMEOUT_MS = 45_000

LOGIN_MARKERS = ("GetLast3DigitOfSLBIP", "ಓಟಿಪಿ ಅನ್ನು ನಮೂದಿಸಿ", "ಮೊಬೈಲ್ ಸಂಖ್ಯೆ ನಮೂದಿಸಿ")

try:
    import lxml  # noqa: F401
    HTML_PARSER = "lxml"
except ImportError:
    HTML_PARSER = "html.parser"

# First name per code in rrs.DISTRICT_CODES is the current official English name
DISTRICT_EN = {}
for _name, _code in rrs.DISTRICT_CODES.items():
    DISTRICT_EN.setdefault(_code, _name.title())

SURVEY_NUM = r"[0-9೦-೯]+(?:/[0-9A-Za-zಀ-೿*]+)*"
SURVEY_RE = re.compile(
    r"(?:ಸ\.?\s*ನಂ|ಸರ್ವೆ\s*ನಂ|ಸರ್ವೇ\s*ನಂ|sy\.?\s*no|s\.\s*no|survey\s*no)\.?\s*[:\-]?\s*"
    rf"({SURVEY_NUM}(?:\s*(?:,|&|and|ಮತ್ತು)\s*{SURVEY_NUM})*)",
    re.IGNORECASE)

SCHEMA = """
CREATE TABLE IF NOT EXISTS districts(
    dist_code INTEGER PRIMARY KEY, name TEXT, tree_done INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS villages(
    dist_code INTEGER, taluk_code INTEGER, hobli_code INTEGER, village_code INTEGER,
    dist_name TEXT, taluk_name TEXT, hobli_name TEXT, village_name TEXT,
    status TEXT DEFAULT 'pending', files INTEGER, cards INTEGER,
    error TEXT, crawled_at TEXT,
    PRIMARY KEY(dist_code, taluk_code, hobli_code, village_code));
CREATE TABLE IF NOT EXISTS files(
    file_id INTEGER PRIMARY KEY, office_code INTEGER, office TEXT, file_no TEXT,
    subject TEXT, year TEXT, kind TEXT, first_seen TEXT);
CREATE TABLE IF NOT EXISTS village_files(
    dist_code INTEGER, taluk_code INTEGER, hobli_code INTEGER, village_code INTEGER,
    file_id INTEGER, tags INTEGER, survey_hint TEXT,
    PRIMARY KEY(dist_code, taluk_code, hobli_code, village_code, file_id));
CREATE INDEX IF NOT EXISTS ix_vf_file ON village_files(file_id);
CREATE INDEX IF NOT EXISTS ix_v_status ON villages(status);
"""

log = rrs.log


class SessionLost(Exception):
    pass


class RequestMissing(Exception):
    pass


class Transient(Exception):
    pass


class ApplicantMissing(Exception):
    pass


# ═══════════════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════════════

def open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH))
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ═══════════════════════════════════════════════════════════════════════════════════════
# PORTAL CALLS (fetch from inside the logged-in page, so the session cookie applies)
# ═══════════════════════════════════════════════════════════════════════════════════════

POST_JSON = """async ([url, body, timeoutMs]) => {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), timeoutMs);
    try {
        const r = await fetch(url, {method: 'POST', credentials: 'same-origin',
            headers: {'Content-Type': 'application/json;charset:utf-8'},
            body: JSON.stringify(body), signal: ctl.signal});
        return {status: r.status, url: r.url, text: await r.text()};
    } catch (e) {
        return {status: 0, url: '', text: String(e)};
    } finally { clearTimeout(t); }
}"""


def post(page: Page, path: str, body: dict) -> str:
    try:
        r = page.evaluate(POST_JSON, [path, body, FETCH_TIMEOUT_MS])
    except Exception as e:                       # page navigated / crashed mid-call
        raise Transient(f"evaluate failed: {e}")
    if r["status"] == 0:
        raise Transient(r["text"][:200])
    if r["status"] in (401, 403) or path not in r["url"] \
            or any(m in r["text"] for m in LOGIN_MARKERS):
        raise SessionLost(f"HTTP {r['status']} → {r['url']}")
    if r["status"] >= 500 or r["status"] == 429:
        raise Transient(f"HTTP {r['status']}")
    if r["status"] != 200:
        raise Transient(f"HTTP {r['status']}: {r['text'][:120]}")
    return r["text"]


def post_list(page: Page, path: str, body: dict) -> list:
    text = post(page, path, body)
    try:
        data = json.loads(text)
        if isinstance(data, str):                # endpoints return a JSON-encoded string
            data = json.loads(data)
        return data or []
    except (json.JSONDecodeError, TypeError):
        raise Transient(f"{path} returned non-JSON: {text[:120]}")


def get_taluks(page, d):
    return [(x["TALUKA_CODE"], x["TALUKA_NAME"]) for x in
            post_list(page, "/Service4/RRS/GetTaluka", {"DistrictCode": str(d)})]


def get_hoblis(page, d, t):
    return [(x["HOBLI_CODE"], x["HOBLI_NAME"]) for x in
            post_list(page, "/Service4/RRS/GetHobli",
                      {"DistrictCode": str(d), "TalukCode": str(t)})]


def get_villages(page, d, t, h):
    return [(x["VILLAGE_CODE"], x["VILLAGE_NAME"]) for x in
            post_list(page, "/Service4/RRS/GetVillage",
                      {"DistrictCode": str(d), "TalukCode": str(t), "HobliCode": str(h)})]


def search_village(page, v: sqlite3.Row):
    """All files tagged to a village: blank survey number. Returns HTML or None (no files)."""
    body = {"census_dist_code": str(v["dist_code"]), "census_taluk_code": str(v["taluk_code"]),
            "hobli_code": str(v["hobli_code"]), "village_code": str(v["village_code"]),
            "FileReg": "0", "survey_no": "", "fileorregistertypeval": "null",
            "dist_name": v["dist_name"] or "", "taluk_name": v["taluk_name"] or "",
            "hobli_name": v["hobli_name"] or "", "village_name": v["village_name"] or "",
            "fileorregister": "ALL", "fileorregistertype": ""}
    text = post(page, "/Service4/RRS/GetFilesBySurveyNumber", body)
    head = text.lstrip()[:1]
    if head in ("{", '"'):
        try:
            j = json.loads(text)
            if isinstance(j, str):
                j = json.loads(j)
            status = (j or {}).get("status", "")
        except (json.JSONDecodeError, AttributeError):
            status = ""
        if status == "ND":
            return None
        if status == "NR":
            raise RequestMissing()
        if status == "NA":
            raise ApplicantMissing()
        raise Transient(f"search status {status or text[:80]}")
    if "fnViewFile" not in text and "Total" not in text:
        raise Transient(f"unexpected search response: {text[:120]}")
    return text


# ═══════════════════════════════════════════════════════════════════════════════════════
# CARD PARSING
# ═══════════════════════════════════════════════════════════════════════════════════════

FIELD_KEYS = (("office", ("office name",)), ("file_no", ("file no",)),
              ("subject", ("sub",)), ("year", ("year",)))


def _field_key(label: str):
    low = label.lower()
    for key, needles in FIELD_KEYS:
        if any(n in low for n in needles):
            return key
    return None


def parse_cards(html: str) -> dict:
    """{file_id: {office_code, office, file_no, subject, year, kind, tags}}"""
    soup = BeautifulSoup(html, HTML_PARSER)
    triggers = soup.select("[onclick*=fnViewFile]")
    # A card is the widest ancestor holding exactly one trigger. The trigger is
    # currently the .card div, and the results panel around the grid is also a
    # .card, so class-based lookup would grab every card at once.
    per_node = {}
    for t in triggers:
        for anc in t.parents:
            per_node[id(anc)] = per_node.get(id(anc), 0) + 1

    def card_of(t):
        node = t
        while (node.parent is not None and node.parent.name != "[document]"
               and per_node.get(id(node.parent), 0) <= 1):
            node = node.parent
        return node

    files = {}
    for btn in triggers:
        m = re.search(r"fnViewFile\((\d+)\s*,\s*(\d+)\)", btn.get("onclick", ""))
        if not m:
            continue
        fid, office_code = int(m.group(1)), int(m.group(2))
        if fid in files:
            files[fid]["tags"] += 1
            continue
        card = card_of(btn)
        rec = {"office_code": office_code, "office": "", "file_no": "", "subject": "",
               "year": "", "kind": "", "tags": 1}
        for lab in card.find_all(["h6", "strong"]):
            label = "".join(lab.find_all(string=True, recursive=False)).strip()
            key = _field_key(label)
            if not key:
                continue
            val = lab.find("span") if lab.name == "h6" else lab.find_next_sibling("span")
            if val:
                rec[key] = re.sub(r"\s+", " ", val.get_text(" ", strip=True))
        badge = card.select_one(".file-type-badge")
        rec["kind"] = badge.get_text(strip=True) if badge else ""
        files[fid] = rec
    return files


KN_DIGITS = str.maketrans("೦೧೨೩೪೫೬೭೮೯", "0123456789")


def survey_hint(subject: str) -> str:
    found = []
    for m in SURVEY_RE.finditer(subject or ""):
        for part in re.split(r"\s*(?:,|&|and|ಮತ್ತು)\s*", m.group(1)):
            part = part.translate(KN_DIGITS)
            if part and part not in found:
                found.append(part)
    return ", ".join(found)


# ═══════════════════════════════════════════════════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════════════════════════════════════════════════

def start_session(page: Page) -> bool:
    return rrs.ensure_logged_in(page) and rrs.ensure_request(page, REQUEST_DESC)


def recover(page: Page, exc: Exception) -> bool:
    if isinstance(exc, RequestMissing):
        log("Portal lost the active request — creating a new one", "WARN")
    else:
        log(f"Session lost ({exc}) — log in again in the browser window", "WARN")
    try:
        page.goto(rrs.RRS_REQUEST_URL, wait_until="domcontentloaded")
    except Exception:
        pass
    return start_session(page)


def with_retries(page: Page, fn, *args):
    """Run a portal call; heal the session / back off on transient errors."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fn(page, *args)
        except (SessionLost, RequestMissing) as e:
            if not recover(page, e):
                raise SystemExit("Could not re-establish the portal session — stopping. "
                                 "Re-run the same command to resume.")
        except Transient as e:
            if attempt == MAX_ATTEMPTS:
                raise
            wait = min(60, 5 * 2 ** (attempt - 1))
            log(f"Transient error ({e}); retrying in {wait}s [{attempt}/{MAX_ATTEMPTS}]", "WARN")
            time.sleep(wait)
    raise Transient("retries exhausted")


# ═══════════════════════════════════════════════════════════════════════════════════════
# LOCATION TREE
# ═══════════════════════════════════════════════════════════════════════════════════════

def page_districts(page: Page) -> list:
    rrs.wait_for_options(page, rrs.SEL["district"], "district")
    opts = page.eval_on_selector_all(
        f"{rrs.SEL['district']} option", "os => os.map(o => [o.value, o.text.trim()])")
    return [(int(v), t) for v, t in opts if v and v != "0"]


def resolve_districts(con, page, wanted: list) -> list:
    all_d = page_districts(page)
    for code, name in all_d:
        con.execute("INSERT OR IGNORE INTO districts(dist_code, name) VALUES (?,?)", (code, name))
    con.commit()
    if not wanted:
        return [c for c, _ in all_d]
    codes = []
    for w in wanted:
        w = w.strip()
        code = int(w) if w.isdigit() else rrs.DISTRICT_CODES.get(w.lower())
        if code is None:
            sk = rrs.skeleton(w)
            hits = [c for c, n in all_d if n == w or (sk and rrs.skeleton(n) == sk)]
            code = hits[0] if len(hits) == 1 else None
        if code is None or code not in {c for c, _ in all_d}:
            raise SystemExit(f"Unknown district '{w}'. Valid: "
                             + ", ".join(f"{c}={DISTRICT_EN.get(c, n)}" for c, n in all_d))
        codes.append(code)
    return codes


def build_tree(con, page, dist_codes: list, taluk_filter: set) -> None:
    for d in dist_codes:
        row = con.execute("SELECT name, tree_done FROM districts WHERE dist_code=?", (d,)).fetchone()
        if row and row[1] and not taluk_filter:
            continue
        dname = row[0] if row else str(d)
        log(f"Enumerating villages of district {d} ({DISTRICT_EN.get(d, dname)})...")
        taluks = with_retries(page, get_taluks, d)
        n_before = con.execute("SELECT COUNT(*) FROM villages WHERE dist_code=?", (d,)).fetchone()[0]
        for t, tname in taluks:
            if taluk_filter and t not in taluk_filter:
                continue
            time.sleep(TREE_DELAY)
            for h, hname in with_retries(page, get_hoblis, d, t):
                time.sleep(TREE_DELAY)
                vils = with_retries(page, get_villages, d, t, h)
                con.executemany(
                    "INSERT OR IGNORE INTO villages(dist_code, taluk_code, hobli_code, village_code,"
                    " dist_name, taluk_name, hobli_name, village_name) VALUES (?,?,?,?,?,?,?,?)",
                    [(d, t, h, v, dname, tname, hname, vname) for v, vname in vils])
                con.commit()
        if not taluk_filter:
            con.execute("UPDATE districts SET tree_done=1 WHERE dist_code=?", (d,))
            con.commit()
        n_after = con.execute("SELECT COUNT(*) FROM villages WHERE dist_code=?", (d,)).fetchone()[0]
        log(f"  district {d}: {n_after} villages known (+{n_after - n_before} new)")


# ═══════════════════════════════════════════════════════════════════════════════════════
# CRAWL
# ═══════════════════════════════════════════════════════════════════════════════════════

def store_village(con, v, files: dict) -> int:
    new = 0
    key = (v["dist_code"], v["taluk_code"], v["hobli_code"], v["village_code"])
    for fid, r in files.items():
        cur = con.execute(
            "INSERT OR IGNORE INTO files(file_id, office_code, office, file_no, subject, year, kind,"
            " first_seen) VALUES (?,?,?,?,?,?,?,?)",
            (fid, r["office_code"], r["office"], r["file_no"], r["subject"], r["year"],
             r["kind"], now()))
        new += cur.rowcount
        con.execute(
            "INSERT OR REPLACE INTO village_files VALUES (?,?,?,?,?,?,?)",
            (*key, fid, r["tags"], survey_hint(r["subject"])))
    status = "done" if files else "empty"
    con.execute(
        "UPDATE villages SET status=?, files=?, cards=?, error=NULL, crawled_at=?"
        " WHERE dist_code=? AND taluk_code=? AND hobli_code=? AND village_code=?",
        (status, len(files), sum(r["tags"] for r in files.values()), now(), *key))
    con.commit()
    return new


def cmd_crawl(args) -> int:
    con = open_db()
    con.row_factory = sqlite3.Row
    taluk_filter = {int(t) for t in args.taluk} if args.taluk else set()
    if taluk_filter and len(args.district or []) != 1:
        raise SystemExit("--taluk needs exactly one --district")

    with sync_playwright() as p:
        ctx = rrs.launch(p)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        if not start_session(page):
            ctx.close()
            return 1
        dist_codes = resolve_districts(con, page, args.district or [])
        build_tree(con, page, dist_codes, taluk_filter)

        statuses = ("pending", "error") if args.retry_errors else ("pending",)
        q = (f"SELECT * FROM villages WHERE status IN ({','.join('?' * len(statuses))})"
             f" AND dist_code IN ({','.join('?' * len(dist_codes))})")
        params = [*statuses, *dist_codes]
        if taluk_filter:
            q += f" AND taluk_code IN ({','.join('?' * len(taluk_filter))})"
            params += sorted(taluk_filter)
        q += " ORDER BY dist_code, taluk_code, hobli_code, village_code"
        todo = con.execute(q, params).fetchall()
        if args.limit:
            todo = todo[:args.limit]
        log(f"{len(todo)} village(s) to crawl in district(s) {dist_codes}"
            + (f", taluk(s) {sorted(taluk_filter)}" if taluk_filter else ""))

        t0, n_files, n_new = time.time(), 0, 0
        try:
            for i, v in enumerate(todo, 1):
                where = f"{v['dist_code']}/{v['taluk_code']}/{v['hobli_code']}/{v['village_code']}"
                try:
                    html = with_retries(page, search_village, v)
                    files = parse_cards(html) if html else {}
                    new = store_village(con, v, files)
                    n_files += len(files)
                    n_new += new
                    rate = (time.time() - t0) / i
                    eta = (len(todo) - i) * rate / 60
                    log(f"[{i}/{len(todo)}] {where} {v['village_name']}: "
                        f"{len(files)} file(s), {new} new  | ETA {eta:.0f} min")
                except ApplicantMissing:
                    raise SystemExit("Portal says applicant details are missing — complete the "
                                     "applicant step once in the browser, then re-run.")
                except Transient as e:
                    con.execute(
                        "UPDATE villages SET status='error', error=?, crawled_at=?"
                        " WHERE dist_code=? AND taluk_code=? AND hobli_code=? AND village_code=?",
                        (str(e)[:300], now(), v["dist_code"], v["taluk_code"],
                         v["hobli_code"], v["village_code"]))
                    con.commit()
                    log(f"[{i}/{len(todo)}] {where}: gave up ({e}) — retry later with "
                        "--retry-errors", "ERROR")
                time.sleep(args.delay * random.uniform(0.7, 1.3))
        except KeyboardInterrupt:
            log("Interrupted — progress is saved. Re-run the same command to resume.", "WARN")
        finally:
            rrs.save_session(page)
            ctx.close()
    log(f"Crawl session finished: {n_files} file listings, {n_new} new unique files "
        f"in {(time.time() - t0) / 60:.1f} min. DB: {DB_PATH}")
    return 0


# ═══════════════════════════════════════════════════════════════════════════════════════
# STATUS / EXPORT
# ═══════════════════════════════════════════════════════════════════════════════════════

def cmd_status(args) -> int:
    if not DB_PATH.exists():
        print("No catalog yet — run: python bhoomi_rrs_catalog.py crawl")
        return 0
    con = open_db()
    rows = con.execute("""
        SELECT v.dist_code, COUNT(*),
               SUM(v.status='done'), SUM(v.status='empty'), SUM(v.status='error'),
               SUM(v.status='pending'), COALESCE(SUM(v.files), 0), d.tree_done
        FROM villages v LEFT JOIN districts d USING(dist_code)
        GROUP BY v.dist_code ORDER BY v.dist_code""").fetchall()
    print(f"{'district':24} {'villages':>8} {'w/files':>8} {'empty':>6} {'error':>6} "
          f"{'pending':>8} {'listings':>9}  tree")
    tot = [0] * 6
    for d, n, done, empty, err, pend, files, tree in rows:
        print(f"{d:>3} {DISTRICT_EN.get(d, ''):20} {n:>8} {done:>8} {empty:>6} {err:>6} "
              f"{pend:>8} {files:>9}  {'full' if tree else 'partial'}")
        for k, x in enumerate((n, done, empty, err, pend, files)):
            tot[k] += x or 0
    unique = con.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    known = con.execute("SELECT COUNT(*) FROM districts").fetchone()[0]
    print("-" * 86)
    print(f"{'TOTAL':24} {tot[0]:>8} {tot[1]:>8} {tot[2]:>6} {tot[3]:>6} {tot[4]:>8} {tot[5]:>9}")
    print(f"\nUnique files catalogued: {unique}   |   districts with villages enumerated: "
          f"{len(rows)}/{known or 31}")
    return 0


EXPORT_HEADER = ["district_code", "district", "district_en", "taluk_code", "taluk",
                 "hobli_code", "hobli", "village_code", "village", "file_id", "office_code",
                 "office", "file_no", "subject", "year", "type", "survey_hint", "survey_tags"]
EXCEL_MAX_ROWS = 1_048_575           # Excel's row limit, minus the header


def _write_csv(con, out: Path, codes: list) -> int:
    q = f"""
        SELECT v.dist_code, v.dist_name, v.taluk_code, v.taluk_name, v.hobli_code, v.hobli_name,
               v.village_code, v.village_name, f.file_id, f.office_code, f.office, f.file_no,
               f.subject, f.year, f.kind, vf.survey_hint, vf.tags
        FROM village_files vf
        JOIN villages v USING(dist_code, taluk_code, hobli_code, village_code)
        JOIN files f USING(file_id)
        WHERE v.dist_code IN ({','.join('?' * len(codes))})
        ORDER BY v.dist_code, v.taluk_code, v.hobli_code, v.village_code, f.year, f.file_id"""
    n = 0
    with open(out, "w", newline="", encoding="utf-8-sig") as fh:     # BOM: Excel shows Kannada
        w = csv.writer(fh)
        w.writerow(EXPORT_HEADER)
        for r in con.execute(q, codes):
            r = list(r)
            r.insert(2, DISTRICT_EN.get(r[0], ""))
            w.writerow(r)
            n += 1
    return n


def cmd_export(args) -> int:
    con = open_db()
    if args.district:
        codes = [int(x) if x.isdigit() else rrs.DISTRICT_CODES.get(x.lower()) for x in args.district]
        if None in codes:
            raise SystemExit("--district accepts codes or English names for export")
    else:
        codes = [r[0] for r in con.execute(
            "SELECT DISTINCT dist_code FROM village_files ORDER BY dist_code")]
    if not codes:
        raise SystemExit("Nothing to export yet — run crawl first.")
    stamp = f"{datetime.now():%Y%m%d_%H%M}"

    if args.split:
        folder = Path(args.out) if args.out else EXPORT_DIR / f"rrs_catalog_{stamp}"
        folder.mkdir(parents=True, exist_ok=True)
        total = 0
        for d in codes:
            name = re.sub(r"\W+", "_", DISTRICT_EN.get(d, str(d))).strip("_")
            n = _write_csv(con, folder / f"{d:02d}_{name}.csv", [d])
            total += n
            log(f"  {d:02d} {DISTRICT_EN.get(d, '')}: {n} rows"
                + ("  ⚠ over Excel's row limit" if n > EXCEL_MAX_ROWS else ""))
        log(f"Exported {total} rows in {len(codes)} file(s) → {folder}")
        return 0

    rows = con.execute(
        f"SELECT COUNT(*) FROM village_files WHERE dist_code IN ({','.join('?' * len(codes))})",
        codes).fetchone()[0]
    if rows > EXCEL_MAX_ROWS:
        log(f"{rows} rows exceeds Excel's limit of {EXCEL_MAX_ROWS + 1:,} — writing it anyway; "
            "use --split for one Excel-sized file per district", "WARN")
    out = Path(args.out) if args.out else EXPORT_DIR / f"rrs_catalog_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    n = _write_csv(con, out, codes)
    log(f"Exported {n} rows → {out}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="State-wide Bhu Suraksha record-room file catalog")
    sub = ap.add_subparsers(dest="cmd", required=True)

    cp = sub.add_parser("crawl", help="Enumerate villages and catalog their files (resumable)")
    cp.add_argument("--district", action="append",
                    help="Code or English name; repeatable. Default: all districts")
    cp.add_argument("--taluk", action="append", help="Taluk code(s); needs one --district")
    cp.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                    help=f"Seconds between village queries (default {DEFAULT_DELAY})")
    cp.add_argument("--retry-errors", action="store_true", help="Also retry villages that failed")
    cp.add_argument("--limit", type=int, default=0, help="Stop after N villages (testing)")
    cp.set_defaults(fn=cmd_crawl)

    sp = sub.add_parser("status", help="Show crawl progress per district")
    sp.set_defaults(fn=cmd_status)

    ep = sub.add_parser("export", help="Write the catalog to CSV (Excel-friendly)")
    ep.add_argument("--district", action="append", help="Limit to district code/name; repeatable")
    ep.add_argument("--out", help="Output CSV path (or folder with --split)")
    ep.add_argument("--split", action="store_true",
                    help="One CSV per district (keeps each file under Excel's row limit)")
    ep.set_defaults(fn=cmd_export)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
