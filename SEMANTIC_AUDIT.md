# SEMANTIC AUDIT — bhoomi_playwright_v12.py (10,032 lines)
Date: 2026-09-22 · Method: full read of every class/function; executed tests against real code paths on a throwaway DB; live-data quantification where possible. Every finding carries file:line and a status: **TESTED** (executed, result shown) or **READ** (verified by reading, not executable offline).

## Coverage statement (honest)
- Python code: 100% read, all 17 subsystems below.
- UI JavaScript: control wiring 100% traced (11 controls, 27 routes); JS runtime behavior not executed.
- NOT covered: behavior against the live portal beyond what this week's runs already demonstrated; correctness of the portal's own data.

---

## CRITICAL — data loss / core-objective violations

### C1. DB silently drops same-owner multi-extent rows — 735 rows lost in current run  **TESTED**
`land_records` UNIQUE key is `(session_id, village, survey_no, surnoc, hissa, period, owner_name)` — **extent is omitted** (schema, ~line 1895). An owner holding multiple extents in one parcel (completely legitimate, e.g. DUDDA Sy:1 ರಂಗಸ್ವಾಮಿ: 0.21.0 + 0.4.0 + 0.7.0) has all but one row dropped by `INSERT OR IGNORE`.
Measured against the live CSV (which keeps all rows): **80,222 CSV rows → 79,336 DB keys → 735 rows dropped across 631 parcels** in the KUMARASWAMY run alone. Every DB-sourced export (`*_stop.csv`, phase1 snapshots, session exports) is missing them. The live writer CSV is currently the only complete record.
**Fix:** add `extent` to the UNIQUE key (schema migration) — one line + migration.

### C2. Page-level owner dedup drops identical (name, extent) co-owners  **TESTED**
`_extract_owners` line ~3390: `if owner_entry not in owners` — two co-owners with the same name AND same extent on one page collapse to one before anything is saved. Test T1d proved it. Frequency unknown (same-name relatives with equal shares); unrecoverable post-hoc since it happens at extraction.
**Fix:** dedup on full table-row position or keep duplicates (DB can dedup with extent in key).

### C3. Pause / Resume / portal-pause / auto-resume are cosmetic  **TESTED (T10 regression-locked)**
`is_paused` (StateManager ~1332) is never read by any worker. Buttons, "PORTAL DOWN — Pausing all workers" (~6047), and auto-resume (~6075) all flip a flag nothing consumes. Workers only honor `state.running` (8 checkpoints). Present in every version since the feature appeared.

### C4. "RATE LIMITED — Throttling workers" is a TODO  **READ**
Line ~6060: log line claims throttling; code beneath is `# TODO: Implement worker throttling`. Config knobs `MIN_WORKERS_DURING_THROTTLE`, `THROTTLE_SCALE_DOWN_DELAY`, `THROTTLE_SCALE_UP_DELAY`: **0 uses**. During this week's real rate-limiting the app logged "Throttling…" while doing nothing.

---

## HIGH

### H1. Rate limiter is advisory — timeout ignored at all 8 call sites  **TESTED (T6c)**
`_global_rate_limiter.acquire()` returns False on timeout; every call site discards the return and proceeds. Under contention the "limiter" delays but never actually limits.

### H2. Flask debug server exposed to the network  **READ**
`app.run(host='0.0.0.0', debug=True)` (line ~10030). Werkzeug interactive debugger (PIN-gated RCE surface) reachable from the LAN, plus every API including `/api/search/stop`. Anyone on your network can stop your 3-day run with one request.
**Fix:** `host='127.0.0.1'` or `DEBUG=False`.

### H3. `_handle_alert` false-positives on generic strings  **READ**
Portal-issue detection includes `'please try again'` (~2960) — a phrase that can legitimately appear in page boilerplate; any hit marks the fetch as portal-failure and burns a 5-retry ladder. Not testable offline; flagged as risk.

### H4. Village-level failure not tracked on non-browser errors  **READ**
`run()` ~5045: a non-browser exception logs "Non-critical error, continuing" and `idx += 1` — the village is skipped **without** being added to `villages_failed` or `skipped_items`. Only the coverage audit's processed-vs-total count reveals the gap.

---

## MEDIUM

- **M1. `village_progress` table is write-only** — registered at start, never updated (start_village/complete_village/fail_village have zero worker callers). Any "resume" logic reading it sees a fresh session forever. TESTED against real DB: all 73 rows of the 9-day v11 run still 'pending'.
- **M2. `skipped_items` has no UNIQUE** — duplicates accumulate (2,444 rows, ~2,010 distinct in v11 run). v12's Phase 2 dedups at read time; table still bloats. TESTED (T4).
- **M3. `BhoomiAPI._cache` unbounded and never expires** (~2655) — stale village lists survive for process lifetime; memory grows per query key. READ.
- **M4. `event_logs` unbounded** — 760k+ rows across sessions, no pruning. READ.
- **M5. `get_state()` under lock contention returns a minimal stub** with fake zeros and a warning log row (~6920) — the UI briefly renders zeroed stats as if real. READ.
- **M6. `StateManager.save_checkpoint`/`load_snapshot`/`get_recovery_info` have no callers** — the advertised "crash recovery" reads nothing back on startup. Recovery = the log line "State Manager initialized - crash recovery enabled". READ.
- **M7. Stop leaves a worker's last survey partially recorded** — checkpoint written only after a fully-processed survey; graceful stop mid-survey re-does that survey on resume (safe direction, but the resume claim is only checkpoint-deep). READ.

## LOW / HYGIENE

- **L1. ~250 lines of dead Selenium-era code**: `WaitStrategy`, `SmartNavigator`, `CachedChromeDriver` — zero references each (TESTED by grep), including a latent `self.page` AttributeError in `SmartNavigator.is_on_portal` (TESTED T8).
- **L2. `requirements.txt` still lists selenium/webdriver-manager** (superseded by requirements_playwright.txt).
- **L3. `config.yaml` is vestigial** — nothing reads it; all config lives in the `Config` class.
- **L4. `owner_variants` double-JSON-encoded in DB** (cosmetic).
- **L5. UI `v11*`-prefixed JS identifiers** in a v12 app (cosmetic, functional).
- **L6. `ENABLE_RETRY_QUEUE=False` in-village retry path** (~4900): half-implemented "RETRY PARTIAL SUCCESS" that doesn't extract data even when it works — currently disabled, dead weight.

---

## What is VERIFIED WORKING (executed, not assumed)
- Owner extraction on well-formed pages (T1a/b/c/e) · exact-dup DB dedup (T2a) · event-log replay backbone (T3) · CSV writer flush (T7) · export function (T5, correct given DB contents) · confidence scoring sanity (T9) · rate-limiter token mechanics (T6a/b) · Start/Stop/downloads/tabs/dropdowns (live-tested earlier this week) · watchdog + graceful stop + hobli-qualified checkpoints (live-tested; false-reap fixed in v12.1 with heartbeat-in-primitives + resurrection).

## Bottom line
The **capture pipeline works** — but for the stated objective ("every survey, every hissa, every name") there are exactly two bugs that lose names: **C1 (735 rows, fixable by schema + re-export from CSV)** and **C2 (page-level co-owner collapse)**. Everything else broken is control/telemetry theater: pause/resume/throttling/recovery claims with no implementation behind them.
