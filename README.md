# 🏛️ POWER-BHOOMI Playwright Edition

### Karnataka Land Records Bulk Extractor — v12.2 Completion-Hardened

![Version](https://img.shields.io/badge/version-12.2.0-blue)
![Python](https://img.shields.io/badge/python-3.10+-green)
![Workers](https://img.shields.io/badge/workers-24%20parallel-orange)
![Tests](https://img.shields.io/badge/tests-51%20passing-brightgreen)
![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)
![License](https://img.shields.io/badge/license-Private-red)

---

## 📋 Overview

**POWER-BHOOMI** is a production-grade bulk extractor for the [Karnataka Bhoomi Land Records portal](https://landrecords.karnataka.gov.in/Service2/). Given a district / taluk / hobli / village selection and an owner-name filter, it walks the full Cartesian product of `Village × Survey × Surnoc × Hissa × Period (latest)` to extract every owner record present, with full CSV + SQLite output.

The application runs as a **Flask web app** at `http://127.0.0.1:5001` with a real-time dashboard showing per-worker progress, live logs, match highlights, and one-click downloads.

**Primary objective**: 100% data completeness for the searched area. Every survey, every owner (including joint owners and multi-extent holdings), every hissa.

> **v12 exists because v11 could not finish.** Every long v11 run ended wedged at ~97% with Phase 2 never running — a worker thread stuck forever inside a Playwright sync call blocked completion for all. v12 adds a stall watchdog, a coverage pass, a graceful stop, and (v12.2) fixes two silent data-loss bugs found by a full semantic audit. See `SEMANTIC_AUDIT.md` for the audit record.

---

## ✨ What's new by generation

| Generation | Fix / feature |
|---|---|
| **v12.0** | **Stall watchdog** — a wedged worker is reaped after 15 min of heartbeat silence; its unfinished villages go to a **coverage pass** instead of hanging the search forever |
| **v12.0** | **Graceful stop** — drains workers (45s window), force-fails stragglers, exports all CSVs + full DB skipped list, resets the coordinator so a new search starts without a server restart |
| **v12.0** | **DB-backed Phase 2** — retries the deduplicated `skipped_items` table (survives restarts; no more 20-row truncation) |
| **v12.0** | **Skeleton-verified smart-stop** — before abandoning a village after 50 empty surveys, checks Service154 for surveys beyond the gap and jumps to them |
| **v12.0** | **Hobli-qualified checkpoints** — village codes repeat across hoblis; the old key made villages inherit each other's checkpoints and silently skip their own low surveys (44/117 villages affected in one real run) |
| **v12.0** | **Robust village enumeration** — poll-until-populated + retry + echawadi-HTTP fallback + count cross-check (v11 silently dropped an entire 66-village hobli) |
| **v12.1** | **Heartbeats in portal primitives** + watchdog **resurrection** (a slow-but-alive worker is never falsely reaped; a falsely reaped one is restored) |
| **v12.1** | UI surfaces start rejections (409) instead of silently showing the existing run |
| **v12.2** | **Data-loss fix (C1)**: DB identity now includes `extent` + `owner_seq` — an owner's multiple extents in one parcel are all kept (735 rows were being silently dropped per ~80k-row run). Startup migration rebuilds existing DBs losslessly |
| **v12.2** | **Data-loss fix (C2)**: identical co-owner rows (same name, same extent) on one page are kept as distinct records |
| **v12.2** | `--repair-import` CLI restores rows the old key dropped, from the live CSV |
| **v12.2** | **Script-safe owner matching** — whitespace-insensitive; warns loudly when the query is English (portal names are Kannada — an English query captures everything but flags 0 matches) |
| **v12.2** | Rate limiter verdict honored at every call site (bounded 120s starvation cap) |
| **v12.2** | Server binds **127.0.0.1** with debug off (set `BHOOMI_HOST=0.0.0.0` to expose deliberately) |
| **v12.2** | **Session resurrection** — reopening the browser mid-search reattaches the live controls (Stop button, polling, panels), not just the data |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                  Flask Web Server (127.0.0.1:5001)                       │
│  Web UI dashboard  ·  REST API (~30 routes)  ·  session state replay    │
├─────────────────────────────────────────────────────────────────────────┤
│               ParallelSearchCoordinator (singleton)                      │
│   SearchWorker × 24 (headless Chromium each)                             │
│   Stall watchdog (heartbeat) → reap / resurrect                          │
│   Phase 1 → Coverage pass (worker 98) → Phase 2 retry (worker 99)        │
│   Service154Client (HTTP skeletons) · PortalHealthManager · RateLimiter  │
├─────────────────────────────────────────────────────────────────────────┤
│                        Persistence                                        │
│   SQLite WAL + pool  ·  buffered CSV writers  ·  JSON state snapshots    │
│   identity: (session, village, survey, surnoc, hissa, period,            │
│              owner_name, extent, owner_seq)                              │
└─────────────────────────────────────────────────────────────────────────┘
```

### Run lifecycle

```
Start → enumerate villages (poll+retry+HTTP cross-check)
      → prefetch Service154 skeletons (advisory only)
      → Phase 1: 24 workers, full survey enumeration per village
          watchdog reaps wedged workers; their villages queue for coverage
      → Phase 1 CSV snapshot (downloadable immediately)
      → Coverage pass: re-enumerates villages abandoned by reaped workers
      → Phase 2: retries every skipped (survey, surnoc, hissa) from the DB
      → Coverage audit + final CSV exports + completion summary
```

---

## 🚀 Quick Start

```bash
git clone git@github.com:flipsideview/POWER-BHOOMI-PLAYWRIGHT.git
cd POWER-BHOOMI-PLAYWRIGHT
python3 -m venv venv && source venv/bin/activate
pip install -r requirements_playwright.txt
playwright install chromium
python3 bhoomi_playwright_v12.py
```

Open **http://127.0.0.1:5001** (auto-opens). The server binds localhost only; run with `BHOOMI_HOST=0.0.0.0` if you explicitly want LAN access.

> **Owner-name tip:** the portal returns names in **Kannada**. Enter the search name in Kannada (e.g. `ಕುಮಾರಸ್ವಾಮಿ`, not `KUMARASWAMY`) or the match counter will stay at 0 — all records are still captured either way, and the app warns you at start.

### Repairing a pre-v12.2 database

Databases written before v12.2 silently dropped same-owner multi-extent rows. The live CSV kept them. After upgrading (the schema migrates automatically at startup):

```bash
python3 bhoomi_playwright_v12.py --repair-import ~/Downloads/bhoomi_all_records_<TS>.csv <session_id>
```

---

## 🔧 Key Configuration (`Config` class, top of file)

| Setting | Default | Description |
|---|---|---|
| `MAX_WORKERS` | 24 | Parallel browser workers (lower to 8–12 if the portal throttles you) |
| `DEFAULT_MAX_SURVEY` | 200 | Survey ceiling (typically overridden to 600–800 in the UI) |
| `SMART_STOP_ENABLED` / `EMPTY_SURVEY_THRESHOLD` | True / 50 | Early village exit after consecutive empties (only after data found) |
| `SMART_STOP_SKELETON_VERIFY` | True | Consult Service154 before stopping; jump to known surveys past the gap |
| `STALL_TIMEOUT_SECONDS` | 900 | Heartbeat silence before the watchdog reaps a worker |
| `GRACEFUL_STOP_TIMEOUT` | 45 | Seconds stop waits for workers to finish their current survey |
| `PHASE2_ENABLED` / `PHASE2_FROM_DB` | True / True | Auto-retry skipped surveys from the deduplicated DB table |
| `SKELETON_FIRST_ENABLED` | True | Service154 pre-fetch (advisory / empty-dropdown rescue only — S154 both over- and under-reports; never used as a filter) |
| `HOST` / `DEBUG` | `127.0.0.1` / False | Bind address (env `BHOOMI_HOST` overrides) and Flask debug |

---

## 📡 REST API (selected)

| Endpoint | Description |
|---|---|
| `POST /api/search/start` · `POST /api/search/stop` | Start (409 if one is running — now surfaced in the UI) / graceful stop |
| `GET /api/search/status` | Live coordinator state (UI polls every 1.5s) |
| `GET /api/session/current` · `/api/session/<id>/logs` · `/api/session/<id>/records` | State replay: reopening the browser reattaches to the running search |
| `GET /api/download/records|matches|records_phase1|matches_phase1` | CSV downloads |
| `GET /api/skipped/current/export` | Full skipped-survey CSV **from the DB** |
| `GET /api/db/sessions…` | Session history, per-session export, cross-session owner search |

---

## 💾 Data Storage

- **SQLite**: `~/Documents/POWER-BHOOMI/bhoomi_data.db` (WAL, connection pool). Tables: `search_sessions`, `land_records`, `event_logs`, `village_progress`, `survey_checkpoints`, `skipped_items`.
- **Record identity** (v12.2): `(session, village, survey_no, surnoc, hissa, period, owner_name, extent, owner_seq)` — `owner_seq` is the row position in the parcel's result table, so identical co-owners are distinct records while re-scrapes still dedup.
- **CSVs**: live `~/Downloads/bhoomi_all_records_<TS>.csv` + matches; `_stop` exports on manual stop; `_phase1` snapshots at Phase 1 completion. CSV now carries an `owner_seq` column.

---

## 🔬 Honest limitations (kept current — see `SEMANTIC_AUDIT.md`)

- **Pause / Resume buttons are cosmetic.** They flip a flag no worker reads. Only Stop actually halts workers. (Audit finding C3 — a real pause gate is designed but not yet implemented.)
- **"Worker throttling" during rate-limiting is a TODO** — the log line exists, the mechanism doesn't (audit C4).
- **The portal throttles sustained 24-worker load** from one IP: throughput can collapse to near-zero for stretches (observed live). Skipped surveys are recorded and recovered by Phase 2; lowering `MAX_WORKERS` helps.
- **English owner queries match nothing** (Kannada data) — capture is unaffected; the app warns at start.
- **Latest period only** — historical RTC period chains are not captured.
- **Service154 is advisory, never authoritative** — it both over-reports (historical entries) and under-reports (truncated survey lists; proven empirically).
- `village_progress` is write-only bookkeeping; crash-resume relies on `survey_checkpoints` (hobli-qualified) instead.

---

## 🧪 Testing

51 executable tests across three suites (extraction fixtures, DB identity/migration, watchdog/stop mechanics, rate gate, repair import) plus:
- a **dress rehearsal** of the v12.2 migration + repair on a snapshot of a real 1.46M-row production DB (zero loss; exact CSV↔DB parity), and
- a **real-browser resurrection test** (fresh context = crashed-and-reopened Chrome; 12/12 checks).

---

## 🔄 Version History

- **v12.2** — Semantic-audit fixes: extent+owner_seq identity (+migration & `--repair-import`), co-owner preservation, script-safe matching, honored rate limiter, localhost binding, session resurrection. `SEMANTIC_AUDIT.md` added.
- **v12.1** — Heartbeats in portal primitives; watchdog resurrection of falsely-reaped workers; UI 409 surfacing.
- **v12.0** — Completion-hardened: stall watchdog, coverage pass, graceful stop, DB-backed Phase 2, skeleton-verified smart-stop, hobli-qualified checkpoints, robust village enumeration, browser-leak fixes.
- **v11.0** — Skeleton-First hybrid (Service154 + Service2), DB-backed event log, state replay, 24 workers. *(Known fatal flaw, fixed in v12: searches never completed — a wedged worker blocked Phase 1 finalization indefinitely.)*
- **v10.0** — 24-worker production edition. **v9.0** — deadlock-free completion. **v8.0** — Phase 2 auto-retry. **v7.x** — column-mapping fixes. **v6.x** — network recovery. **v4–5** — original 12-worker edition.

---

## 📜 License

Private — POWER-BHOOMI Team

*Built with Python 3.10+, Playwright, Flask, SQLite (WAL), BeautifulSoup.*
