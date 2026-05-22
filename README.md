# 🏛️ POWER-BHOOMI Playwright Edition

### Karnataka Land Records Bulk Extractor — v11.0 Skeleton-First Hybrid

![Version](https://img.shields.io/badge/version-11.0.0-blue)
![Python](https://img.shields.io/badge/python-3.10+-green)
![Workers](https://img.shields.io/badge/workers-24%20parallel-orange)
![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)
![License](https://img.shields.io/badge/license-Private-red)

---

## 📋 Overview

**POWER-BHOOMI** is a production-grade bulk extractor for the [Karnataka Bhoomi Land Records portal](https://landrecords.karnataka.gov.in/Service2/). Given a district / taluk / hobli / village selection and an owner-name filter, it walks the full Cartesian product of `Village × Survey × Surnoc × Hissa × Period (latest)` to extract every owner record present, with full CSV + SQLite output.

The application runs as a **Flask web app** at `http://localhost:5001` with a real-time dashboard showing per-worker progress, live logs, match highlights, and one-click downloads.

**Primary objective**: 100% data completeness for the searched area. Every survey, every owner (including joint owners), every hissa.

---

## ✨ Key Features (v11.0)

| Feature | Description |
|---|---|
| **24 Parallel Workers** | Concurrent headless Chromium instances; full Service2 enumeration |
| **Service154 + Service2 Hybrid** | Service154 HTTP API used as advisory skeleton + empty-dropdown rescue; Service2 is authoritative for owner data |
| **Full Survey Enumeration** | Iterates 1..max_survey for every village (no false-positive caps) |
| **No-Owner Refetch Retry** | Re-clicks Fetch up to 3 times with progressive backoff if the page returns blank |
| **Recovery Helper** | Dropdown-select retry with state recovery (handles 24-worker portal races) |
| **Stale-State Detection** | Distinguishes real selection failures from leftover dropdown options |
| **Smart Stop with Data Guard** | Stops after 50 consecutive empty surveys ONLY after data has been found |
| **Phase 2 Auto-Retry** | At end of Phase 1, automatically retries every skipped survey |
| **DB-Backed Event Logs** | Every log line persisted to SQLite with structured fields (level, worker, village) |
| **State Replay** | Close browser → reopen → see exact same state via `/api/session/<id>/logs?since_id=N` |
| **Singleton Coordinator** | One coordinator per process; survives between searches; HTTP 409 on duplicate start |
| **Auto-Mark Interrupted** | On server restart, stale `running` sessions are flipped to `interrupted` |
| **Auto-CSV on Stop** | Manual stop now exports `all_records.csv`, `matches.csv`, `skipped_surveys.csv` to ~/Downloads |
| **Owner Name Search** | Substring match on Kannada/English names; joint owners captured intact |
| **Browser Auto-Open** | Server start opens default browser to localhost:5001 automatically |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       Flask Web Server (:5001)                           │
│  ┌──────────────┐  ┌────────────────────────────┐  ┌─────────────────┐  │
│  │   Web UI      │  │     REST API (20+ routes)   │  │   Static Assets │  │
│  │  Dashboard    │◄─┤  /api/search/start|stop|... │  │                 │  │
│  │  + State      │  │  /api/session/<id>/logs     │  │                 │  │
│  │  Replay JS    │  │  /api/session/<id>/records  │  │                 │  │
│  └──────────────┘  └────────────────────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────────────────┤
│                  ParallelSearchCoordinator (singleton)                   │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                     SearchWorker × 24                               │ │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ … ┌──────┐                             │ │
│  │  │  W0  │ │  W1  │ │  W2  │   │ W23  │                             │ │
│  │  └──┬───┘ └──┬───┘ └──┬───┘   └──┬───┘                             │ │
│  │     ▼        ▼        ▼          ▼                                 │ │
│  │  Headless Chromium (Playwright) — 1 browser per worker             │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  Service154Client (pure HTTP)        — skeleton advisory only      │ │
│  │  PortalHealthManager                  — pings + state machine      │ │
│  │  StateManager (snapshots)             — JSON checkpoint every N s  │ │
│  │  Phase 2 Retry Worker                 — runs after Phase 1         │ │
│  └────────────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────────┤
│                          Persistence Layer                               │
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────────────────────┐ │
│  │  SQLite DB   │  │   CSV Writers  │  │  JSON State Snapshots        │ │
│  │ (WAL + Pool) │  │  (auto-flush)  │  │  state_search_<id>.json      │ │
│  │              │  │  + DB exports  │  │                              │ │
│  │ Tables:      │  │                │  │                              │ │
│  │ • search_…   │  │  ~/Downloads/  │  │  ~/Documents/POWER-BHOOMI/   │ │
│  │ • land_…     │  │  bhoomi_*.csv  │  │     state_snapshots/         │ │
│  │ • event_logs │  │                │  │                              │ │
│  │ • skipped_…  │  │                │  │                              │ │
│  │ • survey_…   │  │                │  │                              │ │
│  └──────────────┘  └────────────────┘  └─────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data flow per worker

```
For each village in worker's queue:
    For survey_no in 1..max_survey:
        Navigate to Service2 → Select dist/taluk/hobli/village → Fill survey_no → click GO
        Get surnoc dropdown options
        For each surnoc (with recovery helper retry):
            Get hissa dropdown options
            For each hissa (with recovery helper retry):
                Select latest period → click Fetch
                Extract owners with BeautifulSoup
                If no owners: re-click Fetch up to 3 times (progressive backoff)
                Save every owner row → DB + CSV writer
        Smart-stop check: 50 consecutive empty surveys AND surveys_with_data > 0 → exit village

After all 24 workers finish (Phase 1):
    Auto-export CSVs (live + DB snapshot)
    Phase 2: re-attempt every skipped (sy, sn, hi)
    Final CSVs + completion summary
```

---

## 📁 File Structure

| File | Description |
|---|---|
| `bhoomi_playwright_v11.py` | **Latest production version** |
| `bhoomi_playwright_v10.py` | v10 — 24-worker production edition |
| `bhoomi_playwright_v9.py` | v9 — Deadlock-free completion |
| `bhoomi_playwright_v8.py` | v8 — Phase 2 auto-retry |
| `bhoomi_playwright_v7.py` | v7 — Column mapping fix |
| `requirements_playwright.txt` | Python dependencies |
| `INSTALL_WINDOWS.md` | Windows-specific installation notes |
| `README.md` | This file |

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.10+** (3.9 also works)
- **16 GB RAM minimum** (24 Chromium instances ~8 GB)
- **macOS / Linux / Windows**

### Installation

```bash
# 1. Clone
git clone https://github.com/flipsideview/POWER-BHOOMI-PLAYWRIGHT.git
cd POWER-BHOOMI-PLAYWRIGHT

# 2. Virtual environment
python3 -m venv venv
source venv/bin/activate   # Windows: .\venv\Scripts\activate

# 3. Dependencies
pip install -r requirements_playwright.txt

# 4. Playwright browsers
playwright install chromium

# 5. Run
python3 bhoomi_playwright_v11.py
```

The default browser will open `http://localhost:5001` automatically after a 2-second delay.

---

## 🖥️ Dashboard UI

The web dashboard provides:

- **Search Form** — Cascading dropdowns for District → Taluk → Hobli → Village, owner-name search, max-survey input
- **Control Panel** — Start / Pause / Resume / Stop
- **Worker Grid** — One card per worker (auto-resized to `MAX_WORKERS`), showing current village, survey number, records found
- **Live Logs Panel** — Backfilled from DB on page load; incremental polling thereafter
- **Records / Matches Tabs** — Paginated view of all extracted records and owner-name matches
- **Confidence Panel** — Per-village quality score (HIGH ≥ 80%, MED 50-79, LOW < 50)
- **Phase 2 Stats Panel** — Recovery rate of retried skipped surveys
- **Download Center** — One-click CSV export for All Records, Matches, Skipped Surveys

### State Replay

Close the browser tab mid-search. Reopen `http://localhost:5001`. The UI calls `/api/session/current` on page-load, identifies the active or most-recent session, and **backfills up to 5,000 logs and 200 records** from the database before resuming live polling. You see exactly what was on screen before.

---

## 🔧 Configuration

All configuration is in the `Config` class at the top of `bhoomi_playwright_v11.py`.

### Worker Settings

| Setting | Default | Description |
|---|---|---|
| `MAX_WORKERS` | `24` | Number of parallel browser workers |
| `WORKER_STARTUP_DELAY` | `2.5` | Staggered startup delay (seconds) |

### Search & Accuracy

| Setting | Default | Description |
|---|---|---|
| `DEFAULT_MAX_SURVEY` | `200` | Default upper bound (user usually overrides to 600-800) |
| `SMART_STOP_ENABLED` | `True` | Stop after consecutive empties **once data has been found** |
| `EMPTY_SURVEY_THRESHOLD` | `50` | Consecutive empty surveys before smart-stop |
| `MIN_SURVEYS_BEFORE_STOP` | `10` | Minimum surveys checked before stop allowed |
| `LATEST_PERIOD_ONLY` | `True` | Extract only most recent period per hissa |
| `PERIOD_SELECTION_RETRIES` | `3` | Retries for period dropdown |
| `MAX_HISSA_RETRIES` | `2` | Retries for hissa dropdown |
| `NO_OWNER_REFETCH_RETRIES` | `3` | Re-click Fetch this many times if page returns blank |
| `NO_OWNER_REFETCH_WAIT` | `5` | Initial wait between re-fetches (s); progressive |

### Service154 Skeleton (Advisory Only — v11.0)

| Setting | Default | Description |
|---|---|---|
| `SKELETON_FIRST_ENABLED` | `True` | Pre-fetch S154 skeleton; used ONLY for empty-dropdown rescue |
| `SKELETON_PARALLEL_WORKERS` | `8` | Threads for parallel skeleton pre-fetch |
| `SKELETON_FETCH_TIMEOUT` | `30` | HTTP timeout per village |
| `SKELETON_MAX_RETRIES` | `3` | Per-village retry count |
| `SKELETON_CACHE_TTL_SECONDS` | `3600` | In-memory cache TTL |

> **Important**: Service154 is NOT trusted as authoritative. Empirical testing (May 2026) shows it can both over-report (historical entries) and under-report (e.g. PALYA misses surveys 53-78). It's used only to rescue surveys where Service2's dropdown returns empty.

### State Retention

| Setting | Default | Description |
|---|---|---|
| `LOG_RETENTION_COUNT` | `500` | Logs kept in memory (DB has all) |
| `RECORDS_BUFFER_COUNT` | `500` | Records buffered for UI snapshot |
| `MATCHES_BUFFER_COUNT` | `200` | Matches buffered for UI snapshot |

### Retry & Recovery

| Setting | Default | Description |
|---|---|---|
| `MAX_PORTAL_RETRIES` | `5` | Per-survey portal access retries |
| `MAX_SESSION_RETRIES` | `3` | Session-expiry retries |
| `CONSECUTIVE_ERROR_RESTART` | `5` | Browser restart after N consecutive errors |
| `RETRY_BACKOFF_BASE` | `3` | Exponential backoff base (seconds) |
| `MAX_HISSA_BEFORE_RESTART` | `200` | Restart browser after this many hissas (memory) |

### Phase 2 (Auto-Retry of Skipped Items)

| Setting | Default | Description |
|---|---|---|
| `PHASE2_ENABLED` | `True` | Run Phase 2 after Phase 1 |
| `PHASE2_MAX_ATTEMPTS` | `2` | Retries per skipped item |
| `PHASE2_DELAY_BETWEEN` | `3` | Seconds between Phase 2 calls |
| `PHASE2_COOLDOWN_BEFORE` | `15` | Wait after Phase 1 before Phase 2 |

---

## 📡 REST API

### Search Control

| Endpoint | Method | Description |
|---|---|---|
| `/api/search/start` | POST | Start a new search (409 if already running) |
| `/api/search/status` | GET | Live coordinator state (UI polls every 2s) |
| `/api/search/stop` | POST | Stop search (auto-exports CSVs to ~/Downloads) |
| `/api/search/pause` | POST | Pause all workers |
| `/api/search/resume` | POST | Resume paused workers |

### Session Replay (v11.0)

| Endpoint | Method | Description |
|---|---|---|
| `/api/session/current` | GET | Active or most-recent session metadata |
| `/api/session/<id>/logs?since_id=N&limit=500` | GET | Incremental log fetch from DB |
| `/api/session/<id>/records?offset=N&limit=200&matches_only=bool` | GET | Paginated records from DB |

### Location Data

| Endpoint | Method | Description |
|---|---|---|
| `/api/districts` | GET | List of districts |
| `/api/taluks/<district_code>` | GET | Taluks under district |
| `/api/hoblis/<district_code>/<taluk_code>` | GET | Hoblis under taluk |
| `/api/villages/<district_code>/<taluk_code>/<hobli_code>` | GET | Villages under hobli |

### File Downloads

| Endpoint | Method | Description |
|---|---|---|
| `/api/download/records` | GET | Live `all_records.csv` |
| `/api/download/matches` | GET | Live `matches.csv` |
| `/api/download/records_phase1` | GET | Phase-1 snapshot of all records |
| `/api/download/matches_phase1` | GET | Phase-1 snapshot of matches |
| `/api/files/info` | GET | File sizes + record counts |

### Database & History

| Endpoint | Method | Description |
|---|---|---|
| `/api/db/info` | GET | Database stats |
| `/api/db/sessions` | GET | List all search sessions |
| `/api/db/sessions/<id>` | GET | Session details |
| `/api/db/sessions/<id>/records` | GET | Records for a session |
| `/api/db/sessions/<id>/export` | GET | Export session to CSV |
| `/api/db/sessions/<id>/skipped/export` | GET | Export skipped surveys CSV |
| `/api/db/search?owner_name=…` | GET | Search records across all sessions |

---

## 💾 Data Storage

### SQLite Database

- **Location**: `~/Documents/POWER-BHOOMI/bhoomi_data.db`
- **Mode**: WAL (Write-Ahead Logging) with connection pool size `MAX_WORKERS + 8`
- **Tables**:
  - `search_sessions` — search metadata + status
  - `land_records` — every owner record (with `is_match` flag)
  - `event_logs` — every UI log line with `level/worker_id/village/kind`
  - `village_progress` — per-village completion tracking
  - `survey_checkpoints` — granular resume capability
  - `skipped_items` — skipped (sy, sn, hi) tuples for Phase 2 retry

### CSV Files

- **Live (during search)**:
  - `~/Downloads/bhoomi_all_records_<TS>.csv`
  - `~/Downloads/bhoomi_matches_<TS>.csv`
- **On stop** (auto-exported):
  - `~/Downloads/bhoomi_all_records_<TS>_stop.csv`
  - `~/Downloads/bhoomi_matches_<TS>_stop.csv`
  - `~/Downloads/bhoomi_skipped_surveys_<TS>_stop.csv`
- **Phase 1 snapshot** (DB-exported, immune to writer state):
  - `<all_records>_phase1.csv`
  - `<matches>_phase1.csv`

### State Snapshots

- **Location**: `~/Documents/POWER-BHOOMI/state_snapshots/`
- **Format**: JSON, written every ~30s during run
- **Use**: Crash recovery, post-mortem analysis

---

## 📊 Performance & Resource Usage

| Metric | Value |
|---|---|
| Workers | **24 parallel browsers** |
| RAM (steady state) | ~8 GB total (Python ≈ 100 MB + 24 × ~340 MB Chromium) |
| Throughput | 70-180 records/min depending on portal load |
| Per-village time | 5-25 min (depends on data density + smart-stop) |
| Typical 226-village run | ~12-36 hours for full Phase 1 |
| Records / data-bearing village | 400-2,300 (varies hugely by village) |

> A 32 GB Mac handles 24 workers with ~14 GB headroom for OS + IDE + browser. On 16 GB systems, set `MAX_WORKERS = 14-16`.

---

## 🔄 Version History

### v11.0 — Skeleton-First Hybrid + State Replay ⭐ LATEST
- **NEW**: `Service154Client` — pure-HTTP wrapper for Service154 skeleton API (advisory only)
- **NEW**: DB-backed `event_logs` table — UI state survives browser refresh and server restart
- **NEW**: `/api/session/<id>/logs?since_id=N` — incremental log fetch
- **NEW**: `/api/session/<id>/records?offset=N` — paginated records from DB
- **NEW**: Recovery helper for surnoc/hissa selection (handles 24-worker portal races)
- **NEW**: Stale-state detection — distinguishes real failures from leftover dropdown options
- **NEW**: Singleton coordinator — survives between searches
- **NEW**: Auto-mark interrupted sessions on server restart
- **NEW**: Auto-export CSVs on manual stop (DB snapshot + flush of live writers)
- **NEW**: Auto-open browser on server start
- **FIX**: `LATEST_PERIOD_ONLY` truly latest period only
- **FIX**: Smart-stop with `surveys_with_data > 0` guard (no premature stops)
- **FIX**: SSL bypass (`ignore_https_errors=True`) for Bhoomi cert lapses
- **REVERTED**: Hissa/Surnoc skeleton UNION (caused false-skip timeouts)
- **REMOVED**: Skeleton survey-set filter (Service154 not authoritative — empirical proof)

### v10.0 — Production Edition (24 Workers)
- Worker count: 12 → 24
- Tuned timeouts and rate limiter for higher concurrency
- DB connection pool scaled to 32
- CSV pre-creation on search start (no more 404s for empty matches)
- Phase 1 CSV auto-snapshot from DB

### v9.0 — Deadlock-Free Completion
- Fixed nested `state_lock` deadlock in `_monitor_completion`
- Phase 2 + finalization moved outside the lock
- Auto-flush CSV writers when Phase 1 completes
- Bypass `get_state()` lock contention in download endpoint

### v8.0 — Enterprise + Phase 2 Auto-Retry
- Automatic Phase 2 retry of skipped surveys
- Per-record dedup via DB UNIQUE constraint

### v7.0 — Column Mapping Fix
- Owner column uses first-match (prevents "Owner Category" overwriting "Owner")
- Removed Khata column (portal no longer provides)

### v6.x — Network Recovery + Silent Failure Detection
- Network errors trigger 5-min wait-and-retry instead of immediate skip
- Empty owner extraction logged as `⚠️ NO OWNERS` and tracked

### v4.0–v5.0 — Original 12-Worker Edition
- Initial Playwright + Flask architecture

---

## 🛡️ Error Handling

The application has **layered error protection**:

1. **Dropdown selection retry** (v11.0) — Recovery helper with 3-stage retry: re-verify, re-issue GO, full re-navigation
2. **No-owner refetch** — Re-click Fetch with progressive backoff (5s → 10s → 15s)
3. **Portal access retry** — Up to 5 retries with exponential backoff
4. **Session recovery** — Detects expiry and re-navigates automatically
5. **Network recovery** — Up to 5 minutes of wait-and-retry on `net::err_*`
6. **Browser restart** — After consecutive errors or every 200 hissas (memory)
7. **Phase 2 retry** — Final pass over every skipped item

**Every survey that cannot be processed is logged** with:
- Village name, code, hobli
- Survey number, surnoc, hissa, period
- Error reason, worker_id, timestamp

Skipped items live in BOTH in-memory state and `skipped_items` SQLite table.

---

## ⚙️ How It Works

1. User picks **District → Taluk → Hobli → Village**, enters **owner name**, sets **max_survey**
2. Coordinator fetches village list via `BhoomiAPI` (one-time HTTP)
3. Coordinator pre-fetches **Service154 skeletons** for all villages (8 parallel HTTP)
4. Villages distributed across **24 workers** round-robin
5. Each worker:
   - Launches headless Chromium with SSL bypass
   - Navigates Service2 portal → fills location dropdowns
   - For each survey 1..max_survey:
     - Click GO → read surnoc dropdown
     - For each surnoc: select → read hissa dropdown
     - For each hissa: select → read period → fetch
     - Extract owners with BeautifulSoup
     - Match against owner-name search variants (substring, case-insensitive)
     - Save to DB + CSV in real-time
6. **Smart-stop** kicks in after 50 consecutive empties (only after data found)
7. **Phase 2** runs after all workers complete: retries every skipped item
8. **Final CSV** export (DB snapshot + live writer flush)

---

## 🔬 Honest Limitations

- **Service154 is approximate, not authoritative** — sometimes over-reports historical entries, occasionally under-reports real surveys (e.g. PALYA misses surveys 53-78). Hence v11 uses Service154 only as an empty-dropdown rescue, never as a survey filter.
- **Joint owners may appear as combined strings** — `"Owner1, Owner2 (ಜಂಟಿ)"` — owner-name substring match still works correctly.
- **Latest period only** — historical period chains are NOT captured; only the most recent RTC period for each `(survey, surnoc, hissa)`.
- **Smart-stop is empirical** — 50 consecutive empties is a tunable threshold; if your village has data clusters separated by gaps > 50, lower `EMPTY_SURVEY_THRESHOLD`.
- **Portal SSL certs occasionally expire** — application bypasses with `ignore_https_errors`. Check certificate validity if you see SSL warnings persistently.

---

## 🐛 Troubleshooting

| Issue | Solution |
|---|---|
| Port 5001 in use | `lsof -ti:5001 \| xargs kill -9` |
| 409 "Search already running" | Wait for current search OR stop it via UI |
| State lock timeout warnings | Self-recovers; long Phase 2 may briefly contend |
| High memory | Lower `MAX_WORKERS` to 14-16 (for 16 GB RAM) |
| "Skeleton fetch failed" warnings | Service154 may be temporarily down — search continues with full enumeration |
| CSVs not appearing | Check `~/Downloads/` (auto-export) and `~/Documents/POWER-BHOOMI/csv_archive/` |
| Owner names appearing as `?` characters | Re-check page rendering — possible OS font issue |

---

## 📜 License

Private — POWER-BHOOMI Team

---

*Built with Python 3.10+, Playwright, Flask, SQLite (WAL mode), BeautifulSoup, and the requests library.*
