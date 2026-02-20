# 🏛️ POWER-BHOOMI Playwright Edition

### Karnataka Land Records Search Tool — Production Grade, 100% Accuracy

![Version](https://img.shields.io/badge/version-7.0.0-blue)
![Python](https://img.shields.io/badge/python-3.10+-green)
![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Windows-lightgrey)
![License](https://img.shields.io/badge/license-Private-red)

---

## 📋 Overview

**POWER-BHOOMI** is a high-performance, production-grade tool for extracting land ownership records from the [Karnataka Bhoomi Portal](https://landrecords.karnataka.gov.in/Service2/). It automates the process of searching through districts, taluks, hoblis, and villages to extract owner names, extents, and survey details — at scale, with 12 parallel browser workers.

The tool runs as a **Flask web application** with a modern UI dashboard, providing real-time progress, pause/resume controls, download capabilities, and comprehensive error tracking.

---

## ✨ Key Features

| Feature | Description |
|---|---|
| **12 Parallel Workers** | Concurrent browser instances for maximum throughput |
| **Portal Health Monitoring** | Dedicated health worker pings portal every 10 seconds |
| **Smart Stop** | Intelligent early termination after 50 consecutive empty surveys |
| **Network Recovery** | Waits up to 5 minutes for network restoration instead of skipping |
| **100% Accuracy Mode** | All skipped surveys tracked, logged, and exportable |
| **Period Selection** | Extracts latest period only (configurable) with 3x retry |
| **Session Persistence** | SQLite + CSV dual persistence — survives crashes |
| **Pause / Resume** | Pause search and resume from exact checkpoint |
| **Real-time Dashboard** | Live progress, logs, matches, and worker status |
| **Owner Name Matching** | Highlights matched owner names in Kannada/English |
| **Export** | Download all records, matches, or skipped surveys as CSV |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Flask Web Server (:5001)                     │
│  ┌──────────┐  ┌──────────────────────────────┐  ┌───────────┐ │
│  │  Web UI   │  │     REST API (15+ routes)    │  │  Static   │ │
│  │ Dashboard │◄─┤  /api/search/start|stop|...  │  │  Assets   │ │
│  └──────────┘  └──────────────────────────────┘  └───────────┘ │
├─────────────────────────────────────────────────────────────────┤
│                    Search Engine (Core)                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              BhoomiSearchEngine                             │ │
│  │  ┌──────┐ ┌──────┐ ┌──────┐     ┌──────┐ ┌────────────┐  │ │
│  │  │ W0   │ │ W1   │ │ W2   │ ... │ W11  │ │  Health    │  │ │
│  │  │Worker│ │Worker│ │Worker│     │Worker│ │  Manager   │  │ │
│  │  └──┬───┘ └──┬───┘ └──┬───┘     └──┬───┘ └────────────┘  │ │
│  │     │        │        │             │                      │ │
│  │  ┌──▼────────▼────────▼─────────────▼───────────────────┐  │ │
│  │  │         Playwright Chromium Browsers                  │  │ │
│  │  │   (12 headless instances, isolated user-data-dirs)    │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│                     Persistence Layer                            │
│  ┌──────────────┐  ┌───────────────┐  ┌──────────────────────┐ │
│  │  SQLite DB    │  │  CSV Files    │  │  JSON State          │ │
│  │  (WAL mode)   │  │  (backup)     │  │  (checkpoints)       │ │
│  └──────────────┘  └───────────────┘  └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 File Structure

| File | Description |
|---|---|
| `bhoomi_playwright_v7.py` | **Latest (recommended)** — v7.0 with column mapping fix |
| `bhoomi_playwright_v6.py` | v6.2 — Network recovery + accuracy fixes |
| `bhoomi_playwright_v5.py` | v5.0 — Latest period only edition |
| `bhoomi_playwright_v4.py` | v4.0 — Enterprise edition (base) |
| `Bhoomi_playwright_windows.py` | v6.2 Windows — Windows-compatible with `taskkill`/`tasklist` |
| `requirements_playwright.txt` | Python dependencies |
| `INSTALL_WINDOWS.md` | Windows installation guide |
| `config.yaml` | Optional external configuration |
| `hoskote_villages.json` | Pre-loaded village data for Hoskote hobli |

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.10+**
- **Chrome/Chromium** (installed automatically by Playwright)
- **4GB+ RAM** recommended (12 browser instances)

### Installation (macOS / Linux)

```bash
# 1. Clone the repository
git clone https://github.com/flipsideview/POWER-BHOOMI-PLAYWRIGHT.git
cd POWER-BHOOMI-PLAYWRIGHT

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements_playwright.txt

# 4. Install Playwright browsers
playwright install chromium

# 5. Run the application
python bhoomi_playwright_v7.py
```

### Installation (Windows)

See [INSTALL_WINDOWS.md](INSTALL_WINDOWS.md) for detailed Windows instructions, or:

```powershell
# 1. Clone and navigate
git clone https://github.com/flipsideview/POWER-BHOOMI-PLAYWRIGHT.git
cd POWER-BHOOMI-PLAYWRIGHT

# 2. Create virtual environment
python -m venv venv
.\venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements_playwright.txt

# 4. Install Playwright browsers
playwright install chromium

# 5. Run the Windows version
python Bhoomi_playwright_windows.py
```

### Access the Dashboard

Open your browser and navigate to:

```
http://localhost:5001
```

---

## 🖥️ Dashboard UI

The web dashboard provides:

- **Search Form** — Select District → Taluk → Hobli (cascading dropdowns), enter owner name, set survey range
- **Control Panel** — Start / Pause / Resume / Stop buttons
- **Live Progress** — Real-time progress bars, record counts, worker status
- **Activity Logs** — Scrollable live logs with ✓ success / ⚠️ warning / ❌ error indicators
- **Matches Tab** — Highlighted owner name matches with village, survey, hissa, extent
- **Records Tab** — All extracted records across all villages
- **Download Center** — Export all records, matches, or skipped surveys as CSV

---

## 🔧 Configuration

All configuration is in the `Config` class at the top of the main file:

### Worker Settings

| Setting | Default | Description |
|---|---|---|
| `MAX_WORKERS` | `12` | Number of parallel browser workers |
| `WORKER_STARTUP_DELAY` | `2.0` | Staggered startup delay (seconds) |

### Search & Accuracy

| Setting | Default | Description |
|---|---|---|
| `SMART_STOP_ENABLED` | `True` | Enable intelligent early stopping |
| `EMPTY_SURVEY_THRESHOLD` | `50` | Stop after N consecutive empty surveys |
| `LATEST_PERIOD_ONLY` | `True` | Only extract latest/first period |
| `PERIOD_SELECTION_RETRIES` | `3` | Retry period selection before failing |
| `ACCURACY_MODE` | `True` | Enable all accuracy tracking features |

### Retry & Recovery

| Setting | Default | Description |
|---|---|---|
| `MAX_PORTAL_RETRIES` | `5` | Retry portal access before skipping |
| `MAX_SESSION_RETRIES` | `3` | Retry on session expiry |
| `CONSECUTIVE_ERROR_RESTART` | `5` | Restart browser after N consecutive errors |
| Network wait | `5 min` | Wait up to 5 minutes for network recovery |

### Portal Health

| Setting | Default | Description |
|---|---|---|
| `ENABLE_HEALTH_MANAGER` | `True` | Dedicated health monitoring worker |
| `HEALTH_CHECK_INTERVAL` | `10s` | Ping portal every 10 seconds |
| `AUTO_RESUME_ON_RECOVERY` | `True` | Auto-resume when portal recovers |

---

## 📡 REST API

The application exposes a full REST API:

### Search Control

| Endpoint | Method | Description |
|---|---|---|
| `/api/search/start` | `POST` | Start a new search |
| `/api/search/status` | `GET` | Get current search status (polled by UI) |
| `/api/search/stop` | `POST` | Stop the current search |
| `/api/search/pause` | `POST` | Pause all workers |
| `/api/search/resume` | `POST` | Resume paused workers |

### Location Data

| Endpoint | Method | Description |
|---|---|---|
| `/api/districts` | `GET` | List all districts |
| `/api/taluks/<district>` | `GET` | Taluks for a district |
| `/api/hoblis/<district>/<taluk>` | `GET` | Hoblis for a taluk |
| `/api/villages/<district>/<taluk>/<hobli>` | `GET` | Villages for a hobli |

### Data Export

| Endpoint | Method | Description |
|---|---|---|
| `/api/download/all` | `GET` | Download all records CSV |
| `/api/download/matches` | `GET` | Download matches CSV |
| `/api/download/skipped` | `GET` | Download skipped surveys CSV |
| `/api/files/info` | `GET` | File sizes and record counts |

### Database & Sessions

| Endpoint | Method | Description |
|---|---|---|
| `/api/db/info` | `GET` | Database stats |
| `/api/db/sessions` | `GET` | List all search sessions |
| `/api/db/sessions/<id>` | `GET` | Session details |
| `/api/db/sessions/<id>/records` | `GET` | Records for a session |
| `/api/db/sessions/<id>/export` | `GET` | Export session to CSV |
| `/api/db/sessions/<id>/skipped` | `GET` | Skipped surveys for session |
| `/api/db/sessions/<id>/skipped/export` | `GET` | Export skipped surveys CSV |
| `/api/db/search` | `GET` | Search records by owner name |
| `/api/db/resumable` | `GET` | List resumable sessions |

### Live Skipped Data

| Endpoint | Method | Description |
|---|---|---|
| `/api/skipped/current` | `GET` | Current session skipped surveys |
| `/api/skipped/current/export` | `GET` | Export current skipped surveys |
| `/api/portal/health` | `GET` | Portal health status |

---

## 🔄 Version History

### v7.0 — Column Mapping Fix ⭐ LATEST
- **FIX**: Owner column now uses **FIRST MATCH** — prevents "Owner Category" from overwriting "Owner" at index 0
- **FIX**: Removed **Khata** column entirely (portal no longer provides it)
- **FIX**: Extent column also uses first-match logic for robust header detection
- All v6.x fixes retained

### v6.2 — Network Recovery
- **FIX**: Network errors (`net::err_internet_disconnected`) now trigger a **wait-and-retry** loop (up to 5 minutes) instead of immediately skipping
- Surveys are only skipped if network does not recover after maximum wait time

### v6.1 — Silent Failure Detection
- **FIX**: Success log (`✓`) now only appears after owner extraction, not just period selection
- **FIX**: Empty owner extraction logged as `⚠️ NO OWNERS` and tracked as skipped

### v6.0 — Production Accuracy Release
- **FIX**: All portal RTC errors saved immediately (not deferred to disabled retry queue)
- **FIX**: Period selection retries 3x before failing
- **FIX**: All error paths save to BOTH memory AND database
- **FIX**: Skipped surveys count now matches CSV export exactly

### v5.0 — Latest Period Only
- Extract only the most recent period for each survey (major speed improvement)

### v4.0 — Enterprise Edition (Base)
- 12 parallel Playwright workers
- Portal health monitoring
- Smart stop with confidence scoring
- SQLite + CSV dual persistence
- Web dashboard with real-time updates

---

## 🛡️ Error Handling & Accuracy

The application has **5 layers of error protection**:

1. **Portal Retry** — Retries portal access up to 5 times with exponential backoff
2. **Session Recovery** — Detects session expiry and re-navigates automatically
3. **Period Selection Retry** — Retries period dropdown selection 3 times
4. **Network Recovery** — Waits up to 5 minutes for network restoration
5. **Browser Restart** — Restarts Chromium after consecutive errors or memory issues

**Every survey that cannot be processed is logged** with:
- Village name and code
- Survey number, surnoc, hissa
- Error reason and timestamp
- Worker ID

Skipped surveys are saved to **both** in-memory state and the SQLite database, and can be exported as CSV at any time.

---

## 💾 Data Storage

### SQLite Database
- Location: `bhoomi_data/bhoomi_land_records.db`
- Mode: WAL (Write-Ahead Logging) for concurrent access
- Tables: `search_sessions`, `land_records`, `village_progress`, `survey_checkpoints`, `skipped_items`

### CSV Files (Backup)
- `bhoomi_all_records_<timestamp>.csv` — All extracted records
- `bhoomi_matches_<timestamp>.csv` — Owner name matches only
- `skipped_surveys_<timestamp>.csv` — All skipped/failed surveys

### State Snapshots
- JSON checkpoints saved periodically for crash recovery
- Includes: completed villages, current progress, worker states

---

## ⚙️ How It Works

1. **User selects** District → Taluk → Hobli and enters owner name
2. **Engine fetches** all villages in the selected hobli
3. **Villages are distributed** across 12 workers (round-robin)
4. **Each worker** opens a headless Chromium browser and:
   - Navigates to the Bhoomi portal
   - Selects district/taluk/hobli/village
   - Iterates through survey numbers (1 to max)
   - For each survey: selects surnoc → hissa → latest period → fetches owner details
   - Extracts owner name and extent from the results table using BeautifulSoup
   - Saves to database and CSV in real-time
5. **Smart Stop** kicks in after 50 consecutive empty surveys
6. **Health Manager** continuously monitors portal availability
7. **On errors**: retry → session refresh → browser restart → network wait → log skip

---

## 🖧 Portal Details

- **Target Portal**: Karnataka Bhoomi Land Records
- **URL**: `https://landrecords.karnataka.gov.in/Service2/`
- **Data Format**: HTML tables with owner details in Kannada/English
- **Portal Columns**: Owner | Extent | Owner Category | Gov Restriction | Court Stay | Alienated

---

## 📊 Performance

| Metric | Value |
|---|---|
| Workers | 12 parallel browsers |
| Throughput | ~500-1000 surveys/minute (depends on portal response) |
| Memory | ~2-4 GB (12 Chromium instances) |
| Storage | ~50 MB per 100,000 records (SQLite) |
| Network Recovery | Up to 5 minutes wait |

---

## 🐛 Troubleshooting

| Issue | Solution |
|---|---|
| Port 5001 in use | Change `Config.PORT` or kill existing process |
| Browsers not closing | Run script — orphan Chrome cleanup runs automatically on start |
| Portal unreachable | Check `https://landrecords.karnataka.gov.in/Service2/` manually |
| High memory usage | Reduce `Config.MAX_WORKERS` to 6 or 4 |
| "Session expired" loops | Portal may be down — health manager will auto-pause |
| Empty owner names | Check portal HTML structure — may need column mapping update |

---

## 📜 License

Private — POWER-BHOOMI Team

---

*Built with Python, Playwright, Flask, SQLite, and BeautifulSoup*

