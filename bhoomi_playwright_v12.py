#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║     POWER-BHOOMI v11.0 - SKELETON-FIRST EDITION (Service154+Service2 hybrid)      ║
║                  Karnataka Land Records Search Tool - Playwright Edition             ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  PRODUCTION FEATURES:                                                                ║
║  • 24 parallel browser workers with dynamic scaling                                  ║
║  • PROACTIVE PORTAL HEALTH MONITORING (Dedicated health worker)                      ║
║  • INTELLIGENT STATE MANAGEMENT (Pause/Resume on portal issues)                      ║
║  • AUTO-RECOVERY from portal outages, rate limiting, network issues                  ║
║  • 100% ACCURACY - ALL skipped surveys tracked and logged                            ║
║  • Real-time state checkpointing (survives process crashes)                          ║
║  • Smart Stop with confidence scoring                                                ║
║  • Thread-safe CSV + SQLite persistence with deduplication                           ║
║  • LATEST PERIOD ONLY - Extracts only the most recent period                        ║
║                                                                                      ║
║  v11.0 ENHANCEMENTS over v10:                                                        ║
║  - Service154 skeleton API: enumerate ALL valid (Survey, Surnoc, Hissa) tuples       ║
║    per village in ONE HTTP call (no captcha, no auth, plain JSON)                    ║
║  - Skeleton-first scraping: drive Service2 fetches off the exact Service154 list,    ║
║    eliminating "guess survey 1..N" enumeration                                       ║
║  - No more smart-stop blind spots: villages with data at high survey numbers         ║
║    (e.g., survey 200+) are no longer abandoned                                       ║
║  - Graceful fallback: if Service154 is unreachable per village, falls back to        ║
║    v10's enumeration logic                                                           ║
║  - Increased log retention (30 -> 500) for better state replay                       ║
║                                                                                       ║
║  Carried forward from v10: 24 workers, no-owner refetch, CSV pre-creation,           ║
║  SSL bypass, Phase 2 auto-retry, deadlock-free completion.                           ║
║  • PHASE 2 AUTO-RETRY: Automatically retries all skipped surveys after Phase 1      ║
║  • DB DEDUPLICATION: UNIQUE constraint prevents duplicate records                    ║
║  • FIX: Selenium API remnants replaced with Playwright equivalents                   ║
║  • FIX: Bare except clauses replaced with explicit Exception handling                ║
║  • FIX: Header/data index alignment (v7.1 SlNo strip fix retained)                  ║
║  • FIX: Two-pass header detection (v7.1 retained)                                   ║
║  • FIX: AJAX polling for dropdown population (v7.1 retained)                         ║
║  • FIX: Errors no longer inflate empty_count / trigger premature SMART STOP         ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

Version: 8.0.0-PRODUCTION-PHASE2_RETRY
Author: POWER-BHOOMI Team
"""

import os
import sys
import json
import time
import logging
import threading
import queue
import platform
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import csv
import traceback

# Flask imports
from flask import Flask, render_template_string, jsonify, request
from flask_cors import CORS

# HTTP imports
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══════════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════════════

class Config:
    """Enterprise configuration for v11.0 — skeleton-first hybrid (S154 + S2)"""
    # Server
    # v12.2 (audit H2): bind localhost by default — the previous 0.0.0.0 +
    # debug=True exposed every control API (including /api/search/stop) and the
    # Werkzeug interactive debugger (an RCE surface) to the whole LAN.
    # Set BHOOMI_HOST=0.0.0.0 explicitly if remote access is genuinely wanted.
    HOST = os.environ.get('BHOOMI_HOST', '127.0.0.1')
    PORT = 5001
    DEBUG = False
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # WORKER SCALING: 24 workers for high-throughput production operation
    # ═══════════════════════════════════════════════════════════════════════════════════
    MAX_WORKERS = 24
    WORKER_STARTUP_DELAY = 2.5  # Stagger more for 24 workers to prevent Chrome init races
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # PORTAL HEALTH MANAGEMENT - Proactive monitoring and intelligent recovery
    # ═══════════════════════════════════════════════════════════════════════════════════
    ENABLE_HEALTH_MANAGER = True           # Enable dedicated health monitoring worker
    HEALTH_CHECK_INTERVAL = 10             # Ping portal every 10 seconds
    FUNCTIONALITY_TEST_INTERVAL = 30       # Full portal test every 30 seconds
    PORTAL_TIMEOUT_THRESHOLD = 5           # Consider slow if response > 5s
    AUTO_RESUME_ON_RECOVERY = True         # Auto-resume search when portal recovers
    MIN_WORKERS_DURING_THROTTLE = 2        # Minimum workers to keep active
    THROTTLE_SCALE_DOWN_DELAY = 15         # Seconds between scaling down workers
    THROTTLE_SCALE_UP_DELAY = 30           # Seconds between scaling up workers
    
    # Timeouts (seconds) - tuned for 24-worker production load on Bhoomi portal
    # Higher waits absorb portal slowness when many workers query simultaneously.
    PAGE_LOAD_TIMEOUT = 30
    ELEMENT_WAIT_TIMEOUT = 12
    POST_CLICK_WAIT = 5     # Wait for AJAX results to populate after Fetch click
    POST_SELECT_WAIT = 2.0  # Wait for dependent dropdowns to load
    
    # Search Settings - INTELLIGENT SMART STOP
    DEFAULT_MAX_SURVEY = 200
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # SMART STOP SETTINGS - Intelligent early termination for 70% time savings
    # ═══════════════════════════════════════════════════════════════════════════════════
    SMART_STOP_ENABLED = True           # Enable intelligent early stopping
    EMPTY_SURVEY_THRESHOLD = 50         # Stop after 50 consecutive empty surveys
    MIN_SURVEYS_BEFORE_STOP = 10        # Check at least 10 surveys before allowing stop
    TRACK_SKIPPED_SURVEYS = True        # Track all skipped surveys for retry capability
    
    # Session Recovery Settings
    MAX_SESSION_RETRIES = 3  # Retry this many times on session expiry
    SESSION_REFRESH_WAIT = 3  # Wait after refreshing session
    
    # Browser Stability Settings
    MAX_HISSA_BEFORE_RESTART = 200  # Restart browser after processing this many Hissa to prevent memory issues
    BROWSER_RESTART_DELAY = 3  # Seconds to wait before restarting browser
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # ACCURACY SETTINGS - 100% ACCURACY MODE (PRODUCTION)
    # ═══════════════════════════════════════════════════════════════════════════════════
    ACCURACY_MODE = True  # Enable all accuracy features
    PROCESS_ALL_PERIODS = False  # Process ALL periods, not just the latest
    LATEST_PERIOD_ONLY = True  # ⚡ Only extract the LATEST/FIRST period (e.g., 2025-2026)
    PERIOD_SELECTION_RETRIES = 3  # 🔧 v6.0: Retry period selection 3 times before failing
    MAX_HISSA_RETRIES = 2  # Retry individual Hissa on failure
    VERIFY_PAGE_LOAD = True  # Verify page loaded after each action
    LOG_SKIPPED_ITEMS = True  # Log all skipped items for later retry
    SAVE_SKIPPED_IMMEDIATELY = True  # 🔧 v6.0: Save skipped surveys IMMEDIATELY (not deferred)
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # ROBUST RETRY SETTINGS - Prevent false positive skips (CRITICAL FOR ACCURACY)
    # ═══════════════════════════════════════════════════════════════════════════════════
    MAX_PORTAL_RETRIES = 5          # Retry 5 times before giving up (was 2)
    RETRY_BACKOFF_BASE = 3          # Base wait time in seconds
    RETRY_BACKOFF_MULTIPLIER = 2    # Exponential backoff multiplier
    RETRY_MAX_WAIT = 60             # Maximum wait time between retries
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # NO-OWNER RE-FETCH RETRY (v9.2 FIX) - Recover from portal-overload empty responses
    # When _extract_owners returns empty after a successful Fetch, the portal likely
    # returned a partial response. Re-click Fetch and re-parse before declaring skip.
    # ═══════════════════════════════════════════════════════════════════════════════════
    NO_OWNER_REFETCH_RETRIES = 3    # Re-click Fetch up to 3 times when no owners returned
    NO_OWNER_REFETCH_WAIT = 5       # Initial wait before re-fetch (5s, 10s, 15s progressive)
    BROWSER_REFRESH_ON_RETRY = 3    # Refresh browser after this many retries
    CONSECUTIVE_ERROR_RESTART = 5   # Restart browser after this many consecutive errors
    ENABLE_RETRY_QUEUE = False      # In-village retry queue (Phase 2 replaces this)
    PORTAL_COOLDOWN_THRESHOLD = 3   # If 3+ workers fail simultaneously, pause all
    PORTAL_COOLDOWN_TIME = 30       # Seconds to wait during portal cooldown
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # PHASE 2 AUTO-RETRY - Automatically retries all skipped surveys after Phase 1
    # ═══════════════════════════════════════════════════════════════════════════════════
    PHASE2_ENABLED = True
    PHASE2_MAX_ATTEMPTS = 2         # Retry each skipped item up to 2 times
    PHASE2_DELAY_BETWEEN = 3        # Seconds between retry attempts (gentle on portal)
    PHASE2_COOLDOWN_BEFORE = 15     # Seconds to wait after Phase 1 before starting Phase 2
    PHASE2_WORKERS = 1              # Single worker for reliability
    PHASE2_FROM_DB = True           # v12: retry deduplicated skipped_items from DB, not
                                    #      the in-memory list (survives restarts, no 20-cap)

    # ═══════════════════════════════════════════════════════════════════════════════════
    # v12: STALL WATCHDOG — the #1 fix for "stuck at the end of a search"
    # ───────────────────────────────────────────────────────────────────────────────────
    # v11 waited for ALL workers to report 'completed'/'failed' with no timeout. A single
    # worker wedged inside a Playwright sync call (greenlet spin — never returns, never
    # raises) blocked Phase 1 finalization forever → no Phase 2, no completion CSVs.
    # The watchdog treats a worker whose heartbeat is older than STALL_TIMEOUT_SECONDS as
    # 'failed', records its unfinished villages for a coverage pass, and lets completion
    # proceed. Legitimate long waits (portal cooldown ≤5min, network wait ≤5min) stay well
    # under the timeout, so this never fires on a healthy-but-slow worker.
    # ═══════════════════════════════════════════════════════════════════════════════════
    STALL_TIMEOUT_SECONDS = 900     # 15 min without a heartbeat → worker considered wedged
    WATCHDOG_CHECK_INTERVAL = 30    # How often the completion monitor evaluates staleness
    GRACEFUL_STOP_TIMEOUT = 45      # Max seconds stop_search waits for workers to notice

    # ═══════════════════════════════════════════════════════════════════════════════════
    # v12: SKELETON-VERIFIED SMART STOP — completeness safety net
    # ───────────────────────────────────────────────────────────────────────────────────
    # When smart-stop would abandon a village after N empty surveys, first ask the
    # Service154 skeleton whether any surveys exist BEYOND the stop point. If so, jump to
    # those and keep scanning (skeleton can only ADD surveys to check, never remove — so a
    # truncating/under-reporting skeleton is safe here). Guarantees data clusters after a
    # >50-survey gap are not silently lost.
    # ═══════════════════════════════════════════════════════════════════════════════════
    SMART_STOP_SKELETON_VERIFY = True
    
    # URLs
    ECHAWADI_BASE = "https://rdservices.karnataka.gov.in/echawadi/Home"
    SERVICE2_URL = "https://landrecords.karnataka.gov.in/Service2/"
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # SERVICE154 SKELETON CLIENT (v11.0)
    # ───────────────────────────────────────────────────────────────────────────────────
    # Service154 (RTC + Mutation Extract Beta) exposes plain JSON APIs to enumerate
    # the EXACT (Survey, Surnoc, Hissa) tuples that exist in any village. We use this
    # as a "skeleton provider" so the Service2 browser scraper no longer has to guess
    # survey numbers 1..N. This eliminates smart-stop blind spots (villages whose data
    # starts at survey 200+) and removes most empty-survey waste.
    #
    # IMPORTANT: We DO NOT touch Service154's encrypted RTC PDFs — owner data still
    # comes from Service2's plain HTML. Service154 is purely an index/skeleton source.
    # ═══════════════════════════════════════════════════════════════════════════════════
    SERVICE154_BASE = "https://landrecords.karnataka.gov.in/Service154/Home"
    SKELETON_FIRST_ENABLED = True       # Master toggle — set False to revert to v10 enumeration
    SKELETON_FETCH_TIMEOUT = 30         # HTTP timeout (seconds) for each Service154 call
    SKELETON_MAX_RETRIES = 3            # Retries per village if Service154 fails transiently
    SKELETON_PARALLEL_WORKERS = 8       # Threads for parallel skeleton pre-fetch
    SKELETON_FALLBACK_ON_FAIL = True    # If skeleton fetch fails, fall back to enumeration
    SKELETON_CACHE_TTL_SECONDS = 3600   # Cache skeletons for 1 hour (re-fetch if older)
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # UI STATE RETENTION (v11.0)
    # ───────────────────────────────────────────────────────────────────────────────────
    # Larger retention so a browser refresh doesn't lose context.
    # ═══════════════════════════════════════════════════════════════════════════════════
    LOG_RETENTION_COUNT = 500           # Logs kept in memory (was 30 in v10)
    RECORDS_BUFFER_COUNT = 500          # Records kept in memory for UI (was 100 in v10)
    MATCHES_BUFFER_COUNT = 200          # Matches kept in memory for UI (was 50 in v10)
    
    # Element IDs (Bhoomi Portal)
    ELEMENT_IDS = {
        'district': 'ctl00_MainContent_ddlCDistrict',
        'taluk': 'ctl00_MainContent_ddlCTaluk',
        'hobli': 'ctl00_MainContent_ddlCHobli',
        'village': 'ctl00_MainContent_ddlCVillage',
        'survey_no': 'ctl00_MainContent_txtCSurveyNo',
        'surnoc': 'ctl00_MainContent_ddlCSurnocNo',
        'hissa': 'ctl00_MainContent_ddlCHissaNo',
        'period': 'ctl00_MainContent_ddlCPeriod',
        'go_btn': 'ctl00_MainContent_btnCGo',
        'fetch_btn': 'ctl00_MainContent_btnCFetchDetails',
    }


# ═══════════════════════════════════════════════════════════════════════════════════════
# SERVICE154 SKELETON CLIENT (v11.0)
# ───────────────────────────────────────────────────────────────────────────────────────
# Pure-HTTP client for Service154's enumeration endpoints. NO browser, NO captcha,
# NO encrypted-PDF handling. We use Service154 only to fetch the authoritative list
# of (Survey, Surnoc, Hissa, LandCode) tuples per village; owner extraction continues
# to use Service2 (browser-driven, plain HTML response).
#
# Endpoints used (verified May 2026, no auth/captcha required):
#     POST /Service154/Home/FillCensusDistrict          -> [{DISTRICT_CODE, DISTRICT_NAME}, ...]
#     POST /Service154/Home/FillCensusTALUKA            -> [{TALUKA_CODE, TALUKA_NAME}, ...]
#     POST /Service154/Home/FillCensushobli             -> [{HOBLI_CODE, HOBLI_NAME}, ...]
#     POST /Service154/Home/FillCensusvillege           -> [{VILLAGE_CODE, VILLAGE_NAME}, ...]
#     POST /Service154/Home/Fn_GetMRSurveyDataVillageWise
#         -> HTML page with embedded `var originalCardData = [...]` JSON array of
#            {Survey, Surnoc, Hissa, LandCode, SurveySurnocHissa, ...}
# ═══════════════════════════════════════════════════════════════════════════════════════

import re as _re_skel  # local alias to avoid clobbering module-level `re`

class Service154Client:
    """
    Thread-safe HTTP client for Service154's skeleton endpoints.
    
    All instances share a single `requests.Session` for connection pooling.
    Handles transient failures with bounded exponential backoff and falls back
    gracefully (returns empty list) if Service154 is permanently unreachable —
    callers should treat empty skeleton as "use enumeration fallback".
    
    NOT a heavy dependency: this is ~150 LOC of pure-HTTP wrapping; it does
    not require Playwright, browser instances, or any persistent state. Safe
    to instantiate per-coordinator or as a module-level singleton.
    """
    
    # ──────────────────────────────────────────────────────────────────────────
    # Embedded JSON regex — Service154's village-survey endpoint returns an HTML
    # page with the actual data in `var originalCardData = [...]`. We extract
    # the JSON via a non-greedy match anchored to the variable declaration.
    # ──────────────────────────────────────────────────────────────────────────
    _CARD_DATA_PATTERN = _re_skel.compile(
        r'var\s+originalCardData\s*=\s*(\[.*?\]);',
        _re_skel.DOTALL,
    )
    
    def __init__(
        self,
        base_url: str = None,
        timeout: float = None,
        max_retries: int = None,
    ):
        self.base_url = (base_url or Config.SERVICE154_BASE).rstrip('/')
        self.timeout = timeout if timeout is not None else Config.SKELETON_FETCH_TIMEOUT
        self.max_retries = max_retries if max_retries is not None else Config.SKELETON_MAX_RETRIES
        
        # Connection-pooled session shared across calls (HTTPAdapter handles
        # concurrency safely). verify=False — Bhoomi cert sometimes expires;
        # we already accept that risk in the Playwright path.
        self._session = requests.Session()
        self._session.verify = False
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; PowerBhoomi/v11.0)',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://landrecords.karnataka.gov.in/service154',
        })
        
        # Suppress urllib3 warnings about verify=False
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
        
        # Optional in-memory cache: {(district, taluk, hobli, village): (timestamp, data)}
        self._cache: Dict[Tuple[str, str, str, str], Tuple[float, List[Dict]]] = {}
        self._cache_lock = threading.Lock()
        self._cache_ttl = Config.SKELETON_CACHE_TTL_SECONDS
    
    # ──────────────────────────────────────────────────────────────────────────
    # LOW-LEVEL: single POST with retry + JSON-or-string parsing.
    # ──────────────────────────────────────────────────────────────────────────
    def _post(
        self,
        endpoint: str,
        data: Any,
        as_json_body: bool = False,
    ) -> Optional[Any]:
        """
        POST to a Service154 endpoint. Handles two body styles:
          - as_json_body=True : Content-Type application/json, body is json.dumps(data)
          - as_json_body=False: form-encoded multipart, requests' default
        
        Returns parsed JSON response (auto-handles double-encoded strings),
        or None if all retries exhausted.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(1, self.max_retries + 1):
            try:
                if as_json_body:
                    resp = self._session.post(
                        url,
                        data=json.dumps(data),
                        headers={'Content-Type': 'application/json; charset=utf-8'},
                        timeout=self.timeout,
                    )
                else:
                    resp = self._session.post(url, data=data, timeout=self.timeout)
                
                if resp.status_code != 200:
                    logger.debug(f"S154 {endpoint} attempt {attempt}: HTTP {resp.status_code}")
                    if attempt < self.max_retries:
                        time.sleep(min(2 ** attempt, 10))
                        continue
                    return None
                
                # Service154 sometimes returns a JSON-encoded string of JSON
                # (e.g., '"[{...}]"'). Unwrap defensively.
                payload = resp.json() if resp.headers.get('content-type', '').startswith('application/json') else None
                if payload is None:
                    # try parsing the text as JSON
                    try:
                        payload = json.loads(resp.text)
                    except json.JSONDecodeError:
                        return resp.text  # caller may want raw HTML (e.g. Fn_GetMRSurveyDataVillageWise)
                
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError:
                        pass
                
                return payload
            
            except (requests.RequestException, ValueError) as e:
                logger.debug(f"S154 {endpoint} attempt {attempt}/{self.max_retries}: {type(e).__name__}: {str(e)[:80]}")
                if attempt < self.max_retries:
                    time.sleep(min(2 ** attempt, 10))
        
        return None
    
    # ──────────────────────────────────────────────────────────────────────────
    # HIGH-LEVEL: skeleton fetch (the only call that matters for v11 scraping).
    # ──────────────────────────────────────────────────────────────────────────
    def fetch_village_skeleton(
        self,
        district_code: str,
        taluk_code: str,
        hobli_code: str,
        village_code: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch the authoritative list of (Survey, Surnoc, Hissa, LandCode) tuples
        that exist in the given village. Uses Service154's
        Fn_GetMRSurveyDataVillageWise endpoint and parses the embedded
        `originalCardData` JSON array.
        
        Returns:
            List of dicts: [{'survey_no': int, 'surnoc': str, 'hissa': str,
                             'land_code': str, 'survey_surnoc_hissa': str}, ...]
            Empty list on failure (caller should fall back to enumeration).
        
        Thread-safe; cached per (district, taluk, hobli, village) for
        Config.SKELETON_CACHE_TTL_SECONDS.
        """
        cache_key = (str(district_code), str(taluk_code), str(hobli_code), str(village_code))
        
        with self._cache_lock:
            cached = self._cache.get(cache_key)
            if cached and (time.time() - cached[0]) < self._cache_ttl:
                return list(cached[1])  # defensive copy
        
        payload = self._post(
            'Fn_GetMRSurveyDataVillageWise',
            data={
                'District': str(district_code),
                'Taluk': str(taluk_code),
                'Hobli': str(hobli_code),
                'Village': str(village_code),
            },
            as_json_body=False,  # this endpoint uses form-encoded
        )
        
        if not isinstance(payload, str):
            return []  # unexpected response shape
        
        match = self._CARD_DATA_PATTERN.search(payload)
        if not match:
            return []
        
        try:
            raw_records = json.loads(match.group(1))
        except json.JSONDecodeError as e:
            logger.warning(f"S154 skeleton JSON parse failed for {cache_key}: {e}")
            return []
        
        # Normalize: trim whitespace from Surnoc/Hissa (S154 pads with spaces),
        # coerce types, and dedup by (survey, surnoc, hissa) since S154 sometimes
        # returns duplicate rows with different LandCode for the same logical key.
        normalized: List[Dict[str, Any]] = []
        seen: set = set()
        for r in raw_records:
            if not isinstance(r, dict):
                continue
            try:
                sy = int(r.get('Survey', 0))
            except (TypeError, ValueError):
                continue
            sn = str(r.get('Surnoc', '')).strip()
            hi = str(r.get('Hissa', '')).strip()
            land_code = str(r.get('LandCode', ''))
            ssh = str(r.get('SurveySurnocHissa', f"{sy}{sn}{hi}")).strip()
            
            key = (sy, sn, hi)
            if key in seen:
                continue
            seen.add(key)
            
            normalized.append({
                'survey_no': sy,
                'surnoc': sn,
                'hissa': hi,
                'land_code': land_code,
                'survey_surnoc_hissa': ssh,
            })
        
        # Sort: (survey_no, surnoc, hissa) — deterministic order helps with
        # checkpointing and resume.
        normalized.sort(key=lambda r: (r['survey_no'], r['surnoc'], r['hissa']))
        
        with self._cache_lock:
            self._cache[cache_key] = (time.time(), list(normalized))
        
        return normalized
    
    def clear_cache(self) -> None:
        """Drop all cached skeletons. Useful after a long pause."""
        with self._cache_lock:
            self._cache.clear()
    
    def is_reachable(self) -> bool:
        """Quick health check — returns True if Service154 responds within timeout."""
        result = self._post('FillCensusDistrict', data={}, as_json_body=False)
        return isinstance(result, list) and len(result) > 0


# Module-level singleton — instantiated lazily on first use.
_service154_client_singleton: Optional[Service154Client] = None
_service154_client_lock = threading.Lock()

def get_service154_client() -> Service154Client:
    """Return the process-wide Service154Client singleton."""
    global _service154_client_singleton
    if _service154_client_singleton is None:
        with _service154_client_lock:
            if _service154_client_singleton is None:
                _service154_client_singleton = Service154Client()
    return _service154_client_singleton


# ═══════════════════════════════════════════════════════════════════════════════════════
# BROWSER CLEANUP UTILITY - CRITICAL FOR STABILITY
# Ensures no orphaned Chrome processes leak memory
# ═══════════════════════════════════════════════════════════════════════════════════════

class BrowserCleanup:
    """
    PRODUCTION-GRADE browser cleanup utility.
    Ensures Chrome processes are ALWAYS cleaned up, even on crashes.
    """
    _active_pids: Dict[int, str] = {}  # pid -> worker_id mapping
    _lock = threading.Lock()
    
    @classmethod
    def register_browser(cls, pid: int, worker_id: str):
        """Register a browser process for tracking"""
        with cls._lock:
            cls._active_pids[pid] = worker_id
            logger.debug(f"Registered browser PID {pid} for {worker_id}")
    
    @classmethod
    def unregister_browser(cls, pid: int):
        """Unregister a browser process"""
        with cls._lock:
            if pid in cls._active_pids:
                del cls._active_pids[pid]
                logger.debug(f"Unregistered browser PID {pid}")
    
    @classmethod
    def kill_browser_by_pid(cls, pid: int) -> bool:
        """Forcefully kill a browser by PID"""
        import signal
        try:
            os.kill(pid, signal.SIGKILL)
            cls.unregister_browser(pid)
            return True
        except ProcessLookupError:
            cls.unregister_browser(pid)
            return True  # Already dead
        except Exception as e:
            logger.warning(f"Failed to kill PID {pid}: {e}")
            return False
    
    @classmethod
    def kill_chrome_for_worker(cls, worker_id: str):
        """Kill all Chrome processes for a specific worker's user data dir"""
        import subprocess
        user_data_dir = f'bhoomi_chrome_{worker_id}'
        try:
            # Find and kill processes with this user data dir
            result = subprocess.run(
                ['pgrep', '-f', user_data_dir],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid in pids:
                    try:
                        os.kill(int(pid), 9)
                        logger.debug(f"Killed Chrome PID {pid} for {worker_id}")
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"Error killing Chrome for {worker_id}: {e}")
    
    @classmethod
    def kill_all_bhoomi_chrome(cls):
        """Nuclear option: Kill ALL bhoomi Chrome processes"""
        import subprocess
        killed = 0
        try:
            # Kill all chromedriver processes for bhoomi
            subprocess.run(['pkill', '-9', '-f', 'bhoomi_chrome'], 
                          capture_output=True, timeout=5)
            # Kill all chromedriver processes
            result = subprocess.run(['pkill', '-9', '-f', 'chromedriver'],
                                   capture_output=True, timeout=5)
            # Clear our tracking
            with cls._lock:
                killed = len(cls._active_pids)
                cls._active_pids.clear()
            logger.info(f"🧹 Killed all bhoomi Chrome processes ({killed} tracked)")
        except Exception as e:
            logger.warning(f"Error in kill_all_bhoomi_chrome: {e}")
        return killed
    
    @classmethod
    def cleanup_orphans(cls):
        """Cleanup orphaned Chrome processes (no corresponding worker)"""
        import subprocess
        try:
            # Count bhoomi_chrome processes
            result = subprocess.run(
                ['pgrep', '-f', 'bhoomi_chrome'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                current_count = len(pids)
                expected_max = Config.MAX_WORKERS + 2  # workers + health + prepare
                
                if current_count > expected_max * 7:  # Each Chrome has ~7 helper processes
                    logger.warning(f"⚠️ {current_count} Chrome processes detected (expected ~{expected_max * 7})")
                    logger.warning("🧹 Running cleanup of orphaned processes...")
                    cls.kill_all_bhoomi_chrome()
                    return True
        except Exception as e:
            logger.debug(f"Error checking orphans: {e}")
        return False
    
    @classmethod
    def get_chrome_count(cls) -> int:
        """Get current number of Chrome processes"""
        import subprocess
        try:
            result = subprocess.run(
                ['pgrep', '-f', 'bhoomi_chrome'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.stdout.strip():
                return len(result.stdout.strip().split('\n'))
        except Exception:
            pass
        return 0


# Background thread that periodically cleans up orphaned browsers
def _browser_cleanup_daemon():
    """Daemon thread that cleans up orphaned Chrome processes every 60 seconds"""
    while True:
        try:
            time.sleep(60)  # Check every 60 seconds
            chrome_count = BrowserCleanup.get_chrome_count()
            max_expected = (Config.MAX_WORKERS + 2) * 7  # Each Chrome ~7 processes
            
            if chrome_count > max_expected:
                logger.warning(f"🧹 Cleanup daemon: {chrome_count} Chrome processes (max expected: {max_expected})")
                BrowserCleanup.cleanup_orphans()
        except Exception as e:
            logger.debug(f"Cleanup daemon error: {e}")

# Start cleanup daemon on module load
_cleanup_thread = threading.Thread(target=_browser_cleanup_daemon, daemon=True, name="BrowserCleanupDaemon")
_cleanup_thread.start()

# CRITICAL: Clean up any orphaned Chrome from previous runs on module load
def _startup_cleanup():
    """Kill all bhoomi Chrome processes on startup for clean slate"""
    try:
        print("🧹 Startup cleanup: Killing any orphaned Chrome processes...")
        BrowserCleanup.kill_all_bhoomi_chrome()
        time.sleep(1)  # Give OS time to clean up
    except Exception as e:
        print(f"Startup cleanup error: {e}")

# Run startup cleanup
threading.Thread(target=_startup_cleanup, daemon=True).start()

# ═══════════════════════════════════════════════════════════════════════════════════════
# ADAPTIVE WAIT STRATEGY - Reduces fixed waits by 60%
# ═══════════════════════════════════════════════════════════════════════════════════════

class WaitStrategy:
    """
    Adaptive waiting based on actual element availability.
    Replaces hardcoded time.sleep() calls with intelligent waiting.
    """
    
    # Cached response times per element type
    _response_times: Dict[str, List[float]] = {}
    _lock = threading.Lock()
    
    @classmethod
    def get_adaptive_timeout(cls, element_type: str, base_timeout: float) -> float:
        """Get adaptive timeout based on historical response times"""
        with cls._lock:
            if element_type in cls._response_times:
                times = cls._response_times[element_type]
                if len(times) >= 5:
                    # Use 90th percentile + 20% buffer
                    sorted_times = sorted(times)
                    p90 = sorted_times[int(len(sorted_times) * 0.9)]
                    return min(p90 * 1.2, base_timeout)
            return base_timeout
    
    @classmethod
    def record_response_time(cls, element_type: str, elapsed: float):
        """Record actual response time for adaptive learning"""
        with cls._lock:
            if element_type not in cls._response_times:
                cls._response_times[element_type] = []
            cls._response_times[element_type].append(elapsed)
            # Keep last 100 measurements
            if len(cls._response_times[element_type]) > 100:
                cls._response_times[element_type] = cls._response_times[element_type][-100:]
    
    @classmethod
    def wait_for_element(cls, driver, locator: tuple, element_type: str = "generic", 
                         timeout: float = None, condition: str = "clickable") -> Any:
        """
        Wait for element with adaptive timeout and response time tracking.
        
        Args:
            driver: Selenium WebDriver instance
            locator: Tuple of (By, value) for element location
            element_type: Category for adaptive timeout (e.g., 'dropdown', 'button')
            timeout: Max timeout (uses adaptive if None)
            condition: 'clickable', 'visible', or 'present'
        
        Returns:
            The located element
        """
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        base_timeout = timeout or Config.ELEMENT_WAIT_TIMEOUT
        adaptive_timeout = cls.get_adaptive_timeout(element_type, base_timeout)
        
        conditions = {
            'clickable': EC.element_to_be_clickable,
            'visible': EC.visibility_of_element_located,
            'present': EC.presence_of_element_located
        }
        ec_condition = conditions.get(condition, EC.element_to_be_clickable)
        
        start_time = time.time()
        try:
            wait = WebDriverWait(driver, adaptive_timeout)
            element = wait.until(ec_condition(locator))
            elapsed = time.time() - start_time
            cls.record_response_time(element_type, elapsed)
            return element
        except Exception:
            # On timeout, increase future timeouts for this element type
            cls.record_response_time(element_type, adaptive_timeout)
            raise
    
    @classmethod
    def wait_for_dropdown_options(cls, driver, select_element, min_options: int = 1, 
                                   timeout: float = None) -> bool:
        """
        Wait for dropdown to have options loaded.
        
        Args:
            driver: Selenium WebDriver instance
            select_element: The Select element to check
            min_options: Minimum number of options required
            timeout: Max timeout
        
        Returns:
            True if options loaded, False if timeout
        """
        base_timeout = timeout or Config.ELEMENT_WAIT_TIMEOUT
        adaptive_timeout = cls.get_adaptive_timeout("dropdown_options", base_timeout)
        
        start_time = time.time()
        while time.time() - start_time < adaptive_timeout:
            try:
                options = select_element.options
                if len(options) >= min_options:
                    elapsed = time.time() - start_time
                    cls.record_response_time("dropdown_options", elapsed)
                    return True
            except Exception:
                pass
            time.sleep(0.1)  # Short poll interval
        
        cls.record_response_time("dropdown_options", adaptive_timeout)
        return False
    
    @classmethod
    def adaptive_sleep(cls, element_type: str, default_seconds: float):
        """
        Sleep for adaptive duration based on historical timings.
        Falls back to default if no history.
        """
        adaptive = cls.get_adaptive_timeout(element_type, default_seconds)
        time.sleep(min(adaptive, default_seconds))


# ═══════════════════════════════════════════════════════════════════════════════════════
# SMART NAVIGATOR - Detects Current State and Minimizes Navigation
# ═══════════════════════════════════════════════════════════════════════════════════════

class SmartNavigator:
    """
    Tracks form state to avoid redundant navigation.
    Instead of re-selecting all dropdowns for each survey, only changes what's needed.
    This can reduce navigation time by 70% for consecutive surveys in the same village.
    """
    
    def __init__(self, driver, worker_id: int):
        self.driver = driver
        self.worker_id = worker_id
        self.logger = logging.getLogger(f'Navigator-{worker_id}')
        
        # Current form state
        self._state = {
            'district': None,
            'taluk': None,
            'hobli': None,
            'village': None,
            'survey_no': None,
            'surnoc': None,
            'hissa': None,
            'period': None
        }
        self._last_page_url = None
    
    def get_current_state(self) -> Dict[str, Optional[str]]:
        """Return current cached state"""
        return self._state.copy()
    
    def update_state(self, **kwargs):
        """Update cached state"""
        for key, value in kwargs.items():
            if key in self._state:
                self._state[key] = value
    
    def reset_state(self):
        """Reset all cached state (e.g., after browser restart)"""
        for key in self._state:
            self._state[key] = None
        self._last_page_url = None
    
    def is_on_portal(self) -> bool:
        """Check if we're currently on the Bhoomi portal"""
        try:
            return 'landrecords.karnataka.gov.in' in self.page.url
        except Exception:
            return False
    
    def needs_navigation(self, target_village: str, target_hobli: str) -> bool:
        """Check if we need to navigate or if we're already on the right village"""
        return (self._state['village'] != target_village or 
                self._state['hobli'] != target_hobli)
    
    def needs_dropdown_update(self, field: str, target_value: str) -> bool:
        """Check if a specific dropdown needs to be changed"""
        return self._state.get(field) != target_value
    
    def clear_survey_state(self):
        """Clear survey-specific state (survey_no, surnoc, hissa, period)"""
        self._state['survey_no'] = None
        self._state['surnoc'] = None
        self._state['hissa'] = None
        self._state['period'] = None
    
    def invalidate_below(self, field: str):
        """
        Invalidate cached state for fields that depend on the given field.
        E.g., changing hobli invalidates village, survey, etc.
        """
        hierarchy = ['district', 'taluk', 'hobli', 'village', 'survey_no', 'surnoc', 'hissa', 'period']
        try:
            idx = hierarchy.index(field)
            for f in hierarchy[idx + 1:]:
                self._state[f] = None
        except ValueError:
            pass


# ═══════════════════════════════════════════════════════════════════════════════════════
# CACHED CHROMEDRIVER MANAGER
# ═══════════════════════════════════════════════════════════════════════════════════════

class CachedChromeDriver:
    """
    Singleton to cache ChromeDriver path.
    Eliminates redundant webdriver-manager lookups which can take 1-2 seconds each.
    With 8 workers, this saves 8-16 seconds on startup.
    """
    
    _instance = None
    _lock = threading.Lock()
    _driver_path: Optional[str] = None
    
    @classmethod
    def get_driver_path(cls) -> str:
        """Get cached ChromeDriver path, downloading if necessary"""
        if cls._driver_path is None:
            with cls._lock:
                if cls._driver_path is None:  # Double-check pattern
                    from webdriver_manager.chrome import ChromeDriverManager
                    cls._driver_path = ChromeDriverManager().install()
                    logger.info(f"🔧 ChromeDriver cached: {cls._driver_path}")
        return cls._driver_path
    
    @classmethod
    def get_service(cls):
        """Get a Chrome Service with the cached driver path"""
        from selenium.webdriver.chrome.service import Service
        return Service(cls.get_driver_path())
    
    @classmethod
    def clear_cache(cls):
        """Clear the cached path (e.g., for testing or updates)"""
        with cls._lock:
            cls._driver_path = None


# ═══════════════════════════════════════════════════════════════════════════════════════
# PORTAL HEALTH MANAGER - Proactive monitoring and intelligent recovery
# ═══════════════════════════════════════════════════════════════════════════════════════

class PortalHealthManager:
    """
    Enterprise-grade portal health management with proactive monitoring.
    
    Features:
    - Proactive health checks (ping, functionality tests)
    - Distinguishes: DOWN vs RATE_LIMITED vs DEGRADED vs HEALTHY
    - Coordinates worker pause/resume based on portal state
    - Automatic recovery when portal becomes healthy
    - Network vs portal issue detection
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        # Portal state tracking
        self.current_state = 'UNKNOWN'  # HEALTHY, DEGRADED, RATE_LIMITED, NETWORK_CONGESTION, DOWN, UNKNOWN
        self.last_check_time = 0
        self.last_response_time = 0
        
        # Error tracking from workers (reactive)
        self.recent_errors = []  # List of (timestamp, worker_id, error_type) tuples
        self.error_window = 10   # Consider errors within this many seconds
        
        # Cooldown management
        self.is_cooling_down = False
        self.cooldown_until = 0
        self.cooldown_reason = ''
        
        # Health check results
        self.ping_success_rate = 1.0  # 0.0 to 1.0
        self.avg_response_time = 0
        self.last_successful_check = time.time()
        
        # Monitoring thread
        self._stop_monitoring = threading.Event()
        self._monitor_thread = None
        self._health_lock = threading.Lock()
        
        # Statistics
        self.total_checks = 0
        self.failed_checks = 0
        self.state_changes = []  # History of state changes
        
        logger.info("🏥 PortalHealthManager initialized")
    
    def start_monitoring(self):
        """Start proactive health monitoring"""
        if Config.ENABLE_HEALTH_MANAGER and not self._monitor_thread:
            self._monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self._monitor_thread.start()
            logger.info("🏥 Portal health monitoring started")
    
    def stop_monitoring(self):
        """Stop health monitoring"""
        if self._monitor_thread:
            self._stop_monitoring.set()
            self._monitor_thread.join(timeout=5)
            logger.info("🏥 Portal health monitoring stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop - runs in background thread"""
        functionality_check_counter = 0
        
        while not self._stop_monitoring.wait(Config.HEALTH_CHECK_INTERVAL):
            try:
                # Ping check every interval
                self._perform_ping_check()
                
                # Functionality check every N intervals
                functionality_check_counter += 1
                if functionality_check_counter * Config.HEALTH_CHECK_INTERVAL >= Config.FUNCTIONALITY_TEST_INTERVAL:
                    self._perform_functionality_check()
                    functionality_check_counter = 0
                
                # Update portal state based on checks
                self._update_portal_state()
                
            except Exception as e:
                logger.error(f"Health monitoring error: {e}")
    
    def _perform_ping_check(self) -> bool:
        """
        Lightweight ping to portal homepage.
        Returns True if portal is reachable, False otherwise.
        """
        try:
            start_time = time.time()
            response = requests.head(Config.SERVICE2_URL, timeout=5, verify=False)
            elapsed = time.time() - start_time
            
            with self._health_lock:
                self.last_check_time = time.time()
                self.last_response_time = elapsed
                self.total_checks += 1
                
                if response.status_code == 200:
                    self.last_successful_check = time.time()
                    # Update success rate (rolling average)
                    self.ping_success_rate = 0.9 * self.ping_success_rate + 0.1
                    self.avg_response_time = 0.8 * self.avg_response_time + 0.2 * elapsed
                    return True
                else:
                    self.failed_checks += 1
                    self.ping_success_rate = 0.9 * self.ping_success_rate
                    return False
                    
        except requests.Timeout:
            with self._health_lock:
                self.failed_checks += 1
                self.ping_success_rate = 0.9 * self.ping_success_rate
            logger.warning("🏥 Portal ping timeout")
            return False
            
        except Exception as e:
            with self._health_lock:
                self.failed_checks += 1
                self.ping_success_rate = 0.9 * self.ping_success_rate
            logger.debug(f"Portal ping failed: {e}")
            return False
    
    def _perform_functionality_check(self) -> bool:
        """Full functionality test (Playwright version)

        v12 FIX (browser leak): the old version closed the browser inline BEFORE the
        except handler — any goto/selector failure skipped cleanup entirely, leaking one
        headless Chromium per failed check (every 30s, forever). Cleanup now lives in a
        finally block with per-resource guards.
        """
        pw = browser = context = None
        try:
            from playwright.sync_api import sync_playwright

            # v9.1 FIX: Bypass SSL errors — Bhoomi portal cert is sometimes invalid.
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                headless=True,
                args=['--ignore-certificate-errors', '--ignore-ssl-errors']
            )
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            page.set_default_timeout(10000)

            start_time = time.time()
            page.goto(Config.SERVICE2_URL)
            page.wait_for_load_state('domcontentloaded')

            # Check if district dropdown has options
            district_opts = page.locator(f'#{Config.ELEMENT_IDS["district"]} option').all()
            district_count = sum(1 for opt in district_opts if opt.get_attribute('value'))

            elapsed = time.time() - start_time

            if district_count > 0:
                logger.info(f"🏥 Portal check PASSED ({elapsed:.1f}s, {district_count} districts)")
                return True
            else:
                logger.warning("🏥 Portal check FAILED: No districts")
                return False

        except Exception as e:
            logger.warning(f"🏥 Portal check FAILED: {str(e)[:50]}")
            return False
        finally:
            # Per-resource guards: one close failing must not skip the others.
            for closer in (
                (lambda: context.close()) if context else None,
                (lambda: browser.close()) if browser else None,
                (lambda: pw.stop()) if pw else None,
            ):
                if closer:
                    try:
                        closer()
                    except Exception:
                        pass
    
    def _update_portal_state(self):
        """Update portal state based on health check results and worker errors"""
        with self._health_lock:
            now = time.time()
            
            # Clean old errors
            self.recent_errors = [(t, w, et) for t, w, et in self.recent_errors 
                                   if now - t < self.error_window]
            
            # Count recent errors by unique workers
            recent_workers_with_errors = set(w for t, w, et in self.recent_errors)
            error_count = len(recent_workers_with_errors)
            
            # Determine new state
            old_state = self.current_state
            new_state = 'UNKNOWN'
            
            # Check 1: Portal ping success
            if self.ping_success_rate > 0.9 and self.avg_response_time < Config.PORTAL_TIMEOUT_THRESHOLD:
                # Portal is responding well
                if error_count == 0:
                    new_state = 'HEALTHY'
                elif error_count < Config.PORTAL_COOLDOWN_THRESHOLD:
                    new_state = 'HEALTHY'  # Few errors are normal
                else:
                    new_state = 'RATE_LIMITED'  # Portal up but blocking us
                    
            elif self.ping_success_rate > 0.7:
                # Portal responding but slowly
                new_state = 'DEGRADED'
                
            elif self.ping_success_rate > 0.3:
                # Portal intermittently reachable
                new_state = 'NETWORK_CONGESTION'
                
            else:
                # Portal mostly unreachable
                new_state = 'DOWN'
            
            # Update state and log changes
            if new_state != old_state:
                self.current_state = new_state
                self.state_changes.append({
                    'from': old_state,
                    'to': new_state,
                    'timestamp': datetime.now().isoformat(),
                    'ping_rate': self.ping_success_rate,
                    'response_time': self.avg_response_time,
                    'worker_errors': error_count
                })
                
                logger.info(f"🏥 Portal state changed: {old_state} → {new_state} (errors: {error_count}, ping: {self.ping_success_rate:.2f})")
                
                # MEMORY FIX: Trim state_changes to prevent unbounded growth
                if len(self.state_changes) > 100:
                    self.state_changes = self.state_changes[-100:]
    
    def report_error(self, worker_id: int, error_type: str = 'generic') -> bool:
        """
        Report a portal error from a worker.
        Returns True if workers should pause (portal appears overloaded).
        """
        now = time.time()
        
        with self._health_lock:
            # Add error
            self.recent_errors.append((now, worker_id, error_type))
            
            # Update state
            self._update_portal_state()
            
            # Trigger cooldown if needed
            recent_workers = set(w for t, w, et in self.recent_errors 
                                if now - t < self.error_window)
            
            if len(recent_workers) >= Config.PORTAL_COOLDOWN_THRESHOLD:
                if not self.is_cooling_down:
                    self.cooldown_reason = f"{len(recent_workers)} workers failing"
                    logger.warning(f"⚠️ PORTAL OVERLOAD: {self.cooldown_reason}. Cooldown {Config.PORTAL_COOLDOWN_TIME}s")
                self.is_cooling_down = True
                self.cooldown_until = now + Config.PORTAL_COOLDOWN_TIME
                return True
            
            return self.is_cooling_down and now < self.cooldown_until
    
    def should_wait(self) -> float:
        """Returns seconds to wait if in cooldown, 0 otherwise."""
        with self._health_lock:
            if self.is_cooling_down:
                now = time.time()
                if now < self.cooldown_until:
                    return self.cooldown_until - now
                else:
                    self.is_cooling_down = False
                    logger.info("✅ Portal cooldown ended")
            return 0
    
    def report_success(self, worker_id: int):
        """Report a successful operation"""
        with self._health_lock:
            # Remove this worker's recent errors
            self.recent_errors = [(t, w, et) for t, w, et in self.recent_errors 
                                   if w != worker_id]
            # Update success rate
            self.ping_success_rate = min(1.0, self.ping_success_rate + 0.05)
    
    def get_state(self) -> str:
        """Get current portal state"""
        with self._health_lock:
            return self.current_state
    
    def is_healthy(self) -> bool:
        """Check if portal is in healthy state"""
        return self.current_state in ('HEALTHY', 'DEGRADED')
    
    def is_down(self) -> bool:
        """Check if portal is completely down"""
        return self.current_state == 'DOWN'
    
    def is_rate_limited(self) -> bool:
        """Check if we're being rate limited"""
        return self.current_state == 'RATE_LIMITED'
    
    def get_stats(self) -> dict:
        """Get health statistics"""
        with self._health_lock:
            return {
                'current_state': self.current_state,
                'ping_success_rate': round(self.ping_success_rate, 3),
                'avg_response_time': round(self.avg_response_time, 2),
                'total_checks': self.total_checks,
                'failed_checks': self.failed_checks,
                'is_cooling_down': self.is_cooling_down,
                'cooldown_seconds_remaining': max(0, int(self.cooldown_until - time.time())) if self.is_cooling_down else 0,
                'recent_state_changes': self.state_changes[-5:] if self.state_changes else []
            }

# Global portal health manager instance (will be initialized after logger is defined)
portal_health = None


# ═══════════════════════════════════════════════════════════════════════════════════════
# STATE MANAGER - Enterprise-grade state preservation and recovery
# ═══════════════════════════════════════════════════════════════════════════════════════

class StateManager:
    """
    Manages search state with multiple persistence layers for bulletproof recovery.
    
    State Layers:
    1. Hot (in-memory): Instant access, lost on crash
    2. Warm (SQLite WAL): Written every 5s, survives crashes
    3. Cold (JSON snapshot): Written every 60s, human-readable backup
    
    Supports:
    - Pause/resume on portal issues
    - Recovery from process crashes
    - State validation and rollback
    - Granular checkpointing per worker
    """
    
    def __init__(self, db: 'DatabaseManager', session_id: str):
        self.db = db
        self.session_id = session_id
        self._lock = threading.Lock()
        
        # State snapshots
        self.last_snapshot_time = 0
        self.snapshot_interval = 60  # Full snapshot every 60s
        self.checkpoint_interval = 5  # Quick checkpoint every 5s
        self.last_checkpoint_time = 0
        
        # State file paths
        self.state_dir = os.path.join(db.db_folder, 'state_snapshots')
        os.makedirs(self.state_dir, exist_ok=True)
        self.state_file = os.path.join(self.state_dir, f'state_{session_id}.json')
        
        # Pause/resume state
        self.is_paused = False
        self.pause_reason = ''
        self.paused_at = None
        self.paused_state = None  # State snapshot taken at pause time
        
        logger.info(f"📊 StateManager initialized for session {session_id}")
    
    def save_snapshot(self, search_state: dict, worker_states: dict):
        """
        Save full state snapshot to JSON file.
        This is the "cold" state - human-readable, survives everything.
        """
        try:
            snapshot = {
                'session_id': self.session_id,
                'timestamp': datetime.now().isoformat(),
                'search_state': search_state,
                'worker_states': worker_states,
                'portal_health': portal_health.get_stats(),
                'is_paused': self.is_paused,
                'pause_reason': self.pause_reason
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(snapshot, f, indent=2)
            
            self.last_snapshot_time = time.time()
            logger.debug(f"📊 State snapshot saved: {self.state_file}")
            
        except Exception as e:
            logger.error(f"Failed to save state snapshot: {e}")
    
    def save_checkpoint(self, worker_id: int, village_code: str, survey_no: int):
        """
        Quick checkpoint - save to SQLite (warm state).
        Called frequently during search for granular recovery.
        """
        try:
            # This uses the existing survey_checkpoints table
            self.db.save_survey_checkpoint(
                session_id=self.session_id,
                village_code=village_code,
                survey_no=survey_no
            )
            self.last_checkpoint_time = time.time()
            
        except Exception as e:
            logger.debug(f"Checkpoint save failed: {e}")
    
    def pause_search(self, reason: str) -> dict:
        """
        Pause search and preserve current state.
        Returns the paused state for later resume.
        """
        with self._lock:
            if self.is_paused:
                return self.paused_state
            
            self.is_paused = True
            self.pause_reason = reason
            self.paused_at = datetime.now().isoformat()
            
            # Take snapshot immediately
            # (This would be called by coordinator with actual state)
            logger.warning(f"⏸️ Search PAUSED: {reason}")
            
            return {
                'paused_at': self.paused_at,
                'reason': reason
            }
    
    def resume_search(self) -> bool:
        """
        Resume search from paused state.
        Returns True if resume successful, False otherwise.
        """
        with self._lock:
            if not self.is_paused:
                logger.warning("Cannot resume - search is not paused")
                return False
            
            self.is_paused = False
            pause_duration = (datetime.now() - datetime.fromisoformat(self.paused_at)).total_seconds()
            
            logger.info(f"▶️ Search RESUMED after {int(pause_duration)}s pause ({self.pause_reason})")
            
            self.pause_reason = ''
            self.paused_at = None
            return True
    
    def load_snapshot(self) -> Optional[dict]:
        """Load last saved snapshot from disk"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load state snapshot: {e}")
        return None
    
    def get_recovery_info(self) -> dict:
        """Get information needed to resume search after crash"""
        try:
            # Get checkpoints from database
            checkpoints = self.db.get_all_checkpoints(self.session_id)
            pending_villages = self.db.get_pending_villages(self.session_id)
            
            return {
                'can_resume': len(pending_villages) > 0,
                'checkpoints': checkpoints,
                'pending_villages': pending_villages,
                'total_pending': len(pending_villages)
            }
        except Exception as e:
            logger.error(f"Failed to get recovery info: {e}")
            return {'can_resume': False}


# ═══════════════════════════════════════════════════════════════════════════════════════
# RATE LIMITER - Prevents overwhelming the portal
# ═══════════════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    """
    Token bucket rate limiter to prevent overwhelming the Bhoomi portal.
    
    Coordinates across all workers to maintain a sustainable request rate.
    This helps avoid triggering portal's DDoS protection and reduces false errors.
    """
    
    def __init__(self, requests_per_second: float = 2.0, burst_size: int = 10):
        """
        Args:
            requests_per_second: Average allowed request rate
            burst_size: Maximum burst of requests allowed
        """
        self.rate = requests_per_second
        self.burst_size = burst_size
        self._tokens = burst_size
        self._last_update = time.time()
        self._lock = threading.Lock()
        
        # Stats tracking
        self._total_requests = 0
        self._total_waits = 0
        self._total_wait_time = 0.0
    
    def acquire(self, timeout: float = 30.0) -> bool:
        """
        Acquire a token to make a request.
        
        Args:
            timeout: Maximum time to wait for a token
        
        Returns:
            True if token acquired, False if timeout
        """
        deadline = time.time() + timeout
        
        while True:
            with self._lock:
                now = time.time()
                
                # Add tokens based on elapsed time
                elapsed = now - self._last_update
                self._tokens = min(self.burst_size, self._tokens + elapsed * self.rate)
                self._last_update = now
                
                if self._tokens >= 1:
                    self._tokens -= 1
                    self._total_requests += 1
                    return True
                
                # Calculate wait time
                wait_time = (1 - self._tokens) / self.rate
            
            if time.time() + wait_time > deadline:
                return False  # Would exceed timeout
            
            self._total_waits += 1
            self._total_wait_time += wait_time
            time.sleep(min(wait_time, 0.5))  # Don't sleep too long, re-check frequently
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics"""
        with self._lock:
            return {
                'total_requests': self._total_requests,
                'total_waits': self._total_waits,
                'total_wait_time': round(self._total_wait_time, 2),
                'avg_wait_time': round(self._total_wait_time / self._total_waits, 3) if self._total_waits > 0 else 0
            }


# Global rate limiter shared by all workers
# 10 workers at 3 req/sec = 30 req/sec burst, 3 req/sec sustained
# Conservative for portal - each worker effectively gets 0.3 req/sec sustained
_global_rate_limiter = RateLimiter(requests_per_second=4.0, burst_size=24)


def _rate_gate(context: str = '') -> bool:
    """v12.2 (audit H1): honor the rate limiter's verdict.

    Previously every call site discarded acquire()'s return value — on a 30s
    token timeout the request proceeded anyway, so the limiter never actually
    limited under exactly the contention it exists for. This gate retries the
    acquire up to 4×30s, logging each starve; only after ~120s of continuous
    starvation does it let the request through (with a loud warning) so a
    limiter pathology can never deadlock a worker.
    Returns True if a token was obtained, False if it gave up waiting.
    """
    for attempt in range(1, 5):
        if _global_rate_limiter.acquire(timeout=30.0):
            return True
        logger.warning(
            f"Rate limiter starved {attempt * 30}s"
            + (f" [{context}]" if context else "")
            + " — portal request pressure exceeds sustainable rate"
        )
    logger.warning("Rate limiter starved 120s — proceeding WITHOUT token (avoiding worker deadlock)")
    return False

def rate_limited_request(func):
    """Decorator to rate limit portal requests"""
    def wrapper(*args, **kwargs):
        _rate_gate()
        return func(*args, **kwargs)
    return wrapper

# ═══════════════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,  # Back to INFO now that debugging is done
    format='%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('POWER-BHOOMI')

# Initialize global portal health manager now that logger is available
portal_health = PortalHealthManager()

# ═══════════════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════════════

@dataclass
class LandRecord:
    """Represents a single land record"""
    district: str
    taluk: str
    hobli: str
    village: str
    survey_no: int
    surnoc: str
    hissa: str
    period: str
    owner_name: str
    extent: str
    # v12.2 (audit C2): position of this owner row within the parcel's result
    # table — makes identical co-owner rows distinct records.
    owner_seq: int = 0
    # 🔧 v7.0: Removed khatah - portal no longer has Khata column
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    worker_id: int = 0

@dataclass
class WorkerStatus:
    """Status of a single worker"""
    worker_id: int
    status: str = 'idle'  # idle, running, completed, failed
    current_village: str = ''
    current_survey: int = 0
    max_survey: int = 0
    villages_completed: int = 0
    villages_total: int = 0
    records_found: int = 0
    matches_found: int = 0
    errors: int = 0
    last_update: str = field(default_factory=lambda: datetime.now().isoformat())
    # v12: monotonic heartbeat (time.time()) stamped on every status update AND every log
    # line. The watchdog compares this against Config.STALL_TIMEOUT_SECONDS to detect a
    # wedged worker. Uses a raw float (not isoformat) so comparison is cheap and TZ-free.
    last_heartbeat: float = field(default_factory=lambda: time.time())

@dataclass
class SearchState:
    """Global search state"""
    running: bool = False
    completed: bool = False
    start_time: str = ''
    owner_name: str = ''
    owner_variants: List[str] = field(default_factory=list)
    
    # Aggregate stats
    total_workers: int = Config.MAX_WORKERS
    active_workers: int = 0
    total_villages: int = 0
    villages_completed: int = 0
    total_records: int = 0
    total_matches: int = 0
    
    # Village tracking - BULLETPROOF: Track every village
    villages_all: List[str] = field(default_factory=list)  # All villages to search
    villages_processed: List[str] = field(default_factory=list)  # Successfully processed
    villages_retried: List[str] = field(default_factory=list)  # Had to retry (session expiry)
    villages_failed: List[str] = field(default_factory=list)  # Failed after retries
    session_recoveries: int = 0  # Count of session recovery attempts
    
    # Accuracy tracking
    total_periods_processed: int = 0  # Track ALL periods processed
    skipped_items: List[Dict] = field(default_factory=list)  # Items that couldn't be processed
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # SMART STOP TRACKING - For user confidence and accuracy reporting
    # ═══════════════════════════════════════════════════════════════════════════════════
    skipped_surveys: List[Dict] = field(default_factory=list)  # Surveys skipped due to portal errors
    village_stats: Dict[str, Dict] = field(default_factory=dict)  # Per-village completion stats
    smart_stops: int = 0  # Count of villages stopped early via smart stop
    surveys_saved: int = 0  # Total surveys saved by smart stop (time savings metric)
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # PHASE 2 TRACKING - Auto-retry state for skipped surveys
    # ═══════════════════════════════════════════════════════════════════════════════════
    current_phase: str = 'idle'           # idle | phase1 | phase1_done | phase2 | completed
    phase1_completed_at: str = ''
    phase2_started_at: str = ''
    phase2_completed_at: str = ''
    phase2_total: int = 0                 # Total items queued for Phase 2
    phase2_attempted: int = 0             # Items attempted so far
    phase2_recovered: int = 0             # Items successfully recovered
    phase2_failed: int = 0               # Items that failed again
    phase2_records_added: int = 0         # New records from Phase 2
    
    # Worker details
    workers: Dict[int, WorkerStatus] = field(default_factory=dict)
    
    # Logs
    logs: List[str] = field(default_factory=list)
    
    # File paths
    all_records_file: str = ''
    matches_file: str = ''
    # v10.0: Standalone Phase-1 snapshot files (DB-exported, not writer-dependent)
    phase1_records_file: str = ''
    phase1_matches_file: str = ''
    
    # Real-time records storage (for UI display)
    all_records: List[Dict] = field(default_factory=list)
    matches: List[Dict] = field(default_factory=list)
    
    # v10.0: Track no-owner false-skip recoveries
    no_owner_recovered: int = 0

    # v12: villages a stalled/failed worker never finished, as (code, name, hobli_code,
    # hobli_name) tuples. Collected by the watchdog; re-enumerated in a coverage pass
    # after Phase 1 so a wedged worker no longer silently drops its remaining villages.
    unfinished_villages: List[Tuple] = field(default_factory=list)
    coverage_villages_done: int = 0   # how many unfinished villages the coverage pass cleared
    stalled_workers: List[int] = field(default_factory=list)  # worker ids the watchdog failed

# ═══════════════════════════════════════════════════════════════════════════════════════
# BUFFERED THREAD-SAFE CSV WRITER
# ═══════════════════════════════════════════════════════════════════════════════════════

class ThreadSafeCSVWriter:
    """
    Buffered thread-safe CSV writer for parallel access.
    
    Improvements:
    - Buffers writes to reduce file I/O operations by 90%
    - Auto-flushes based on time or buffer size
    - Keeps file handle open for faster writes
    - Guarantees data persistence on flush/close
    """
    
    BUFFER_SIZE = 50  # Flush after this many records
    FLUSH_INTERVAL = 5.0  # Flush after this many seconds
    
    def __init__(self, filepath: str, fieldnames: List[str]):
        self.filepath = filepath
        self.fieldnames = fieldnames
        self.lock = threading.Lock()
        self._initialized = False
        
        # Buffering
        self._buffer: List[Dict[str, Any]] = []
        self._last_flush = time.time()
        self._file = None
        self._writer = None
        
        # Background flusher
        self._stop_flusher = threading.Event()
        self._flusher_thread = threading.Thread(target=self._auto_flush_loop, daemon=True)
        self._flusher_thread.start()
    
    def _initialize(self):
        """Initialize CSV file with headers and keep handle open"""
        if not self._initialized:
            self._file = open(self.filepath, 'w', newline='', encoding='utf-8')
            self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames)
            self._writer.writeheader()
            self._file.flush()
            self._initialized = True
    
    def _auto_flush_loop(self):
        """Background thread to auto-flush buffer periodically"""
        while not self._stop_flusher.wait(timeout=1.0):
            with self.lock:
                if self._buffer and (time.time() - self._last_flush) >= self.FLUSH_INTERVAL:
                    self._flush_buffer_internal()
    
    def _flush_buffer_internal(self):
        """Internal flush - must be called with lock held"""
        if self._buffer and self._writer:
            try:
                self._writer.writerows(self._buffer)
                self._file.flush()
                self._buffer.clear()
                self._last_flush = time.time()
            except Exception as e:
                logger.error(f"CSV flush error: {e}")
    
    def write_record(self, record: Dict[str, Any]):
        """Write a single record to buffer (thread-safe)"""
        with self.lock:
            if not self._initialized:
                self._initialize()
            
            self._buffer.append(record)
            
            # Flush if buffer is full
            if len(self._buffer) >= self.BUFFER_SIZE:
                self._flush_buffer_internal()
    
    def write_records(self, records: List[Dict[str, Any]]):
        """Write multiple records to buffer (thread-safe)"""
        with self.lock:
            if not self._initialized:
                self._initialize()
            
            self._buffer.extend(records)
            
            # Flush if buffer is full
            if len(self._buffer) >= self.BUFFER_SIZE:
                self._flush_buffer_internal()
    
    def flush(self):
        """Force flush buffer to disk"""
        with self.lock:
            self._flush_buffer_internal()
    
    def close(self):
        """Close writer and flush remaining data"""
        self._stop_flusher.set()
        self._flusher_thread.join(timeout=2.0)
        
        with self.lock:
            self._flush_buffer_internal()
            if self._file:
                try:
                    self._file.close()
                except Exception:
                    pass
                self._file = None
                self._writer = None
    
    def __del__(self):
        """Ensure data is flushed on garbage collection"""
        try:
            self.close()
        except Exception:
            pass

# ═══════════════════════════════════════════════════════════════════════════════════════
# DATABASE CONNECTION POOL
# ═══════════════════════════════════════════════════════════════════════════════════════

import sqlite3
from contextlib import contextmanager

class ConnectionPool:
    """
    Thread-safe SQLite connection pool to eliminate connection creation overhead.
    Maintains a pool of reusable connections for better performance under high concurrency.
    """
    
    def __init__(self, db_path: str, pool_size: int = 10, timeout: float = 30.0):
        self.db_path = db_path
        self.pool_size = pool_size
        self.timeout = timeout
        self._pool = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created = 0
        
        # Pre-create some connections
        for _ in range(min(3, pool_size)):
            conn = self._create_connection()
            self._pool.put(conn)
            self._created += 1
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new optimized SQLite connection"""
        conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # WAL mode for better concurrency (allows readers while writing)
        conn.execute("PRAGMA journal_mode=WAL")
        # Normal sync is safe enough with WAL and much faster
        conn.execute("PRAGMA synchronous=NORMAL")
        # Larger cache for better read performance
        conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        # Memory-mapped I/O for faster reads
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB
        # Temp tables in memory
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn
    
    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool.
        Creates new connection if pool is empty and under limit.
        """
        conn = None
        try:
            # Try to get from pool (non-blocking)
            try:
                conn = self._pool.get_nowait()
            except queue.Empty:
                # Pool empty - create new if under limit
                with self._lock:
                    if self._created < self.pool_size:
                        conn = self._create_connection()
                        self._created += 1
                    else:
                        # Wait for a connection to be returned
                        conn = self._pool.get(timeout=self.timeout)
            
            yield conn
            conn.commit()
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise e
        finally:
            # Return connection to pool
            if conn:
                try:
                    # Check if connection is still valid
                    conn.execute("SELECT 1")
                    self._pool.put_nowait(conn)
                except Exception:
                    # Connection is dead, decrement count
                    with self._lock:
                        self._created -= 1
    
    def close_all(self):
        """Close all pooled connections"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break
            except Exception:
                pass
        with self._lock:
            self._created = 0


# ═══════════════════════════════════════════════════════════════════════════════════════
# PERSISTENT DATABASE MANAGER (SQLite)
# ═══════════════════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    """
    Thread-safe SQLite database manager for persistent storage.
    
    Features:
    - Real-time saving of every record (no data loss on crash)
    - Search session tracking with resume capability
    - Village/survey progress tracking
    - Export to CSV functionality
    - Search history with statistics
    
    Database is stored in user's Documents/POWER-BHOOMI folder.
    """
    
    # Database version for migrations
    DB_VERSION = 1
    
    def __init__(self, db_path: str = None, pool_size: int = None):
        """Initialize database manager with optional custom path and connection pool"""
        if db_path is None:
            # Default: Documents/POWER-BHOOMI/bhoomi_data.db
            import platform
            if platform.system() == 'Windows':
                docs_folder = os.path.join(os.environ.get('USERPROFILE', ''), 'Documents')
            else:
                docs_folder = os.path.expanduser('~/Documents')
            
            self.db_folder = os.path.join(docs_folder, 'POWER-BHOOMI')
            os.makedirs(self.db_folder, exist_ok=True)
            self.db_path = os.path.join(self.db_folder, 'bhoomi_data.db')
        else:
            self.db_path = db_path
            self.db_folder = os.path.dirname(db_path)
        
        self.lock = threading.Lock()
        
        # v10.0: Initialize connection pool (size = workers + 8 for overhead:
        # main + Flask + health monitor + Phase 2 worker + UI status polls + margin)
        pool_size = pool_size or (Config.MAX_WORKERS + 8)
        self._pool = ConnectionPool(self.db_path, pool_size=pool_size)
        
        self._init_database()
        logger.info(f"📁 Database initialized with pool size {pool_size}: {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool (thread-safe)"""
        with self._pool.get_connection() as conn:
            yield conn
    
    def close(self):
        """Close all database connections"""
        self._pool.close_all()
    
    def _init_database(self):
        """Initialize database schema"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Search Sessions Table - Track each search operation
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS search_sessions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT UNIQUE NOT NULL,
                        owner_name TEXT NOT NULL,
                        owner_variants TEXT,
                        district_code TEXT,
                        district_name TEXT,
                        taluk_code TEXT,
                        taluk_name TEXT,
                        hobli_code TEXT,
                        hobli_name TEXT,
                        village_code TEXT,
                        village_name TEXT,
                        max_survey INTEGER DEFAULT 200,
                        status TEXT DEFAULT 'running',  -- running, completed, stopped, crashed
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        total_villages INTEGER DEFAULT 0,
                        villages_completed INTEGER DEFAULT 0,
                        total_records INTEGER DEFAULT 0,
                        total_matches INTEGER DEFAULT 0,
                        notes TEXT
                    )
                ''')
                
                # Land Records Table - All records found (REAL-TIME SAVES)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS land_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        district TEXT,
                        taluk TEXT,
                        hobli TEXT,
                        village TEXT,
                        survey_no INTEGER,
                        surnoc TEXT,
                        hissa TEXT,
                        period TEXT,
                        owner_name TEXT,
                        extent TEXT,
                        owner_seq INTEGER DEFAULT 0,
                        is_match INTEGER DEFAULT 0,
                        worker_id INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id),
                        -- v12.2 (audit C1/C2): identity includes extent AND owner_seq.
                        -- extent: an owner can legitimately hold multiple extents in one
                        -- parcel — the old key silently dropped them (735 rows measured).
                        -- owner_seq: row position within the parcel's result table, so
                        -- two co-owners with identical name+extent are distinct rows,
                        -- while a re-scrape of the same page reproduces the same seqs
                        -- and still dedups cleanly via INSERT OR IGNORE.
                        UNIQUE(session_id, village, survey_no, surnoc, hissa, period, owner_name, extent, owner_seq)
                    )
                ''')
                
                # Village Progress Table - Track which villages/surveys are done
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS village_progress (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        village_code TEXT NOT NULL,
                        village_name TEXT NOT NULL,
                        hobli_code TEXT,
                        hobli_name TEXT,
                        status TEXT DEFAULT 'pending',  -- pending, in_progress, completed, failed
                        last_survey_no INTEGER DEFAULT 0,
                        max_survey_no INTEGER DEFAULT 200,
                        records_found INTEGER DEFAULT 0,
                        matches_found INTEGER DEFAULT 0,
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        error_message TEXT,
                        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id),
                        UNIQUE(session_id, village_code)
                    )
                ''')
                
                # Create indexes for fast lookups
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_session ON land_records(session_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_village ON land_records(village)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_owner ON land_records(owner_name)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_match ON land_records(is_match)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_progress_session ON village_progress(session_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_status ON search_sessions(status)')

                # ═══════════════════════════════════════════════════════════════
                # v12.2 MIGRATION (audit C1): existing DBs created before the
                # extent/owner_seq identity fix must be rebuilt — SQLite cannot
                # alter a table-level UNIQUE in place. Runs once, at startup,
                # before any worker writes. Existing rows are preserved verbatim
                # (owner_seq backfilled as 0).
                # ═══════════════════════════════════════════════════════════════
                row = cursor.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='land_records'"
                ).fetchone()
                if row and 'owner_seq' not in row[0]:
                    logger.warning("v12.2 migration: rebuilding land_records with extent+owner_seq identity...")
                    cursor.execute("ALTER TABLE land_records RENAME TO land_records_old")
                    cursor.execute('''
                        CREATE TABLE land_records (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            session_id TEXT NOT NULL,
                            district TEXT, taluk TEXT, hobli TEXT, village TEXT,
                            survey_no INTEGER, surnoc TEXT, hissa TEXT, period TEXT,
                            owner_name TEXT, extent TEXT,
                            owner_seq INTEGER DEFAULT 0,
                            is_match INTEGER DEFAULT 0,
                            worker_id INTEGER DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (session_id) REFERENCES search_sessions(session_id),
                            UNIQUE(session_id, village, survey_no, surnoc, hissa, period, owner_name, extent, owner_seq)
                        )''')
                    cursor.execute('''
                        INSERT OR IGNORE INTO land_records
                            (id, session_id, district, taluk, hobli, village, survey_no,
                             surnoc, hissa, period, owner_name, extent, owner_seq,
                             is_match, worker_id, created_at)
                        SELECT id, session_id, district, taluk, hobli, village, survey_no,
                               surnoc, hissa, period, owner_name, extent, 0,
                               is_match, worker_id, created_at
                        FROM land_records_old''')
                    migrated = cursor.execute("SELECT COUNT(*) FROM land_records").fetchone()[0]
                    cursor.execute("DROP TABLE land_records_old")
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_session ON land_records(session_id)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_village ON land_records(village)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_owner ON land_records(owner_name)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_match ON land_records(is_match)')
                    logger.warning(f"v12.2 migration complete: {migrated} rows preserved")
                
                # Skipped Items Table - For 100% accuracy retry
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS skipped_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        village_name TEXT,
                        survey_no INTEGER,
                        surnoc TEXT,
                        hissa TEXT,
                        period TEXT,
                        error_message TEXT,
                        retry_count INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'pending',  -- pending, retried, success, failed
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id)
                    )
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_skipped_session ON skipped_items(session_id)')
                
                # Survey Checkpoint Table - For granular resume capability
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS survey_checkpoints (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        village_code TEXT NOT NULL,
                        survey_no INTEGER NOT NULL,
                        surnoc_processed TEXT,  -- JSON list of processed surnocs
                        completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id),
                        UNIQUE(session_id, village_code, survey_no)
                    )
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_checkpoint_session_village ON survey_checkpoints(session_id, village_code)')
                
                # ═══════════════════════════════════════════════════════════════════
                # v11.0 EVENT LOGS — every UI log line persisted with structure
                # ═══════════════════════════════════════════════════════════════════
                # Solves the "close browser, lose context" problem. Every log entry
                # is queried by `since_id` for incremental UI updates, and survives
                # server restarts. Structured fields enable filtering by level/worker
                # /village without parsing emoji-prefixed strings.
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS event_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        level TEXT DEFAULT 'INFO',           -- DEBUG/INFO/WARN/ERROR/MATCH/PHASE
                        worker_id INTEGER,                   -- nullable
                        village TEXT,                        -- nullable
                        kind TEXT,                           -- e.g. 'progress','retry','skip','match'
                        message TEXT NOT NULL,
                        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id)
                    )
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_logs_session_id ON event_logs(session_id, id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_logs_level ON event_logs(session_id, level)')
                
                # Version tracking
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS db_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                ''')
                cursor.execute('INSERT OR REPLACE INTO db_meta (key, value) VALUES (?, ?)', 
                              ('version', str(self.DB_VERSION)))
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # SESSION MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def create_session(self, params: dict) -> str:
        """Create a new search session and return session_id"""
        import uuid
        session_id = f"search_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO search_sessions (
                        session_id, owner_name, owner_variants,
                        district_code, district_name, taluk_code, taluk_name,
                        hobli_code, hobli_name, village_code, village_name,
                        max_survey, total_villages
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    session_id,
                    params.get('owner_name', ''),
                    json.dumps(params.get('owner_variants', [])),
                    params.get('district_code', ''),
                    params.get('district_name', ''),
                    params.get('taluk_code', ''),
                    params.get('taluk_name', ''),
                    params.get('hobli_code', ''),
                    params.get('hobli_name', ''),
                    params.get('village_code', ''),
                    params.get('village_name', ''),
                    params.get('max_survey', 200),
                    params.get('total_villages', 0)
                ))
        
        logger.info(f"📝 Created session: {session_id}")
        return session_id
    
    def update_session_status(self, session_id: str, status: str, **kwargs):
        """Update session status and optional fields"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                updates = ['status = ?']
                values = [status]
                
                if status in ('completed', 'stopped'):
                    updates.append('completed_at = CURRENT_TIMESTAMP')
                
                for key, value in kwargs.items():
                    if key in ('villages_completed', 'total_records', 'total_matches', 'notes', 'total_villages'):
                        updates.append(f'{key} = ?')
                        values.append(value)
                
                values.append(session_id)
                cursor.execute(f'''
                    UPDATE search_sessions SET {', '.join(updates)} WHERE session_id = ?
                ''', values)
    
    def get_session(self, session_id: str) -> Optional[dict]:
        """Get session details"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM search_sessions WHERE session_id = ?', (session_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_recent_sessions(self, limit: int = 20) -> List[dict]:
        """Get recent search sessions"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM search_sessions 
                ORDER BY started_at DESC 
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_resumable_sessions(self) -> List[dict]:
        """Get sessions that can be resumed (running or crashed)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT s.*, 
                       (SELECT COUNT(*) FROM village_progress WHERE session_id = s.session_id AND status = 'pending') as pending_villages,
                       (SELECT COUNT(*) FROM village_progress WHERE session_id = s.session_id AND status = 'completed') as done_villages
                FROM search_sessions s
                WHERE s.status IN ('running', 'crashed')
                ORDER BY s.started_at DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # VILLAGE PROGRESS TRACKING
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def register_villages(self, session_id: str, villages: List[tuple]):
        """Register all villages for a session (for resume tracking)"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for village_code, village_name, hobli_code, hobli_name in villages:
                    cursor.execute('''
                        INSERT OR IGNORE INTO village_progress 
                        (session_id, village_code, village_name, hobli_code, hobli_name)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (session_id, village_code, village_name, hobli_code, hobli_name))
    
    def start_village(self, session_id: str, village_code: str, max_survey: int = 200):
        """Mark village as in_progress"""
        # Validate max_survey
        if not max_survey or max_survey <= 0:
            max_survey = 200
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE village_progress
                    SET status = 'in_progress', started_at = CURRENT_TIMESTAMP, max_survey_no = ?
                    WHERE session_id = ? AND village_code = ?
                ''', (max_survey, session_id, village_code))
    
    def update_village_progress(self, session_id: str, village_code: str, last_survey: int, records: int = 0, matches: int = 0):
        """Update village progress (call periodically during search)"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE village_progress 
                    SET last_survey_no = ?, records_found = records_found + ?, matches_found = matches_found + ?
                    WHERE session_id = ? AND village_code = ?
                ''', (last_survey, records, matches, session_id, village_code))
    
    def complete_village(self, session_id: str, village_code: str, records: int, matches: int):
        """Mark village as completed"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE village_progress 
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                        records_found = ?, matches_found = ?
                    WHERE session_id = ? AND village_code = ?
                ''', (records, matches, session_id, village_code))
    
    def fail_village(self, session_id: str, village_code: str, error: str):
        """Mark village as failed"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE village_progress 
                    SET status = 'failed', error_message = ?
                    WHERE session_id = ? AND village_code = ?
                ''', (error, session_id, village_code))
    
    def get_pending_villages(self, session_id: str) -> List[dict]:
        """Get villages that still need to be searched (for resume)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM village_progress 
                WHERE session_id = ? AND status IN ('pending', 'in_progress', 'failed')
                ORDER BY id
            ''', (session_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # RECORD MANAGEMENT (REAL-TIME SAVES)
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def save_record(self, session_id: str, record: dict, is_match: bool = False) -> int:
        """Save a single record immediately (thread-safe, real-time)"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.lock:
                    with self.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT OR IGNORE INTO land_records (
                                session_id, district, taluk, hobli, village,
                                survey_no, surnoc, hissa, period,
                                owner_name, extent, owner_seq, is_match, worker_id
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            session_id,
                            record.get('district', ''),
                            record.get('taluk', ''),
                            record.get('hobli', ''),
                            record.get('village', ''),
                            record.get('survey_no', 0),
                            record.get('surnoc', ''),
                            record.get('hissa', ''),
                            record.get('period', ''),
                            record.get('owner_name', ''),
                            record.get('extent', ''),
                            record.get('owner_seq', 0),
                            1 if is_match else 0,
                            record.get('worker_id', 0)
                        ))
                        return cursor.lastrowid
            except sqlite3.OperationalError as e:
                if 'locked' in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"DB locked, retrying ({attempt + 1}/{max_retries})...")
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Database save failed after {max_retries} attempts: {e}")
                    raise
            except Exception as e:
                logger.error(f"Database save error: {e}")
                raise
        return -1
    
    def save_records_batch(self, session_id: str, records: List[dict], matches: List[bool] = None):
        """Save multiple records in a single transaction (faster for batch)"""
        if not records:
            return
        
        if matches is None:
            matches = [False] * len(records)
        
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT OR IGNORE INTO land_records (
                        session_id, district, taluk, hobli, village,
                        survey_no, surnoc, hissa, period,
                        owner_name, extent, is_match, worker_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', [
                    (
                        session_id,
                        r.get('district', ''),
                        r.get('taluk', ''),
                        r.get('hobli', ''),
                        r.get('village', ''),
                        r.get('survey_no', 0),
                        r.get('surnoc', ''),
                        r.get('hissa', ''),
                        r.get('period', ''),
                        r.get('owner_name', ''),
                        r.get('extent', ''),
                        1 if matches[i] else 0,
                        r.get('worker_id', 0)
                    )
                    for i, r in enumerate(records)
                ])
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # SURVEY-LEVEL CHECKPOINTING - For granular resume
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def save_survey_checkpoint(self, session_id: str, village_code: str, survey_no: int, 
                               surnocs_processed: List[str] = None):
        """
        Save a checkpoint after completing a survey.
        This allows resuming from the exact survey if interrupted.
        """
        surnoc_json = json.dumps(surnocs_processed or [])
        
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO survey_checkpoints 
                    (session_id, village_code, survey_no, surnoc_processed, completed_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (session_id, village_code, survey_no, surnoc_json))
    
    def get_last_checkpoint(self, session_id: str, village_code: str) -> Optional[Dict]:
        """
        Get the last completed survey for a village in this session.
        Returns None if no checkpoint exists.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT survey_no, surnoc_processed, completed_at
                FROM survey_checkpoints
                WHERE session_id = ? AND village_code = ?
                ORDER BY survey_no DESC
                LIMIT 1
            ''', (session_id, village_code))
            
            row = cursor.fetchone()
            if row:
                return {
                    'survey_no': row['survey_no'],
                    'surnocs_processed': json.loads(row['surnoc_processed'] or '[]'),
                    'completed_at': row['completed_at']
                }
            return None
    
    def get_all_checkpoints(self, session_id: str) -> Dict[str, int]:
        """
        Get all village checkpoints for a session.
        Returns dict of village_code -> last_completed_survey_no
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT village_code, MAX(survey_no) as last_survey
                FROM survey_checkpoints
                WHERE session_id = ?
                GROUP BY village_code
            ''', (session_id,))
            
            return {row['village_code']: row['last_survey'] for row in cursor.fetchall()}
    
    def clear_checkpoints(self, session_id: str, village_code: str = None):
        """Clear checkpoints for a session (optionally only for a specific village)"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if village_code:
                    cursor.execute(
                        'DELETE FROM survey_checkpoints WHERE session_id = ? AND village_code = ?',
                        (session_id, village_code)
                    )
                else:
                    cursor.execute(
                        'DELETE FROM survey_checkpoints WHERE session_id = ?',
                        (session_id,)
                    )
    
    def get_session_records(self, session_id: str, limit: int = None, matches_only: bool = False) -> List[dict]:
        """Get records for a session"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = 'SELECT * FROM land_records WHERE session_id = ?'
            params = [session_id]
            
            if matches_only:
                query += ' AND is_match = 1'
            
            query += ' ORDER BY id DESC'
            
            if limit:
                query += ' LIMIT ?'
                params.append(limit)
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_session_stats(self, session_id: str) -> dict:
        """Get statistics for a session"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_records,
                    SUM(is_match) as total_matches,
                    COUNT(DISTINCT village) as villages_with_records
                FROM land_records WHERE session_id = ?
            ''', (session_id,))
            
            row = cursor.fetchone()
            return {
                'total_records': row['total_records'] or 0,
                'total_matches': row['total_matches'] or 0,
                'villages_with_records': row['villages_with_records'] or 0
            }
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # v11.0 EVENT LOG PERSISTENCE — solves "close browser, lose state"
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def save_event_log(
        self,
        session_id: str,
        message: str,
        level: str = 'INFO',
        worker_id: Optional[int] = None,
        village: Optional[str] = None,
        kind: Optional[str] = None,
    ) -> Optional[int]:
        """
        Persist a single log entry. Returns the auto-incremented row id (used by UI
        for incremental polling via `since_id`). Returns None on failure.
        Designed for high-frequency calls (one per worker log line); uses the
        connection pool for low overhead.
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute('''
                    INSERT INTO event_logs (session_id, level, worker_id, village, kind, message)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (session_id, level, worker_id, village, kind, message[:2000]))
                return cur.lastrowid
        except Exception as e:
            logger.debug(f"save_event_log failed: {e}")
            return None
    
    def get_logs_after(
        self,
        session_id: str,
        since_id: int = 0,
        limit: int = 500,
        level_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch logs for a session whose id > since_id. Used by the UI's incremental
        polling. The UI sends back the highest id it's seen, gets only newer rows.
        Returns at most `limit` rows ordered by id ascending.
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                if level_filter:
                    cur.execute('''
                        SELECT id, ts, level, worker_id, village, kind, message
                        FROM event_logs
                        WHERE session_id = ? AND id > ? AND level = ?
                        ORDER BY id ASC
                        LIMIT ?
                    ''', (session_id, since_id, level_filter, limit))
                else:
                    cur.execute('''
                        SELECT id, ts, level, worker_id, village, kind, message
                        FROM event_logs
                        WHERE session_id = ? AND id > ?
                        ORDER BY id ASC
                        LIMIT ?
                    ''', (session_id, since_id, limit))
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"get_logs_after failed: {e}")
            return []
    
    def count_logs(self, session_id: str) -> int:
        """Total log count for a session (for UI display 'showing N of M')."""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute('SELECT COUNT(*) FROM event_logs WHERE session_id = ?', (session_id,))
                return cur.fetchone()[0]
        except Exception:
            return 0
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # v11.0 RECORD PAGINATION — UI loads any chunk on demand
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def get_records_paginated(
        self,
        session_id: str,
        offset: int = 0,
        limit: int = 200,
        matches_only: bool = False,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Return (records, total_count). The total_count is how many records this
        session has total — UI uses it for pagination math.
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                where_match = 'AND is_match = 1' if matches_only else ''
                cur.execute(f'''
                    SELECT COUNT(*) FROM land_records WHERE session_id = ? {where_match}
                ''', (session_id,))
                total = cur.fetchone()[0]
                
                cur.execute(f'''
                    SELECT id, district, taluk, hobli, village, survey_no, surnoc, hissa,
                           period, owner_name, extent, is_match, worker_id, created_at
                    FROM land_records
                    WHERE session_id = ? {where_match}
                    ORDER BY id DESC
                    LIMIT ? OFFSET ?
                ''', (session_id, limit, offset))
                rows = [dict(r) for r in cur.fetchall()]
                return rows, total
        except Exception as e:
            logger.debug(f"get_records_paginated failed: {e}")
            return [], 0
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # v11.0 STARTUP HOUSEKEEPING — mark stale 'running' sessions as interrupted
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def mark_interrupted_sessions(self) -> int:
        """
        Called once at server startup. Any session still flagged 'running' from a
        previous process is logically dead — flip it to 'interrupted' so the UI
        doesn't lie about it being live. Returns count of sessions reset.
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute('''
                    UPDATE search_sessions
                    SET status = 'interrupted',
                        completed_at = CURRENT_TIMESTAMP,
                        notes = COALESCE(notes, '') || ' [auto-marked interrupted on server restart]'
                    WHERE status = 'running'
                ''')
                return cur.rowcount
        except Exception as e:
            logger.warning(f"mark_interrupted_sessions failed: {e}")
            return 0
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # EXPORT FUNCTIONS
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def export_to_csv(self, session_id: str, output_path: str, matches_only: bool = False) -> str:
        """Export session records to CSV file"""
        records = self.get_session_records(session_id, matches_only=matches_only)
        
        if not records:
            return None
        
        fieldnames = ['district', 'taluk', 'hobli', 'village', 'survey_no',
                      'surnoc', 'hissa', 'period', 'owner_name', 'extent', 'owner_seq', 'created_at']
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)
        
        logger.info(f"📁 Exported {len(records)} records to {output_path}")
        return output_path
    
    def get_all_records_count(self) -> int:
        """Get total records across all sessions"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM land_records')
            return cursor.fetchone()[0]
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # ACCURACY TRACKING - Skipped Items for Retry
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def save_skipped_item(self, session_id: str, village_name: str, survey_no: int, 
                          surnoc: str = '', hissa: str = '', period: str = '', error: str = ''):
        """Save a skipped item for later retry"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO skipped_items 
                    (session_id, village_name, survey_no, surnoc, hissa, period, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (session_id, village_name, survey_no, surnoc, hissa, period, error))
    
    def get_skipped_items(self, session_id: str) -> List[dict]:
        """Get all skipped items for a session"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM skipped_items 
                WHERE session_id = ? AND status = 'pending'
                ORDER BY id
            ''', (session_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_skipped_count(self, session_id: str) -> int:
        """Get count of skipped items for a session"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) FROM skipped_items 
                WHERE session_id = ? AND status = 'pending'
            ''', (session_id,))
            return cursor.fetchone()[0]
    
    def search_records(self, owner_name: str, limit: int = 100) -> List[dict]:
        """Search records by owner name across all sessions"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT r.*, s.owner_name as search_owner, s.started_at as search_date
                FROM land_records r
                JOIN search_sessions s ON r.session_id = s.session_id
                WHERE r.owner_name LIKE ?
                ORDER BY r.created_at DESC
                LIMIT ?
            ''', (f'%{owner_name}%', limit))
            return [dict(row) for row in cursor.fetchall()]


# Global database instance
db_manager: Optional[DatabaseManager] = None

def get_database() -> DatabaseManager:
    """Get or create the global database instance"""
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager()
    return db_manager


# ═══════════════════════════════════════════════════════════════════════════════════════
# BHOOMI API CLIENT
# ═══════════════════════════════════════════════════════════════════════════════════════

class BhoomiAPI:
    """Client for Karnataka Bhoomi eChawadi API"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/json; charset=utf-8',
        })
        self._cache = {}
    
    def _make_request(self, endpoint: str, data: dict = None, method: str = 'POST') -> Optional[dict]:
        """Make API request with error handling"""
        url = f"{Config.ECHAWADI_BASE}/{endpoint}"
        cache_key = f"{endpoint}:{json.dumps(data, sort_keys=True)}"
        
        # Check cache
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            if method == 'GET':
                response = self.session.get(url, verify=False, timeout=30)
            else:
                response = self.session.post(url, json=data, verify=False, timeout=30)
            
            result = response.text
            # Handle double-encoded JSON
            if result.startswith('"') and result.endswith('"'):
                result = json.loads(result)
            if isinstance(result, str):
                result = json.loads(result)
            
            # Cache result
            self._cache[cache_key] = result
            return result
            
        except Exception as e:
            logger.error(f"API Error [{endpoint}]: {e}")
            return None
    
    def get_districts(self) -> List[dict]:
        result = self._make_request('LoadDistrict', method='GET')
        if result and 'data' in result:
            return sorted(result['data'], key=lambda x: x.get('district_name_kn', ''))
        return []
    
    def get_taluks(self, district_code: int) -> List[dict]:
        result = self._make_request('LoadTaluk', {'pDistCode': str(district_code)})
        if result and 'data' in result:
            return sorted(result['data'], key=lambda x: x.get('taluka_name_kn', ''))
        return []
    
    def get_hoblis(self, district_code: int, taluk_code: int) -> List[dict]:
        result = self._make_request('LoadHobli', {
            'pDistCode': str(district_code),
            'pTalukCode': str(taluk_code)
        })
        if result and 'data' in result:
            return sorted(result['data'], key=lambda x: x.get('hobli_name_kn', ''))
        return []
    
    def get_villages(self, district_code: int, taluk_code: int, hobli_code: int) -> List[dict]:
        result = self._make_request('LoadVillage', {
            'pDistCode': str(district_code),
            'pTalukCode': str(taluk_code),
            'pHobliCode': str(hobli_code)
        })
        if result and 'data' in result:
            return sorted(result['data'], key=lambda x: x.get('village_name_kn', ''))
        return []

# ═══════════════════════════════════════════════════════════════════════════════════════
# SEARCH WORKER
# ═══════════════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════════════
# v12.1: OWNER-NAME MATCH NORMALIZATION
# ───────────────────────────────────────────────────────────────────────────────────────
# Bhoomi returns owner names in KANNADA. A search entered in English (Latin) can never
# substring-match Kannada text — this silently produced matches=0 for real hits (e.g.
# "KUMARASWAMY" vs "ಕುಮಾರಸ್ವಾಮಿ"). We also collapse internal whitespace so spacing
# variants of the SAME Kannada name match ("ಕುಮಾರ ಸ್ವಾಮಿ" == "ಕುಮಾರಸ್ವಾಮಿ").
# ═══════════════════════════════════════════════════════════════════════════════════════
def _norm_for_match(s: str) -> str:
    """Lowercase + strip ALL whitespace for script-safe substring comparison."""
    return ''.join(str(s).split()).lower()

def _is_latin_only(s: str) -> bool:
    """True if the string has letters and ALL of them are ASCII Latin (a-z)."""
    letters = [c for c in str(s) if c.isalpha()]
    return bool(letters) and all(ord(c) < 128 for c in letters)


class SearchWorker:
    """
    Individual search worker that runs in its own thread with its own browser.
    Each worker processes a subset of villages independently.
    
    NOW WITH PERSISTENT DATABASE INTEGRATION:
    - Every record saved to SQLite in real-time
    - Survives browser crashes
    - Supports resume functionality
    """
    
    def __init__(
        self,
        worker_id: int,
        search_params: dict,
        villages: List[Tuple[str, str, str, str]],  # (village_code, village_name, hobli_code, hobli_name)
        state: SearchState,
        all_records_writer: ThreadSafeCSVWriter,
        matches_writer: ThreadSafeCSVWriter,
        state_lock: threading.Lock,
        db: DatabaseManager = None,  # Persistent database
        session_id: str = None,  # Current search session ID
        skeletons: Dict[str, List[Dict]] = None,  # v11: pre-fetched Service154 skeletons
    ):
        self.worker_id = worker_id
        self.params = search_params
        self.villages = villages
        self.state = state
        self.all_records_writer = all_records_writer
        self.matches_writer = matches_writer
        self.state_lock = state_lock
        self.skeletons = skeletons or {}  # v11: {village_code: [{survey_no, surnoc, hissa, ...}]}
        
        # Database integration
        self.db = db
        self.session_id = session_id
        
        self.driver = None
        self.logger = logging.getLogger(f'Worker-{worker_id}')
        self._user_data_dir = None  # Set during browser init, used for cleanup
        
        # Worker-local stats
        self.records_found = 0
        self.matches_found = 0
        self.errors = 0
        
        # Browser stability tracking - prevents memory leaks
        self.hissa_processed_count = 0
        self.last_browser_restart = time.time()
    
    def _update_status(self, **kwargs):
        """Thread-safe status update"""
        with self.state_lock:
            worker_status = self.state.workers.get(self.worker_id)
            if worker_status:
                for key, value in kwargs.items():
                    if hasattr(worker_status, key):
                        setattr(worker_status, key, value)
                worker_status.last_update = datetime.now().isoformat()
                worker_status.last_heartbeat = time.time()  # v12: feed the stall watchdog
    
    def _add_log(self, message: str, level: str = 'INFO', kind: Optional[str] = None,
                 village: Optional[str] = None):
        """
        Thread-safe log addition. v11.0: also persisted to DB asynchronously
        so the UI can query the full backlog after a browser refresh / restart.
        
        Args:
            message: log text (display unchanged for backward compat)
            level: INFO / WARN / ERROR / MATCH / PHASE — used for filtering
            kind: tag like 'progress', 'retry', 'skip', 'match' — used for UI styling
            village: optional village name for filtering
        """
        with self.state_lock:
            log_entry = f"[W{self.worker_id}] {message}"
            self.state.logs.append(log_entry)
            # v11.0: keep last LOG_RETENTION_COUNT in memory (was hard-coded 100)
            if len(self.state.logs) > Config.LOG_RETENTION_COUNT:
                self.state.logs = self.state.logs[-Config.LOG_RETENTION_COUNT:]
            # v12: any log line is also a liveness signal — refresh the heartbeat so a
            # worker doing real work (even without a status change) is never mistaken for
            # wedged by the watchdog.
            ws = self.state.workers.get(self.worker_id)
            if ws is not None:
                ws.last_heartbeat = time.time()
        self.logger.info(message)
        
        # v11.0: persist to DB so closing/reopening the browser preserves history
        if self.db and self.session_id:
            try:
                self.db.save_event_log(
                    session_id=self.session_id,
                    message=log_entry,
                    level=level,
                    worker_id=self.worker_id,
                    village=village,
                    kind=kind,
                )
            except Exception:
                # Never let logging failures impact scraping
                pass
    
    def _update_global_stats(self):
        """Update global statistics"""
        with self.state_lock:
            # Recalculate totals from all workers
            total_records = 0
            total_matches = 0
            villages_completed = 0
            active_workers = 0
            
            for wid, ws in self.state.workers.items():
                total_records += ws.records_found
                total_matches += ws.matches_found
                villages_completed += ws.villages_completed
                if ws.status == 'running':
                    active_workers += 1
            
            self.state.total_records = total_records
            self.state.total_matches = total_matches
            self.state.villages_completed = villages_completed
            self.state.active_workers = active_workers
    
    def _calculate_village_confidence(self, surveys_checked: int, surveys_with_data: int,
                                       last_survey_with_data: int, stopped_at_survey: int,
                                       skipped_count: int, completion_reason: str,
                                       max_survey: int) -> int:
        """
        Calculate confidence score (0-100) that a village was fully searched.
        
        High confidence when:
        - Smart stop after many consecutive empties
        - Last data found far from stop point
        - Low skip rate
        - All expected surveys checked
        
        Returns:
            Confidence score 0-100
        """
        confidence = 100
        
        # Factor 1: Skip rate penalty (max -30 points)
        if surveys_checked > 0:
            skip_rate = skipped_count / surveys_checked
            if skip_rate > 0.2:  # >20% skipped
                confidence -= 30
            elif skip_rate > 0.1:  # >10% skipped
                confidence -= 15
            elif skip_rate > 0.05:  # >5% skipped
                confidence -= 5
        
        # Factor 2: Gap between last data and stop point (max -20 points)
        if last_survey_with_data > 0:
            gap = stopped_at_survey - last_survey_with_data
            if gap < 20:  # Stopped very close to last data
                confidence -= 20
            elif gap < 35:
                confidence -= 10
            elif gap < 50:
                confidence -= 5
            # >= 50 gap is good (full buffer used)
        
        # Factor 3: Completion reason bonus/penalty
        if completion_reason == 'smart_stop':
            # Smart stop is reliable if gap is good
            if last_survey_with_data > 0 and (stopped_at_survey - last_survey_with_data) >= 50:
                confidence += 5  # Bonus for clean smart stop
        elif completion_reason == 'max_reached':
            # Reached max survey - might have more data
            confidence -= 10
        elif completion_reason == 'error':
            confidence -= 25
        
        # Factor 4: Data density (penalty if suspiciously sparse)
        if surveys_checked > 50 and surveys_with_data < 3:
            confidence -= 10  # Very sparse data might indicate issues
        
        # Factor 5: Bonus for finding substantial data
        if surveys_with_data > 10:
            confidence += 5
        
        return max(0, min(100, confidence))
    
    def _init_browser(self, retry_count: int = 3):
        """Initialize Playwright browser"""
        from playwright.sync_api import sync_playwright
        
        last_error = None
        for attempt in range(retry_count):
            try:
                self.playwright = sync_playwright().start()
                self.browser = self.playwright.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-gpu',
                        '--disable-images',
                        '--disable-dev-shm-usage',
                        # v9.1 FIX: Bypass SSL/TLS errors. The Bhoomi portal's cert
                        # frequently expires/has issues — without these flags every
                        # navigation fails with net::err_cert_date_invalid etc.
                        '--ignore-certificate-errors',
                        '--ignore-ssl-errors',
                        '--allow-insecure-localhost',
                    ]
                )
                # v9.1 FIX: ignore_https_errors=True at the context level is the
                # canonical Playwright way to bypass cert errors.
                self.context = self.browser.new_context(ignore_https_errors=True)
                self.page = self.context.new_page()
                self.page.set_default_timeout(Config.PAGE_LOAD_TIMEOUT * 1000)
                
                # Store for cleanup
                self.driver = self.page  # Compatibility layer
                
                self._add_log(f"✅ Worker {self.worker_id} browser ready (Playwright)!")
                return
                
            except Exception as e:
                last_error = e
                self._add_log(f"Browser init failed (attempt {attempt + 1}): {str(e)[:50]}")
                time.sleep(1)
                
                try:
                    if hasattr(self, 'browser') and self.browser:
                        self.browser.close()
                    if hasattr(self, 'playwright') and self.playwright:
                        self.playwright.stop()
                except Exception:
                    pass
                self.browser = None
                self.page = None
                self.playwright = None
        
        raise Exception(f"Failed to initialize browser after {retry_count} attempts: {last_error}")
    
    def _close_browser(self):
        """Cleanup Playwright browser"""
        try:
            if self.page:
                self.page.close()
            if getattr(self, 'context', None):
                try:
                    self.context.close()
                except Exception:
                    pass
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            self._add_log(f"✓ Browser closed")
        except Exception as e:
            self.logger.debug(f"Browser close error: {e}")
        finally:
            self.driver = None
            self.page = None
            self.context = None
            self.browser = None
            self.playwright = None
        time.sleep(0.5)
    
    def _handle_alert(self) -> tuple:
        """Handle alerts/dialogs (Playwright version)"""
        try:
            # Check for visible alert elements
            page_content = self.page.content()
            
            # v12.2 (audit H3): 'please try again' removed — it is generic UI
            # boilerplate that can occur on healthy pages; a false hit here burns
            # a full 5-retry backoff ladder per survey. The remaining phrases are
            # portal-outage specific ('try after some time' covers the real
            # Kannada-portal error message this list was built for).
            portal_issues = [
                'facing some issues',
                'try after some time',
                'currently facing',
                'service unavailable',
                'server error',
                'technical difficulties',
                'contact bhoomi'
            ]
            
            # Check page content for alert messages
            page_lower = page_content.lower()
            for issue in portal_issues:
                if issue in page_lower:
                    self._add_log(f"⚠️ Portal issue detected")
                    return (True, issue, True)
            
            return (False, '', False)
                
        except Exception as e:
            self.logger.warning(f"Alert check error: {e}")
            return (False, '', False)
    
    def _wait_for_portal_recovery(self, max_wait: int = 30) -> bool:
        """
        Wait for portal to recover from issues.
        Returns True if recovered, False if still having issues.
        """
        self._add_log(f"⏳ Waiting for portal to recover (max {max_wait}s)...")
        
        for attempt in range(max_wait // 5):
            time.sleep(5)
            
            # Try to access the portal (Playwright)
            try:
                self.page.goto(Config.SERVICE2_URL)
                self.page.wait_for_load_state('domcontentloaded')
                time.sleep(2)
                
                # Check for alerts
                had_alert, alert_text, is_portal_issue = self._handle_alert()
                
                if not had_alert or not is_portal_issue:
                    self._add_log(f"✅ Portal recovered after {(attempt + 1) * 5}s")
                    return True
                    
            except Exception as e:
                pass  # Keep waiting
        
        self._add_log(f"❌ Portal still having issues after {max_wait}s")
        return False
    
    def _is_session_expired(self, page_source: str = None) -> bool:
        """
        Detect if the Bhoomi portal session has expired.
        Returns True if session expired, False otherwise.
        
        Also handles any alerts that might be blocking.
        """
        try:
            # First, handle any pending alerts
            had_alert, alert_text, is_portal_issue = self._handle_alert()
            
            if is_portal_issue:
                # Portal is having issues - wait and retry
                if self._wait_for_portal_recovery(30):
                    return False  # Recovered, session is OK
                else:
                    return True  # Still having issues, treat as session issue
            
            if page_source is None:
                page_source = self.page.content()
            
            # Check for session expiry messages
            session_expired_indicators = [
                'session expired',
                'please login again',
                'session timeout',
                'your session has expired',
                'login again',
                'session has been terminated',
            ]
            
            page_lower = page_source.lower()
            for indicator in session_expired_indicators:
                if indicator in page_lower:
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Session check error: {e}")
            # Handle the case where error is due to alert
            if 'unexpected alert' in str(e).lower():
                self._handle_alert()
            return False
    
    def _refresh_session(self) -> bool:
        """Refresh session (Playwright version)"""
        self._add_log(f"🔄 Refreshing session...")
        try:
            self.page.context.clear_cookies()
            self.page.goto(Config.SERVICE2_URL)
            self.page.wait_for_load_state('domcontentloaded')
            time.sleep(Config.SESSION_REFRESH_WAIT)
            
            if not self._is_session_expired():
                self._add_log(f"✅ Session refreshed")
                return True
            else:
                self._add_log(f"⚠️ Session still expired")
                return False
        except Exception as e:
            self.logger.error(f"Refresh error: {e}")
            return False
    
    def _beat(self):
        """v12.1: lock-free heartbeat stamp from portal-op primitives.

        Live evidence (W18, 2026-09-21): a hissa-recovery chain went 17 minutes without
        a single _add_log/_update_status call while working normally — the watchdog
        false-reaped it at 15 min idle, then it resumed as a 'failed' zombie. Every
        portal primitive now beats, so silent-but-working never trips the watchdog while
        a true greenlet wedge (no primitive ever returns) still does. Plain attribute
        assignment is GIL-atomic; the watchdog only reads — no lock needed.
        """
        ws = self.state.workers.get(self.worker_id)
        if ws is not None:
            ws.last_heartbeat = time.time()

    def _pw_select(self, selector_id, value):
        """Helper: Select dropdown by value"""
        self._beat()
        self.page.select_option(f'#{selector_id}', value=value)

    def _pw_select_text(self, selector_id, text):
        """Helper: Select dropdown by visible text"""
        self._beat()
        try:
            # Use force=True to bypass actionability checks (portal may show as disabled but still works)
            self.page.select_option(f'#{selector_id}', label=text, force=True, timeout=5000)
        except Exception as e:
            self.logger.debug(f"Error selecting '{text[:50]}' in {selector_id}: {str(e)[:100]}")
            raise
    
    def _safe_select_with_recovery(
        self,
        dropdown_id: str,
        target_value: str,
        survey_no: Optional[int] = None,
        re_select_chain: Optional[Dict[str, str]] = None,
        max_retries: int = 3,
    ) -> bool:
        """
        v11.0: Robust dropdown selection with progressive recovery.
        
        Why this exists: Service2's WebForms dropdowns are AJAX-driven and can
        race under 24-worker concurrency. A single `select_option` timeout was
        causing 1,172 false-skip "Surnoc error: timeout" failures in production.
        
        Recovery strategy (per attempt):
            1. Verify option is currently in dropdown (poll up to 3s)
            2. If present, attempt select with 5s timeout
            3. On failure, re-issue GO click to refresh dropdown state
            4. On second failure, full re-navigation chain (re-select location)
            5. After max_retries, return False (caller decides what to do)
        
        Returns True if selection succeeded, False if all attempts exhausted.
        Never raises — caller can rely on bool return for control flow.
        
        Args:
            dropdown_id: HTML id of the <select> element
            target_value: visible text we want to pick
            survey_no: if set, attempt 2 will re-issue GO with this survey number
            re_select_chain: optional {dropdown_id: value} map to re-select for
                             attempt 3 (e.g., {district_id: "21", taluk_id: "3"})
            max_retries: total attempts (default 3)
        """
        IDS = Config.ELEMENT_IDS
        last_error: Optional[str] = None
        
        for attempt in range(1, max_retries + 1):
            try:
                # Phase A: verify option is actually in the dropdown right now
                # (cheap — bounded by 3s poll, returns immediately if present)
                opts = self._pw_get_dropdown_options(dropdown_id, max_wait=3, poll_interval=0.3)
                option_present = (
                    target_value in opts or
                    any(o.strip() == str(target_value).strip() for o in opts)
                )
                
                if option_present:
                    # Phase B: actual select (5s timeout from _pw_select_text)
                    self._pw_select_text(dropdown_id, target_value)
                    if attempt > 1:
                        self._add_log(
                            f"   ✅ Recovered: select '{target_value}' on attempt {attempt}/{max_retries}",
                            level='INFO', kind='select_recovery',
                        )
                    return True
                else:
                    last_error = f"Option '{target_value}' not present in dropdown (got: {opts[:5]})"
            
            except Exception as e:
                last_error = str(e)[:80]
            
            # Phase C: recovery — escalate per attempt
            if attempt < max_retries:
                if attempt == 1:
                    # Attempt 2: brief wait, AJAX may still be in flight
                    time.sleep(2)
                elif attempt == 2 and survey_no is not None:
                    # Attempt 3: re-issue GO to refresh the dropdown state
                    try:
                        self.page.fill(f'#{IDS["survey_no"]}', str(survey_no))
                        _rate_gate()
                        self.page.evaluate(f'document.getElementById("{IDS["go_btn"]}").click()')
                        self.page.wait_for_load_state('domcontentloaded')
                        time.sleep(Config.POST_CLICK_WAIT)
                    except Exception:
                        # If even GO fails, try a soft sleep
                        time.sleep(2)
                else:
                    time.sleep(1)
        
        # All retries exhausted
        self.logger.debug(
            f"_safe_select_with_recovery: dropdown={dropdown_id} value='{target_value}' "
            f"all {max_retries} attempts failed. last={last_error}"
        )
        return False
    
    def _pw_get_dropdown_options(self, selector_id, max_wait=10, poll_interval=0.5):
        """Helper: Get dropdown options with polling to avoid AJAX race."""
        try:
            elapsed = 0.0
            while elapsed < max_wait:
                self._beat()  # v12.1: long retry chains poll here — keep the watchdog fed
                time.sleep(poll_interval)
                elapsed += poll_interval
                options = self.page.locator(f'#{selector_id} option').all_text_contents()
                filtered = [o for o in options if 'Select' not in o and o.strip()]
                if filtered:
                    return filtered
            return []
        except Exception as e:
            self.logger.error(f"Error getting dropdown options for {selector_id}: {e}")
            return []
    
    def _extract_owners(self, page_source: str) -> List[dict]:
        """
        Extract owner details from page source.
        
        IMPROVED: Multi-strategy extraction with validation and fuzzy matching.
        """
        from bs4 import BeautifulSoup
        import re
        
        owners = []
        try:
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # CRITICAL FIX: Exclude form elements and dropdowns
            # Remove all select dropdowns, form elements, and navigation before parsing
            for unwanted in soup.find_all(['select', 'nav', 'header', 'footer', 'button', 'input', 'script', 'style']):
                unwanted.decompose()
            
            # ═══════════════════════════════════════════════════════════════════════
            # STRATEGY 1: Look for the RESULTS table by structural patterns
            # ═══════════════════════════════════════════════════════════════════════
            results_table = None
            
            # Keywords that indicate actual results vs form elements
            # 🔧 v7.0 FIX: Removed 'Khata'/'ಖಾತಾ' - portal no longer has Khata column
            RESULT_KEYWORDS = ['Owner', 'ಮಾಲೀಕರ', 'Extent', 'ವಿಸ್ತೀರ್ಣ', 'Name', 'ಹೆಸರು']
            FORM_KEYWORDS = [
                'Select District', 'Select Taluk', 'Select Hobli', 'Select Village',
                'Select Survey', 'Select Surnoc', 'Select Hissa', 'Select Period',
                'Toggle navigation', 'ಜಿಲ್ಲೆ ಆಯ್ಕೆಮಾಡಿ', 'ತಾಲ್ಲೂಕು ಆಯ್ಕೆಮಾಡಿ'
            ]
            SKIP_PATTERNS = re.compile(r'^(Sl\.?\s*No\.?|ಕ್ರಮ|ಸಂ|#|\d{1,3})$', re.IGNORECASE)
            
            for table in soup.find_all('table'):
                table_text = table.get_text()
                table_html = str(table).lower()
                
                # Score this table
                score = 0
                
                # Positive: has result keywords
                for kw in RESULT_KEYWORDS:
                    if kw in table_text:
                        score += 10
                
                # Negative: has form keywords
                for kw in FORM_KEYWORDS:
                    if kw in table_text:
                        score -= 50
                
                # Negative: has select tags
                if '<select' in table_html:
                    score -= 100
                
                # Positive: has reasonable rows
                num_rows = len(table.find_all('tr'))
                if 2 <= num_rows <= 100:
                    score += 5
                
                # Positive: has numeric cells (extent data)
                if re.search(r'\d+[\.\-]\d+[\.\-]\d+', table_text):
                    score += 15
                
                if score > 0 and (results_table is None or score > getattr(results_table, '_score', 0)):
                    results_table = table
                    results_table._score = score
            
            if not results_table:
                # ═══════════════════════════════════════════════════════════════════════
                # STRATEGY 2: Try to find owner data in divs with specific classes
                # ═══════════════════════════════════════════════════════════════════════
                for div in soup.find_all('div', class_=re.compile(r'result|data|owner|record', re.I)):
                    div_text = div.get_text(strip=True)
                    if len(div_text) > 50 and not any(kw in div_text for kw in FORM_KEYWORDS):
                        # Try to parse owner info from this div
                        name_match = re.search(r'(?:Owner|Name|ಮಾಲೀಕ)[:\s]*([^\n,]+)', div_text)
                        extent_match = re.search(r'(?:Extent|ವಿಸ್ತೀರ್ಣ)[:\s]*(\d+[\.\-]\d+[\.\-]\d+)', div_text)
                        if name_match:
                            owners.append({
                                'owner_name': name_match.group(1).strip(),
                                'extent': extent_match.group(1) if extent_match else '',
                                'owner_seq': len(owners),
                            })
                
                if not owners:
                    self.logger.debug(f"No valid results table or div found in page")
                    return owners
            
            if results_table:
                rows = results_table.find_all('tr')
                header_idx = {}
                header_found = False
                
                for row_idx, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        cell_texts = [c.get_text(strip=True) for c in cells]
                        row_text = ' '.join(cell_texts)
                        
                        # v7.1 FIX: Only check first 2 rows for headers.
                        # Prevents data rows containing "name"/"extent" in owner
                        # name text from being misidentified as headers.
                        if not header_found and row_idx <= 1:
                            is_header = any(
                                h.lower() in row_text.lower()
                                for h in ['owner', 'extent', 'slno', 'ಮಾಲೀಕ', 'ವಿಸ್ತೀರ್ಣ', 'ಕ್ರಮ']
                            )
                            if row_idx == 0 or is_header:
                                # v7.1 FIX: Strip SlNo from header so indices
                                # align with data rows (which also strip serials).
                                if cell_texts and SKIP_PATTERNS.match(cell_texts[0]):
                                    cell_texts = cell_texts[1:]
                                
                                # Pass 1: exact 'owner'/'ಮಾಲೀಕ' (high confidence)
                                for i, txt in enumerate(cell_texts):
                                    txt_lower = txt.lower()
                                    if 'owner' not in header_idx:
                                        if 'owner' in txt_lower or 'ಮಾಲೀಕ' in txt:
                                            header_idx['owner'] = i
                                    if 'extent' not in header_idx:
                                        if 'extent' in txt_lower or 'ವಿಸ್ತೀರ್ಣ' in txt:
                                            header_idx['extent'] = i
                                
                                # Pass 2: fallback to generic 'name'/'ಹೆಸರು'
                                if 'owner' not in header_idx:
                                    for i, txt in enumerate(cell_texts):
                                        if 'name' in txt.lower() or 'ಹೆಸರು' in txt:
                                            header_idx['owner'] = i
                                            break
                                
                                header_found = True
                                continue
                        
                        # Skip rows that look like form elements
                        is_form_row = any(pattern in row_text for pattern in FORM_KEYWORDS)
                        is_district_list = re.search(r'[A-Z]{5,}[A-Z]{5,}', row_text.replace(' ', ''))
                        
                        if is_form_row or is_district_list:
                            continue
                        
                        # Strip serial number from data rows
                        if SKIP_PATTERNS.match(cell_texts[0]):
                            cell_texts = cell_texts[1:]
                            if not cell_texts:
                                continue
                        
                        owner_idx = header_idx.get('owner', 0)
                        extent_idx = header_idx.get('extent', 1)
                        
                        owner_name = cell_texts[owner_idx] if owner_idx < len(cell_texts) else ''
                        extent = cell_texts[extent_idx] if extent_idx < len(cell_texts) else ''
                        
                        if not owner_name or len(owner_name) < 2:
                            continue
                        if owner_name.isdigit():
                            continue
                        if any(skip in owner_name for skip in ['Select', 'ಆಯ್ಕೆ', 'Toggle']):
                            continue
                        
                        # v12.2 (audit C2): value-dedup removed. Each accepted table
                        # row is a distinct ownership entry — two co-owners with the
                        # same name AND extent are two records. owner_seq (row order)
                        # carries the identity; the DB key dedups re-scrapes instead.
                        owners.append({
                            'owner_name': owner_name,
                            'extent': extent,
                            'owner_seq': len(owners),
                        })
            
            # Log extraction result for debugging
            if not owners:
                self.logger.debug(f"No owners extracted from page")
                
        except Exception as e:
            self.logger.error(f"Extract error: {e}")
        
        return owners
    
    def _search_village(self, village_code: str, village_name: str, hobli_code: str, hobli_name: str):
        """
        Search a single village - PLAYWRIGHT VERSION
        """
        from playwright.sync_api import TimeoutError as PlaywrightTimeout

        IDS = Config.ELEMENT_IDS
        
        # Validate max_survey parameter
        max_survey_raw = self.params.get('max_survey', Config.DEFAULT_MAX_SURVEY)
        try:
            max_survey = int(max_survey_raw) if max_survey_raw else Config.DEFAULT_MAX_SURVEY
            if max_survey <= 0:
                max_survey = Config.DEFAULT_MAX_SURVEY
        except (ValueError, TypeError):
            max_survey = Config.DEFAULT_MAX_SURVEY
        
        owner_variants = self.state.owner_variants

        district_name = self.params.get('district_name', 'Unknown')
        taluk_name = self.params.get('taluk_name', 'Unknown')

        self._update_status(
            current_village=village_name,
            current_survey=0,
            max_survey=max_survey
        )
        
        empty_count = 0
        surveys_checked = 0
        surveys_with_data = 0
        session_retries = 0  # Track session recovery attempts
        portal_retries = 0   # Track RTC access retries per survey
        
        # ═══════════════════════════════════════════════════════════════════════
        # ROBUST RETRY TRACKING - Prevents false positive skips
        # ═══════════════════════════════════════════════════════════════════════
        consecutive_errors = 0     # Track consecutive errors for browser health
        retry_queue = []           # Surveys to retry at end of village
        
        # ═══════════════════════════════════════════════════════════════════════
        # SMART STOP TRACKING - For accurate reporting and confidence scoring
        # ═══════════════════════════════════════════════════════════════════════
        last_survey_with_data = 0  # Track last survey where data was found
        skipped_in_village = []    # Track skipped surveys in this village
        completion_reason = 'max_reached'  # Default completion reason

        # ═══════════════════════════════════════════════════════════════════════
        # CHECK FOR RESUME CHECKPOINT - Skip already completed surveys
        # ═══════════════════════════════════════════════════════════════════════
        start_survey = 1
        if self.db and self.session_id:
            try:
                # v12: hobli-qualified checkpoint key. Village codes repeat across hoblis
                # in an 'all-hobli' search; keying on village_code alone let one hobli's
                # village inherit another's checkpoint and SKIP already-"done" surveys —
                # silent data loss. Qualify with hobli_code so each village is distinct.
                checkpoint = self.db.get_last_checkpoint(self.session_id, f"{hobli_code}:{village_code}")
                if checkpoint:
                    start_survey = checkpoint['survey_no'] + 1  # Resume from next survey
                    self._add_log(f"📍 Resuming {village_name} from survey {start_survey} (checkpoint found)")
            except Exception as chkpt_err:
                self.logger.debug(f"Checkpoint lookup failed: {chkpt_err}")

        # ═══════════════════════════════════════════════════════════════════════
        # v11.0: SKELETON-BASED SURVEY FILTERING
        # ───────────────────────────────────────────────────────────────────────
        # If we have a Service154 skeleton for this village, the user's `max_survey`
        # input becomes IRRELEVANT — Service154 has already told us the authoritative
        # list of surveys that exist. We:
        #   1. Set start_survey = max(checkpoint, skeleton_min) — respects resume.
        #   2. Set max_survey   = skeleton_max — IGNORES user's guess (which was a
        #      v10-era enumeration ceiling that has no meaning when we have the
        #      exact list).
        #   3. Iterate ONLY surveys present in the skeleton — empty/missing ones
        #      are skipped without portal calls.
        #
        # If no skeleton (fetch failed or S154 unreachable), behavior is identical
        # to v10 (1..max_survey enumeration with smart-stop). max_survey then acts
        # as the safety ceiling for enumeration only.
        # ═══════════════════════════════════════════════════════════════════════
        # ═══════════════════════════════════════════════════════════════════════
        # v11.0 LESSONS LEARNED — SKELETON IS NOT AUTHORITATIVE
        # ───────────────────────────────────────────────────────────────────────
        # Empirical evidence (May 21 2026): Service154's `Fn_GetMRSurveyDataVillageWise`
        # endpoint TRUNCATES the survey list. For GANGAVARA CHOWDAPPANAHALLI, skeleton
        # reported max survey=320, but Service2 has data at survey 350, 400, etc.
        # Using skeleton as the upper bound silently dropped ~25% of records per village.
        #
        # CURRENT POLICY:
        #   - Iterate ALL surveys 1..max_survey (user input) — like v10
        #   - Smart-stop handles genuinely empty trailing surveys
        #   - Skeleton is loaded but used ONLY for empty-dropdown rescue (safety net)
        #     and informational logging — NEVER as a filter or upper bound.
        # ═══════════════════════════════════════════════════════════════════════
        skeleton_survey_set: Optional[Set[int]] = None
        skeleton_active_for_rescue = False  # only enables empty-dropdown rescue
        skeleton_hissa_map: Dict[Tuple[int, str], Set[str]] = {}
        skeleton_surnocs_per_survey: Dict[int, Set[str]] = {}
        
        if Config.SKELETON_FIRST_ENABLED:
            village_skel = self.skeletons.get(str(village_code), [])
            if village_skel:
                skeleton_survey_set = {int(r['survey_no']) for r in village_skel if isinstance(r.get('survey_no'), int)}
                for r in village_skel:
                    try:
                        sy = int(r['survey_no'])
                    except (TypeError, ValueError):
                        continue
                    sn = str(r.get('surnoc', '')).strip()
                    hi = str(r.get('hissa', '')).strip()
                    skeleton_hissa_map.setdefault((sy, sn), set()).add(hi)
                    skeleton_surnocs_per_survey.setdefault(sy, set()).add(sn)
                
                if skeleton_survey_set:
                    skel_min = min(skeleton_survey_set)
                    skel_max = max(skeleton_survey_set)
                    skeleton_active_for_rescue = True
                    self._add_log(
                        f"📋 {village_name}: Skeleton loaded for empty-dropdown rescue "
                        f"({len(skeleton_survey_set)} surveys advisory; iterating 1..{max_survey} for safety)"
                    )
        
        # Sync worker_status max_survey for accurate UI display
        self._update_status(max_survey=max_survey)
        self._add_log(
            f"🏘️ Starting {village_name}: Surveys {start_survey} to {max_survey} "
            f"(full enumeration; smart-stop active)"
        )
        
        # Validate survey range
        if start_survey > max_survey:
            self._add_log(f"❌ ERROR: Invalid survey range - start ({start_survey}) > max ({max_survey})")
            return

        # SEQUENTIAL SURVEY ITERATION: 1, 2, 3... NO SKIPPING
        # v11.0 (post May 21 fix): skeleton-based skip removed — Service154 truncates
        # survey lists. We iterate the full user range; smart-stop handles tail.
        survey_no = start_survey
        while survey_no <= max_survey:
            if not self.state.running:
                self._add_log(f"⏹️ Stopped at survey {survey_no}/{max_survey}")
                return
            
            surveys_checked += 1
            self._update_status(current_survey=survey_no)
            
            # Log every 10th survey for better tracking
            if survey_no == 1 or survey_no % 10 == 0:
                self._add_log(f"📍 {village_name}: Survey {survey_no}/{max_survey} (found {surveys_with_data})")
            
            try:
                # Navigate to portal (Playwright)
                self.page.goto(Config.SERVICE2_URL)
                self.page.wait_for_load_state('domcontentloaded')
                time.sleep(Config.POST_SELECT_WAIT)
                
                # SESSION EXPIRATION CHECK
                if self._is_session_expired():
                    self._add_log(f"⚠️ Session expired at {village_name} survey {survey_no}")
                    if session_retries < Config.MAX_SESSION_RETRIES:
                        session_retries += 1
                        if self._refresh_session():
                            continue
                        else:
                            self._close_browser()
                            time.sleep(2)
                            self._init_browser()
                            continue
                    else:
                        raise Exception(f"Session expired {session_retries} times")
                
                session_retries = 0
                
                # Select location (Playwright)
                self._pw_select(IDS['district'], self.params['district_code'])
                time.sleep(Config.POST_SELECT_WAIT)
                
                self._pw_select(IDS['taluk'], self.params['taluk_code'])
                time.sleep(Config.POST_SELECT_WAIT)
                
                self._pw_select(IDS['hobli'], hobli_code)
                time.sleep(Config.POST_SELECT_WAIT)
                
                self._pw_select(IDS['village'], village_code)
                time.sleep(Config.POST_SELECT_WAIT)
                
                # Enter survey number
                self.page.fill(f'#{IDS["survey_no"]}', str(survey_no))
                
                # Click GO using JavaScript (CRITICAL: Portal requires JS click, not direct click)
                _rate_gate()
                self.page.evaluate(f'document.getElementById("{IDS["go_btn"]}").click()')
                self.page.wait_for_load_state('domcontentloaded')
                time.sleep(Config.POST_CLICK_WAIT)
                
                # Wait for surnoc dropdown to populate via AJAX
                time.sleep(2)
                
                # ═══════════════════════════════════════════════════════════════════════
                # ROBUST PORTAL ISSUE HANDLING - Prevents false positive skips
                # Key improvements:
                # 1. 5 retries with exponential backoff (3s→6s→12s→24s→48s)
                # 2. Browser refresh on 3rd retry
                # 3. Portal health monitoring across workers
                # 4. Only add to skip list after ALL retries fail
                # ═══════════════════════════════════════════════════════════════════════
                
                # First, handle any portal alerts (e.g., "facing issues" messages)
                had_alert, alert_text, is_portal_issue = self._handle_alert()
                
                if is_portal_issue:
                    portal_retries += 1
                    consecutive_errors += 1
                    
                    # ═══════════════════════════════════════════════════════════════════════
                    # CONSECUTIVE ERROR BROWSER RESTART - Browser might be unhealthy
                    # ═══════════════════════════════════════════════════════════════════════
                    if consecutive_errors >= Config.CONSECUTIVE_ERROR_RESTART and portal_retries == 1:
                        # First retry on this survey but many consecutive errors overall
                        # Browser is likely degraded - restart it proactively
                        self._add_log(f"🔄 {consecutive_errors} consecutive errors - proactive browser restart...")
                        try:
                            self._close_browser()
                            time.sleep(Config.BROWSER_RESTART_DELAY)
                            self._init_browser()
                            consecutive_errors = 0
                            self.hissa_processed_count = 0
                            self._add_log(f"✅ Browser restarted (consecutive error threshold)")
                        except Exception as restart_err:
                            self._add_log(f"⚠️ Proactive browser restart failed: {str(restart_err)[:30]}")
                        # Don't continue - let the normal retry flow handle it
                    
                    # Report to global health monitor
                    should_pause = portal_health.report_error(self.worker_id, 'rtc_access')
                    
                    # Check if we're in a portal-wide cooldown
                    wait_time = portal_health.should_wait()
                    if wait_time > 0:
                        self._add_log(f"⏸️ Portal cooldown: waiting {int(wait_time)}s for portal recovery...")
                        time.sleep(wait_time)
                        continue  # Retry after cooldown
                    
                    # Calculate exponential backoff wait time
                    backoff_wait = min(
                        Config.RETRY_BACKOFF_BASE * (Config.RETRY_BACKOFF_MULTIPLIER ** (portal_retries - 1)),
                        Config.RETRY_MAX_WAIT
                    )
                    
                    if portal_retries <= Config.MAX_PORTAL_RETRIES:
                        # ═══════════════════════════════════════════════════════════════════════
                        # RETRY STRATEGY: Different actions at different retry levels
                        # ═══════════════════════════════════════════════════════════════════════
                        
                        if portal_retries <= 2:
                            # Retries 1-2: Simple wait and retry
                            self._add_log(f"⚠️ RTC issue at {village_name} Sy:{survey_no} (retry {portal_retries}/{Config.MAX_PORTAL_RETRIES}, wait {int(backoff_wait)}s)")
                            time.sleep(backoff_wait)
                            
                        elif portal_retries == Config.BROWSER_REFRESH_ON_RETRY:
                            # Retry 3: Clear cookies and refresh session
                            self._add_log(f"🔄 RTC issue retry {portal_retries}/{Config.MAX_PORTAL_RETRIES} - Refreshing session...")
                            try:
                                self.page.context.clear_cookies()
                                time.sleep(1)
                                self.page.goto(Config.SERVICE2_URL)
                                self.page.wait_for_load_state('domcontentloaded')
                                time.sleep(Config.SESSION_REFRESH_WAIT)
                            except Exception as refresh_err:
                                self._add_log(f"⚠️ Session refresh failed: {str(refresh_err)[:30]}")
                            time.sleep(backoff_wait)
                            
                        elif portal_retries == 4:
                            # Retry 4: Full browser restart
                            self._add_log(f"🔄 RTC issue retry {portal_retries}/{Config.MAX_PORTAL_RETRIES} - Restarting browser...")
                            try:
                                self._close_browser()
                                time.sleep(Config.BROWSER_RESTART_DELAY)
                                self._init_browser()
                                consecutive_errors = 0  # Reset after browser restart
                                self.hissa_processed_count = 0
                                self._add_log(f"✅ Browser restarted for retry")
                            except Exception as restart_err:
                                self._add_log(f"⚠️ Browser restart failed: {str(restart_err)[:30]}")
                            time.sleep(backoff_wait)
                            
                        else:
                            # Retry 5: Last attempt with maximum wait
                            self._add_log(f"⚠️ FINAL retry {portal_retries}/{Config.MAX_PORTAL_RETRIES} for Sy:{survey_no}, wait {int(backoff_wait)}s...")
                            time.sleep(backoff_wait)
                        
                        continue  # Retry same survey
                    
                    else:
                        # ═══════════════════════════════════════════════════════════════════════
                        # ALL RETRIES EXHAUSTED - IMMEDIATELY save to skip list (v6.0 FIX)
                        # ═══════════════════════════════════════════════════════════════════════
                        self._add_log(f"⏭️ Skipping Sy:{survey_no} after {Config.MAX_PORTAL_RETRIES} retries (RTC access issue)")
                        
                        # 🔧 v6.0 FIX: Create skip record with full details
                        skip_record = {
                            'village': village_name,
                            'village_code': village_code,
                            'survey_no': survey_no,
                            'surnoc': '',  # Unknown - couldn't access survey
                            'hissa': '',   # Unknown - couldn't access survey
                            'period': '',  # Unknown - couldn't access survey
                            'reason': f'RTC access issue after {Config.MAX_PORTAL_RETRIES} retries: {alert_text[:50] if alert_text else "Portal error"}',
                            'timestamp': datetime.now().isoformat()
                        }
                        
                        # 🔧 v6.0 FIX: IMMEDIATELY save to ALL tracking systems
                        skipped_in_village.append(skip_record)
                        
                        # 🔧 v6.0 FIX: Save to in-memory state IMMEDIATELY
                        with self.state_lock:
                            self.state.skipped_surveys.append(skip_record)
                        
                        # 🔧 v6.0 FIX: Save to database IMMEDIATELY
                        if self.db and self.session_id:
                            try:
                                self.db.save_skipped_item(
                                    session_id=self.session_id,
                                    village_name=village_name,
                                    survey_no=survey_no,
                                    surnoc='',
                                    hissa='',
                                    period='',
                                    error=f'RTC access issue after {Config.MAX_PORTAL_RETRIES} retries'
                                )
                            except Exception as db_err:
                                self.logger.debug(f"Failed to save skipped item to DB: {db_err}")
                        
                        # Also add to retry queue for potential future use
                        retry_queue.append({
                            'survey_no': survey_no,
                            'attempts': portal_retries,
                            'last_error': alert_text[:50] if alert_text else 'RTC access issue'
                        })
                        
                        portal_retries = 0  # Reset for next survey
                        survey_no += 1
                        continue  # Move to next survey
                
                # SUCCESS - Reset consecutive error counter and report to health monitor
                consecutive_errors = 0
                portal_health.report_success(self.worker_id)
                
                page_source = self.page.content()
                if self._is_session_expired(page_source):
                    self._add_log(f"⚠️ Session expired after GO")
                    if session_retries < Config.MAX_SESSION_RETRIES:
                        session_retries += 1
                        self._refresh_session()
                        continue
                    else:
                        raise Exception(f"Persistent session expiry")
                
                # Check if surnoc populated (Playwright)
                surnoc_opts = self._pw_get_dropdown_options(IDS['surnoc'])
                
                # ═══════════════════════════════════════════════════════════════════════
                # v11.0 100% ACCURACY — DROPDOWN-EMPTY RESCUE
                # ───────────────────────────────────────────────────────────────────────
                # If Service2 returned an empty surnoc dropdown but Service154 says this
                # survey HAS data (skeleton has tuples for this survey), it's almost
                # certainly a race/AJAX glitch — not a genuinely empty survey. Re-issue
                # GO once and, if still empty, fall through to use skeleton's surnoc list
                # directly. This prevents the false-skip path from claiming survey is empty.
                # ═══════════════════════════════════════════════════════════════════════
                if not surnoc_opts and skeleton_active_for_rescue and survey_no in (skeleton_survey_set or set()):
                    self._add_log(
                        f"🔬 Sy:{survey_no} dropdown empty but skeleton has data — refetching",
                        level='WARN', kind='precision',
                    )
                    try:
                        # One quick re-issue of GO to give the AJAX another chance
                        time.sleep(2)
                        self.page.evaluate(f'document.getElementById("{IDS["go_btn"]}").click()')
                        self.page.wait_for_load_state('domcontentloaded')
                        time.sleep(Config.POST_CLICK_WAIT + 2)
                        surnoc_opts = self._pw_get_dropdown_options(IDS['surnoc'])
                    except Exception:
                        pass
                    # If STILL empty, use skeleton surnocs directly. The select_option
                    # call later may still fail if Service2 truly has no data, but at
                    # least we'll attempt — beats silently treating as empty.
                    if not surnoc_opts:
                        sk_surnocs = skeleton_surnocs_per_survey.get(survey_no, set())
                        if sk_surnocs:
                            surnoc_opts = sorted(sk_surnocs)
                            self._add_log(
                                f"   ⚠️ Using skeleton surnocs as fallback: {surnoc_opts}",
                                level='WARN', kind='precision',
                            )
                
                if not surnoc_opts:
                    # This is a genuinely empty survey (not session expired)
                    empty_count += 1
                    
                    # ═══════════════════════════════════════════════════════════════════════
                    # SMART STOP LOGIC - Stop after N consecutive empty surveys
                    # Only triggers after minimum surveys checked AND data was found.
                    # v9.0 FIX: Never stop if no data has been found yet — the data
                    # region may start at higher survey numbers (e.g., survey 80+).
                    # ═══════════════════════════════════════════════════════════════════════
                    if (Config.SMART_STOP_ENABLED and
                        surveys_checked >= Config.MIN_SURVEYS_BEFORE_STOP and
                        empty_count >= Config.EMPTY_SURVEY_THRESHOLD and
                        surveys_with_data > 0):

                        # ═══════════════════════════════════════════════════════════════
                        # v12: SKELETON-VERIFIED SMART STOP.
                        # Before abandoning the village, ask the Service154 skeleton whether
                        # any surveys exist BEYOND this point (within the user's max_survey).
                        # If so, jump straight to the next one and keep scanning — this
                        # rescues data clusters that sit after a >THRESHOLD empty gap.
                        # Skeleton can only ADD surveys here, so its known under-reporting
                        # never causes a miss; over-reporting just costs a few extra checks.
                        # ═══════════════════════════════════════════════════════════════
                        if Config.SMART_STOP_SKELETON_VERIFY and skeleton_survey_set:
                            future_surveys = sorted(
                                s for s in skeleton_survey_set
                                if s > survey_no and s <= max_survey
                            )
                            if future_surveys:
                                nxt = future_surveys[0]
                                self._add_log(
                                    f"🔎 {village_name}: smart-stop deferred — skeleton lists "
                                    f"{len(future_surveys)} survey(s) beyond {survey_no} "
                                    f"(next {nxt}); continuing instead of stopping",
                                    level='WARN', kind='precision',
                                )
                                empty_count = 0
                                survey_no = nxt
                                continue

                        completion_reason = 'smart_stop'
                        surveys_saved = max_survey - survey_no
                        
                        self._add_log(f"🏁 SMART STOP: {village_name}")
                        self._add_log(f"   └─ Reason: {empty_count} consecutive empty surveys")
                        self._add_log(f"   └─ Last data at survey: {last_survey_with_data}")
                        self._add_log(f"   └─ Checked: {surveys_checked}, Found data in: {surveys_with_data}")
                        self._add_log(f"   └─ Records: {self.records_found}, Skipped: {len(skipped_in_village)}")
                        self._add_log(f"   └─ Surveys saved: {surveys_saved} (⏱️ ~{surveys_saved * 3}s saved)")
                        
                        # Update global smart stop stats
                        with self.state_lock:
                            self.state.smart_stops += 1
                            self.state.surveys_saved += surveys_saved
                        
                        break
                    
                    survey_no += 1  # Move to next survey
                    continue
                
                # Found data - reset counters and update tracking
                empty_count = 0
                portal_retries = 0  # Reset portal retry counter on success
                surveys_with_data += 1
                last_survey_with_data = survey_no  # Track last successful survey
                
                # ═══════════════════════════════════════════════════════════════════════
                # v11.0 LEARNING (Option B reverted, keep informational logs):
                # ───────────────────────────────────────────────────────────────────────
                # We previously tried UNIONing skeleton surnocs with dropdown surnocs.
                # Empirically, skeleton-only surnocs are HISTORICAL / SUBDIVIDED entries
                # that Service2's dropdown intentionally doesn't expose (because their
                # data has been migrated to current sub-entries). Trying to select them
                # fails with a 5s timeout per entry — wasted time AND false skips.
                #
                # Correct approach: trust Service2's dropdown for surnocs (authoritative
                # for queryable current data). Just LOG when skeleton differs so we have
                # visibility, but don't try to select non-existent options.
                # ═══════════════════════════════════════════════════════════════════════
                target_surnocs = list(surnoc_opts)
                if skeleton_active_for_rescue:
                    sk_surnocs_for_sy = skeleton_surnocs_per_survey.get(survey_no, set())
                    extras = sk_surnocs_for_sy - set(surnoc_opts)
                    if extras:
                        # INFORMATIONAL only — these are typically historical/composite
                        # entries (e.g., '*' summary, subdivided ancestors). Their data
                        # is covered by the current dropdown entries.
                        self.logger.debug(
                            f"Sy:{survey_no} skeleton has {len(extras)} surnoc(s) not in dropdown "
                            f"(likely historical/composite, ignoring): {sorted(extras)}"
                        )
                
                # Process each surnoc
                for surnoc in target_surnocs:
                    if not self.state.running:
                        return
                    
                    try:
                        # ═══════════════════════════════════════════════════════════════
                        # v11.0 ROBUST SURNOC SELECTION + STALE-STATE DETECTION
                        # ───────────────────────────────────────────────────────────────
                        # 1. Try select with recovery (verify + 3 retries with GO refresh)
                        # 2. If still fails, RE-READ dropdown to distinguish:
                        #    - Empty dropdown = stale state from previous survey;
                        #      survey is genuinely empty → continue, NOT a skip
                        #    - Non-empty dropdown = real selection failure → skip + Phase 2
                        # ═══════════════════════════════════════════════════════════════
                        if not self._safe_select_with_recovery(
                            IDS['surnoc'], surnoc,
                            survey_no=survey_no,
                            max_retries=3,
                        ):
                            # Distinguish stale-state false-positive from real failure
                            try:
                                current_opts = self._pw_get_dropdown_options(
                                    IDS['surnoc'], max_wait=2, poll_interval=0.3
                                )
                            except Exception:
                                current_opts = []
                            
                            if not current_opts:
                                # Dropdown is now empty — survey doesn't actually have data,
                                # the original surnoc list was stale state from prior survey.
                                # Treat as genuinely empty survey, NOT a skip.
                                self.logger.debug(
                                    f"Sy:{survey_no} surnoc='{surnoc}' selection failed but "
                                    f"dropdown is empty → stale state, treating as empty survey"
                                )
                                # Don't increment empty_count further (already handled by
                                # outer empty-survey path) — just skip this surnoc loop iter.
                                continue
                            
                            # Genuine selection failure — dropdown still has options
                            self._add_log(
                                f"⚠️ Surnoc '{surnoc}' could not be selected for Sy:{survey_no} "
                                f"after recovery — added to Phase 2 retry queue",
                                level='WARN', kind='select_recovery',
                            )
                            self.errors += 1
                            skip_record = {
                                'village': village_name,
                                'village_code': village_code,
                                'survey_no': survey_no,
                                'surnoc': surnoc,
                                'hissa': '*',
                                'period': '',
                                'reason': f'Surnoc selection failed after recovery retries',
                                'timestamp': datetime.now().isoformat()
                            }
                            skipped_in_village.append(skip_record)
                            with self.state_lock:
                                self.state.skipped_surveys.append(skip_record)
                            if self.db and self.session_id:
                                try:
                                    self.db.save_skipped_item(
                                        session_id=self.session_id,
                                        village_name=village_name,
                                        survey_no=survey_no,
                                        surnoc=surnoc,
                                        hissa='*',
                                        period='',
                                        error='Surnoc selection failed after recovery'
                                    )
                                except Exception:
                                    pass
                            continue
                        
                        time.sleep(Config.POST_SELECT_WAIT + 1)
                        
                        # Get hissa options (Playwright) — Service2's dropdown is authoritative
                        # for which hissas are CURRENTLY selectable. Skeleton has historical
                        # entries too (subdivided/composite) that fail with timeout if we try
                        # them. So we trust the dropdown here.
                        hissa_opts = self._pw_get_dropdown_options(IDS['hissa'])
                        target_hissas = list(hissa_opts)
                        
                        if skeleton_active_for_rescue:
                            sk_hissas = skeleton_hissa_map.get((survey_no, surnoc), set())
                            extras_hi = sk_hissas - set(hissa_opts)
                            if extras_hi:
                                # INFORMATIONAL only — log but don't attempt. These are
                                # historical entries (e.g., '*' summary, '-p1' partitions,
                                # 'P1' provisional, '2A'/'2B' subdivided) whose CURRENT
                                # data is covered by the dropdown's entries.
                                self.logger.debug(
                                    f"Sy:{survey_no} Sn:{surnoc} skeleton has {len(extras_hi)} "
                                    f"hissa(s) not in dropdown (likely historical, ignoring): "
                                    f"{sorted(extras_hi)}"
                                )
                        
                        # Process each hissa
                        for hissa in target_hissas:
                            if not self.state.running:
                                return
                            
                            hissa_retry_count = 0
                            max_hissa_retries = 2
                            
                            while hissa_retry_count <= max_hissa_retries:
                                try:
                                    # v11.0: use recovery helper for hissa too — same race
                                    # conditions affect this dropdown under high concurrency.
                                    if not self._safe_select_with_recovery(
                                        IDS['hissa'], hissa,
                                        survey_no=survey_no,
                                        max_retries=3,
                                    ):
                                        # Genuine hissa selection failure — raise so the
                                        # outer hissa_retry loop can do its own recovery
                                        # (re-select surnoc + retry hissa once)
                                        raise Exception(f"Hissa '{hissa}' selection failed after recovery")
                                    
                                    # Wait for period dropdown to enable after selecting hissa
                                    time.sleep(Config.POST_SELECT_WAIT + 2)
                                    
                                    # PERIOD PROCESSING
                                    period_opts = self._pw_get_dropdown_options(IDS['period'])
                                    
                                    if not period_opts:
                                        self._add_log(f"⚠️ No periods for Sy:{survey_no} H:{hissa}")
                                        # TRACK THIS GAP - period dropdown empty
                                        skip_record = {
                                            'village': village_name,
                                            'village_code': village_code,
                                            'survey_no': survey_no,
                                            'surnoc': surnoc,
                                            'hissa': hissa,
                                            'period': '',
                                            'reason': 'No periods available in dropdown',
                                            'timestamp': datetime.now().isoformat()
                                        }
                                        skipped_in_village.append(skip_record)
                                        with self.state_lock:
                                            self.state.skipped_surveys.append(skip_record)
                                        if self.db and self.session_id:
                                            try:
                                                self.db.save_skipped_item(
                                                    session_id=self.session_id,
                                                    village_name=village_name,
                                                    survey_no=survey_no,
                                                    surnoc=surnoc,
                                                    hissa=hissa,
                                                    period='',
                                                    error='No periods available in dropdown'
                                                )
                                            except Exception:
                                                pass
                                        break  # Move to next hissa
                                    
                                    # Determine how many periods to process based on config
                                    period_selected = False
                                    if Config.PROCESS_ALL_PERIODS:
                                        # Process ALL periods for 100% accuracy
                                        max_period_attempts = len(period_opts)
                                    elif Config.LATEST_PERIOD_ONLY:
                                        # ⚡ LATEST PERIOD ONLY: Only process the first (most recent) period
                                        # The first option is always the latest (e.g., "2001-09-25 00:00:00 To Till Date (2025-2026)")
                                        max_period_attempts = 1
                                    else:
                                        # Process only first few periods (speed mode)
                                        max_period_attempts = min(5, len(period_opts))
                                    
                                    for period_idx in range(max_period_attempts):
                                        if not self.state.running:
                                            return
                                        
                                        period = period_opts[period_idx]
                                        
                                        # ═══════════════════════════════════════════════════════════════════════
                                        # 🔧 v6.0 FIX: Period selection with retry logic
                                        # ═══════════════════════════════════════════════════════════════════════
                                        period_select_success = False
                                        period_select_retries = 0
                                        max_period_select_retries = getattr(Config, 'PERIOD_SELECTION_RETRIES', 3)
                                        
                                        while not period_select_success and period_select_retries < max_period_select_retries:
                                            try:
                                                self._pw_select_text(IDS['period'], period)
                                                time.sleep(1)
                                                period_select_success = True
                                            except Exception as period_select_err:
                                                period_select_retries += 1
                                                if period_select_retries < max_period_select_retries:
                                                    self._add_log(f"⚠️ Period select retry {period_select_retries}/{max_period_select_retries} for Sy:{survey_no} H:{hissa}")
                                                    time.sleep(2)
                                                    # Try to re-select hissa to refresh state
                                                    try:
                                                        self._pw_select_text(IDS['hissa'], hissa)
                                                        time.sleep(1)
                                                    except Exception:
                                                        pass
                                                else:
                                                    # All period selection retries exhausted
                                                    raise Exception(f"Period selection failed after {max_period_select_retries} retries: {str(period_select_err)[:30]}")
                                        
                                        try:
                                            # ROBUST FETCH WITH RETRY (Playwright)
                                            fetch_success = False
                                            fetch_retries = 0
                                            max_fetch_retries = 3
                                            
                                            while not fetch_success and fetch_retries < max_fetch_retries:
                                                # Click Fetch using JavaScript (CRITICAL: Portal requires JS click)
                                                _rate_gate()
                                                self.page.evaluate(f'document.getElementById("{IDS["fetch_btn"]}").click()')
                                                self.page.wait_for_load_state('domcontentloaded')
                                                time.sleep(Config.POST_CLICK_WAIT)
                                                
                                                # Handle any portal alerts after Fetch
                                                had_alert, alert_text, is_portal_issue = self._handle_alert()
                                                
                                                if is_portal_issue:
                                                    fetch_retries += 1
                                                    if fetch_retries < max_fetch_retries:
                                                        # Calculate backoff wait
                                                        backoff = Config.RETRY_BACKOFF_BASE * fetch_retries
                                                        self._add_log(f"⚠️ FETCH retry {fetch_retries}/{max_fetch_retries} for Sy:{survey_no} H:{hissa} (wait {backoff}s)")
                                                        
                                                        # Check portal health
                                                        wait_time = portal_health.should_wait()
                                                        if wait_time > 0:
                                                            time.sleep(wait_time)
                                                        else:
                                                            time.sleep(backoff)
                                                        
                                                        # On 2nd retry, refresh the page state
                                                        if fetch_retries == 2:
                                                            try:
                                                                self._pw_select_text(IDS['period'], period)
                                                                time.sleep(1)
                                                            except Exception:
                                                                pass
                                                        
                                                        portal_health.report_error(self.worker_id, 'fetch_error')
                                                        continue  # Retry fetch
                                                    else:
                                                        # All fetch retries exhausted for this period
                                                        self._add_log(f"⏭️ FETCH failed after {max_fetch_retries} retries: Sy:{survey_no} H:{hissa} P:{period[:15]}")
                                                        
                                                        # Track this as a skipped hissa
                                                        skip_record = {
                                                            'village': village_name,
                                                            'village_code': village_code,
                                                            'survey_no': survey_no,
                                                            'surnoc': surnoc,
                                                            'hissa': hissa,
                                                            'period': period,
                                                            'reason': f'FETCH failed after {max_fetch_retries} retries',
                                                            'timestamp': datetime.now().isoformat()
                                                        }
                                                        skipped_in_village.append(skip_record)
                                                        with self.state_lock:
                                                            self.state.skipped_surveys.append(skip_record)
                                                        
                                                        # Save to database
                                                        if self.db and self.session_id:
                                                            try:
                                                                self.db.save_skipped_item(
                                                                    session_id=self.session_id,
                                                                    village_name=village_name,
                                                                    survey_no=survey_no,
                                                                    surnoc=surnoc,
                                                                    hissa=hissa,
                                                                    period=period,
                                                                    error=f'FETCH failed after {max_fetch_retries} retries'
                                                                )
                                                            except Exception:
                                                                pass
                                                        
                                                        break  # Exit fetch retry loop, try next period
                                                else:
                                                    fetch_success = True
                                                    portal_health.report_success(self.worker_id)
                                            
                                            if not fetch_success:
                                                continue  # Try next period
                                            
                                            # Verify page loaded
                                            page_source = self.page.content()
                                            if 'Session expired' in page_source or 'login again' in page_source.lower():
                                                raise Exception("Session expired during fetch")
                                            
                                            # Extract owners FIRST before logging success
                                            owners = self._extract_owners(page_source)
                                            
                                            # ═══════════════════════════════════════════════════════════════
                                            # v9.2 FIX: NO-OWNERS RE-FETCH RETRY
                                            # ═══════════════════════════════════════════════════════════════
                                            # If owner extraction returns empty, the portal likely returned a
                                            # partial response under load. Re-click Fetch a few times before
                                            # giving up. Confirmed empirically: surveys marked as skipped DO
                                            # have owner data when re-tested in isolation.
                                            # ═══════════════════════════════════════════════════════════════
                                            if not owners:
                                                no_owner_retries = getattr(Config, 'NO_OWNER_REFETCH_RETRIES', 3)
                                                refetch_wait = getattr(Config, 'NO_OWNER_REFETCH_WAIT', 5)
                                                
                                                for refetch_attempt in range(1, no_owner_retries + 1):
                                                    self._add_log(f"🔄 Sy:{survey_no} H:{hissa}: empty page, re-fetch {refetch_attempt}/{no_owner_retries}")
                                                    time.sleep(refetch_wait * refetch_attempt)  # progressive backoff: 5s, 10s, 15s
                                                    
                                                    try:
                                                        _rate_gate()
                                                        self.page.evaluate(f'document.getElementById("{IDS["fetch_btn"]}").click()')
                                                        self.page.wait_for_load_state('domcontentloaded')
                                                        time.sleep(Config.POST_CLICK_WAIT + 2)  # extra wait for AJAX
                                                        
                                                        # Skip alert handling failure — if alert appears now, accept failure
                                                        had_alert, _, is_portal_issue = self._handle_alert()
                                                        if is_portal_issue:
                                                            continue  # try next refetch
                                                        
                                                        page_source = self.page.content()
                                                        owners = self._extract_owners(page_source)
                                                        
                                                        if owners:
                                                            self._add_log(f"✅ RECOVERED Sy:{survey_no} H:{hissa} [{len(owners)} owners] on re-fetch {refetch_attempt}")
                                                            with self.state_lock:
                                                                self.state.no_owner_recovered = getattr(self.state, 'no_owner_recovered', 0) + 1
                                                            break
                                                    except Exception as refetch_err:
                                                        self.logger.debug(f"Refetch error: {refetch_err}")
                                                        continue
                                            
                                            # 🔧 v6.1 FIX: Only log success if owners were actually found
                                            if owners:
                                                self._add_log(f"✓ Sy:{survey_no} H:{hissa} [{len(owners)} owners] Period: {period[:30]}")
                                            else:
                                                # ALL re-fetches returned empty - genuinely no data OR persistent portal issue
                                                self._add_log(f"⚠️ Sy:{survey_no} H:{hissa} NO OWNERS after {getattr(Config, 'NO_OWNER_REFETCH_RETRIES', 3)} re-fetches")
                                                # Save as potential skipped item for tracking
                                                skip_record = {
                                                    'village': village_name,
                                                    'village_code': village_code,
                                                    'survey_no': survey_no,
                                                    'surnoc': surnoc,
                                                    'hissa': hissa,
                                                    'period': period[:50] if period else '',
                                                    'reason': f'No owner data extracted after {getattr(Config, "NO_OWNER_REFETCH_RETRIES", 3)} re-fetches (likely portal overload or genuinely empty hissa)',
                                                    'timestamp': datetime.now().isoformat()
                                                }
                                                skipped_in_village.append(skip_record)
                                                with self.state_lock:
                                                    self.state.skipped_surveys.append(skip_record)
                                                if self.db and self.session_id:
                                                    try:
                                                        self.db.save_skipped_item(
                                                            session_id=self.session_id,
                                                            village_name=village_name,
                                                            survey_no=survey_no,
                                                            surnoc=surnoc,
                                                            hissa=hissa,
                                                            period=period[:50] if period else '',
                                                            error='No owner data after re-fetch retries'
                                                        )
                                                    except Exception:
                                                        pass
                                            
                                            period_selected = True
                                            
                                            for owner in owners:
                                                record = LandRecord(
                                                    district=district_name,
                                                    taluk=taluk_name,
                                                    hobli=hobli_name,
                                                    village=village_name,
                                                    survey_no=survey_no,
                                                    surnoc=surnoc,
                                                    hissa=hissa,
                                                    period=period,
                                                    owner_name=owner['owner_name'],
                                                    extent=owner['extent'],
                                                    owner_seq=owner.get('owner_seq', 0),
                                                    worker_id=self.worker_id
                                                )
                                                
                                                record_dict = asdict(record)
                                                
                                                # Check for match
                                                is_match = any(_norm_for_match(v) in _norm_for_match(owner['owner_name']) for v in owner_variants if v)
                                                
                                                # SAVE TO PERSISTENT DATABASE (REAL-TIME)
                                                try:
                                                    if self.db and self.session_id:
                                                        self.db.save_record(self.session_id, record_dict, is_match=is_match)
                                                except Exception as db_err:
                                                    self.logger.error(f"DB save failed: {db_err}")
                                                    # Continue even if DB fails - CSV is backup
                                                
                                                # Write to CSV (backup - always succeeds)
                                                try:
                                                    self.all_records_writer.write_record(record_dict)
                                                except Exception as csv_err:
                                                    self.logger.error(f"CSV save failed: {csv_err}")
                                                
                                                self.records_found += 1
                                                
                                                # FIXED: Sync worker stats to shared state for UI display
                                                self._update_status(records_found=self.records_found)
                                                
                                                # Add to state for real-time UI display
                                                with self.state_lock:
                                                    self.state.all_records.append(record_dict)
                                                    if len(self.state.all_records) > 500:
                                                        self.state.all_records = self.state.all_records[-500:]
                                                
                                                if is_match:
                                                    self.matches_writer.write_record(record_dict)
                                                    self.matches_found += 1
                                                    # FIXED: Sync match count too
                                                    self._update_status(matches_found=self.matches_found)
                                                    with self.state_lock:
                                                        self.state.matches.append(record_dict)
                                                    self._add_log(f"🎯 MATCH: {owner['owner_name']} in {village_name} Sy:{survey_no}")
                                            
                                            # Successfully processed this period
                                            period_selected = True
                                            
                                            # Track period count for stats
                                            with self.state_lock:
                                                self.state.total_periods_processed += 1
                                            
                                            # Track hissa count for memory management
                                            self.hissa_processed_count += 1
                                            
                                            # MEMORY LEAK PREVENTION: Restart browser periodically
                                            if self.hissa_processed_count >= Config.MAX_HISSA_BEFORE_RESTART:
                                                elapsed = time.time() - self.last_browser_restart
                                                self._add_log(f"🔄 Memory cleanup: Restarting browser after {self.hissa_processed_count} hissas ({int(elapsed)}s)")
                                                try:
                                                    self._close_browser()
                                                    time.sleep(Config.BROWSER_RESTART_DELAY)
                                                    self._init_browser()
                                                    self.hissa_processed_count = 0
                                                    self.last_browser_restart = time.time()
                                                    self._add_log(f"✅ Browser restarted for memory cleanup")
                                                except Exception as restart_err:
                                                    self._add_log(f"⚠️ Browser restart failed: {str(restart_err)[:50]}")
                                            
                                            # If LATEST_PERIOD_ONLY or NOT processing all periods, stop after first success
                                            if Config.LATEST_PERIOD_ONLY or not Config.PROCESS_ALL_PERIODS:
                                                break
                                            # Otherwise, continue to process remaining periods
                                        
                                        except Exception as period_error:
                                            # This period had an error
                                            error_msg = str(period_error)[:50]
                                            
                                            if Config.LATEST_PERIOD_ONLY:
                                                # 🔧 v6.0 FIX: Latest period failed - save to skipped IMMEDIATELY
                                                self._add_log(f"⚠️ Latest period failed for Sy:{survey_no} H:{hissa}: {error_msg}")
                                                self.errors += 1
                                                
                                                # 🔧 v6.0 FIX: Save period failure to skipped surveys
                                                skip_record = {
                                                    'village': village_name,
                                                    'village_code': village_code,
                                                    'survey_no': survey_no,
                                                    'surnoc': surnoc,
                                                    'hissa': hissa,
                                                    'period': period[:50] if period else '',
                                                    'reason': f'Period selection/fetch failed: {error_msg}',
                                                    'timestamp': datetime.now().isoformat()
                                                }
                                                skipped_in_village.append(skip_record)
                                                with self.state_lock:
                                                    self.state.skipped_surveys.append(skip_record)
                                                if self.db and self.session_id:
                                                    try:
                                                        self.db.save_skipped_item(
                                                            session_id=self.session_id,
                                                            village_name=village_name,
                                                            survey_no=survey_no,
                                                            surnoc=surnoc,
                                                            hissa=hissa,
                                                            period=period[:50] if period else '',
                                                            error=f'Period failed: {error_msg}'
                                                        )
                                                    except Exception:
                                                        pass
                                                # Don't continue - LATEST_PERIOD_ONLY means we're done with this hissa
                                                break
                                            elif Config.PROCESS_ALL_PERIODS:
                                                # When processing all periods, log each error but continue
                                                self.logger.debug(f"Period {period} error: {error_msg}")
                                                continue
                                            elif period_idx < max_period_attempts - 1:
                                                # Speed mode: silently continue to next period
                                                continue
                                            else:
                                                # Last attempt failed - log and save it
                                                self._add_log(f"⚠️ All periods failed for Sy:{survey_no} H:{hissa}")
                                                self.errors += 1
                                                
                                                # 🔧 v6.0 FIX: Save to skipped surveys
                                                skip_record = {
                                                    'village': village_name,
                                                    'village_code': village_code,
                                                    'survey_no': survey_no,
                                                    'surnoc': surnoc,
                                                    'hissa': hissa,
                                                    'period': '',
                                                    'reason': f'All {max_period_attempts} periods failed: {error_msg}',
                                                    'timestamp': datetime.now().isoformat()
                                                }
                                                skipped_in_village.append(skip_record)
                                                with self.state_lock:
                                                    self.state.skipped_surveys.append(skip_record)
                                                if self.db and self.session_id:
                                                    try:
                                                        self.db.save_skipped_item(
                                                            session_id=self.session_id,
                                                            village_name=village_name,
                                                            survey_no=survey_no,
                                                            surnoc=surnoc,
                                                            hissa=hissa,
                                                            period='',
                                                            error=f'All periods failed: {error_msg}'
                                                        )
                                                    except Exception:
                                                        pass
                                    
                                    if not period_selected:
                                        # 🔧 v6.0 FIX: No period could be selected - save to skipped
                                        self._add_log(f"⚠️ No period selected for Sy:{survey_no} S:{surnoc} H:{hissa}")
                                        
                                        # Only save if we haven't already saved in the error handler above
                                        # Check if this hissa was already added to skipped_in_village
                                        already_logged = any(
                                            s.get('survey_no') == survey_no and 
                                            s.get('surnoc') == surnoc and 
                                            s.get('hissa') == hissa 
                                            for s in skipped_in_village[-5:]  # Check last 5 entries
                                        )
                                        
                                        if not already_logged:
                                            skip_record = {
                                                'village': village_name,
                                                'village_code': village_code,
                                                'survey_no': survey_no,
                                                'surnoc': surnoc,
                                                'hissa': hissa,
                                                'period': '',
                                                'reason': 'No period could be selected (unknown error)',
                                                'timestamp': datetime.now().isoformat()
                                            }
                                            skipped_in_village.append(skip_record)
                                            with self.state_lock:
                                                self.state.skipped_surveys.append(skip_record)
                                            if self.db and self.session_id:
                                                try:
                                                    self.db.save_skipped_item(
                                                        session_id=self.session_id,
                                                        village_name=village_name,
                                                        survey_no=survey_no,
                                                        surnoc=surnoc,
                                                        hissa=hissa,
                                                        period='',
                                                        error='No period could be selected'
                                                    )
                                                except Exception:
                                                    pass
                                    
                                    # Update stats after processing all periods for this hissa
                                    self._update_status(
                                        records_found=self.records_found,
                                        matches_found=self.matches_found
                                    )
                                    self._update_global_stats()
                                    
                                    # Successfully processed this hissa - break retry loop
                                    break
                                    
                                except Exception as hissa_error:
                                    hissa_retry_count += 1
                                    error_msg = str(hissa_error)[:50]
                                    
                                    if hissa_retry_count <= max_hissa_retries:
                                        self._add_log(f"🔄 Retry {hissa_retry_count}/{max_hissa_retries} for Hissa {hissa}: {error_msg}")
                                        # Reload page for retry (Playwright)
                                        try:
                                            self.page.goto(Config.SERVICE2_URL)
                                            self.page.wait_for_load_state('domcontentloaded')
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            self._pw_select(IDS['district'], self.params['district_code'])
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            self._pw_select(IDS['taluk'], self.params['taluk_code'])
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            self._pw_select(IDS['hobli'], hobli_code)
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            self._pw_select(IDS['village'], village_code)
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            self.page.fill(f'#{IDS["survey_no"]}', str(survey_no))
                                            self.page.click(f'#{IDS["go_btn"]}')
                                            self.page.wait_for_load_state('domcontentloaded')
                                            time.sleep(Config.POST_CLICK_WAIT)
                                            self._pw_select_text(IDS['surnoc'], surnoc)
                                            time.sleep(Config.POST_SELECT_WAIT)
                                        except Exception as retry_err:
                                            self.logger.debug(f"Retry setup failed: {retry_err}")
                                    else:
                                        self._add_log(f"❌ Max retries for Hissa {hissa}, skipping")
                                        self.errors += 1
                                        # TRACK THIS GAP - hissa processing failed after retries
                                        skip_record = {
                                            'village': village_name,
                                            'village_code': village_code,
                                            'survey_no': survey_no,
                                            'surnoc': surnoc,
                                            'hissa': hissa,
                                            'period': '',
                                            'reason': f'Hissa processing failed after {max_hissa_retries} retries: {error_msg}',
                                            'timestamp': datetime.now().isoformat()
                                        }
                                        skipped_in_village.append(skip_record)
                                        with self.state_lock:
                                            self.state.skipped_surveys.append(skip_record)
                                        if self.db and self.session_id:
                                            try:
                                                self.db.save_skipped_item(
                                                    session_id=self.session_id,
                                                    village_name=village_name,
                                                    survey_no=survey_no,
                                                    surnoc=surnoc,
                                                    hissa=hissa,
                                                    period='',
                                                    error=f'Hissa failed after {max_hissa_retries} retries: {error_msg}'
                                                )
                                            except Exception:
                                                pass
                                
                    except Exception as surnoc_error:
                        error_msg = str(surnoc_error)[:40]
                        self._add_log(f"⚠️ Surnoc error Sy:{survey_no} S:{surnoc}: {error_msg}")
                        self.errors += 1
                        # TRACK THIS GAP - entire surnoc failed
                        skip_record = {
                            'village': village_name,
                            'village_code': village_code,
                            'survey_no': survey_no,
                            'surnoc': surnoc,
                            'hissa': '*',  # All hissas in this surnoc
                            'period': '',
                            'reason': f'Surnoc processing error: {error_msg}',
                            'timestamp': datetime.now().isoformat()
                        }
                        skipped_in_village.append(skip_record)
                        with self.state_lock:
                            self.state.skipped_surveys.append(skip_record)
                        if self.db and self.session_id:
                            try:
                                self.db.save_skipped_item(
                                    session_id=self.session_id,
                                    village_name=village_name,
                                    survey_no=survey_no,
                                    surnoc=surnoc,
                                    hissa='*',
                                    period='',
                                    error=f'Surnoc error: {error_msg}'
                                )
                            except Exception:
                                pass
                        continue
                
                # ═══════════════════════════════════════════════════════════════════════
                # SUCCESSFULLY PROCESSED SURVEY - Save checkpoint and move to next
                # ═══════════════════════════════════════════════════════════════════════
                
                # Save survey-level checkpoint for granular resume capability
                if self.db and self.session_id:
                    try:
                        self.db.save_survey_checkpoint(
                            session_id=self.session_id,
                            village_code=f"{hobli_code}:{village_code}",  # v12: hobli-qualified
                            survey_no=survey_no,
                            surnocs_processed=surnoc_opts  # Save which surnocs were processed
                        )
                    except Exception as chkpt_err:
                        self.logger.debug(f"Checkpoint save failed: {chkpt_err}")
                
                survey_no += 1
                        
            except Exception as e:
                error_str = str(e).lower()
                
                # ═══════════════════════════════════════════════════════════════════════
                # CRITICAL: Detect browser death (invalid session id) vs session expiry
                # ═══════════════════════════════════════════════════════════════════════
                if 'invalid session id' in error_str or 'no such session' in error_str:
                    # Browser is DEAD - must restart it completely
                    self._add_log(f"💀 BROWSER DIED at survey {survey_no}! Restarting...")
                    browser_restart_attempts = 0
                    max_restart_attempts = 3
                    
                    while browser_restart_attempts < max_restart_attempts:
                        try:
                            self._close_browser()
                            time.sleep(2)
                            self._init_browser()
                            self._add_log(f"✅ Browser restarted! Retrying survey {survey_no}")
                            session_retries = 0  # Reset session retries
                            break  # Successfully restarted
                        except Exception as restart_err:
                            browser_restart_attempts += 1
                            self._add_log(f"❌ Browser restart attempt {browser_restart_attempts} failed: {str(restart_err)[:30]}")
                            time.sleep(3)
                    
                    if browser_restart_attempts >= max_restart_attempts:
                        self._add_log(f"❌ Could not restart browser after {max_restart_attempts} attempts. Stopping village.")
                        # TRACK THIS GAP - browser died, remaining surveys not processed
                        remaining_surveys = max_survey - survey_no
                        skip_record = {
                            'village': village_name,
                            'village_code': village_code,
                            'survey_no': survey_no,
                            'surnoc': '*',
                            'hissa': '*',
                            'period': '',
                            'reason': f'Browser died - {remaining_surveys} surveys from {survey_no} to {max_survey} not processed',
                            'timestamp': datetime.now().isoformat()
                        }
                        skipped_in_village.append(skip_record)
                        with self.state_lock:
                            self.state.skipped_surveys.append(skip_record)
                        if self.db and self.session_id:
                            try:
                                self.db.save_skipped_item(
                                    session_id=self.session_id,
                                    village_name=village_name,
                                    survey_no=survey_no,
                                    surnoc='*',
                                    hissa='*',
                                    period='',
                                    error=f'Browser died - surveys {survey_no}-{max_survey} not processed'
                                )
                            except Exception:
                                pass
                        completion_reason = 'browser_death'
                        break  # Exit village loop - browser is dead
                    
                    continue  # RETRY same survey with new browser
                
                elif 'session' in error_str or 'expired' in error_str:
                    # Session expired but browser may be alive - try refresh first
                    self._add_log(f"⚠️ Session expired at survey {survey_no}")
                    if session_retries < Config.MAX_SESSION_RETRIES:
                        session_retries += 1
                        try:
                            self._refresh_session()
                            continue  # RETRY same survey
                        except Exception as refresh_err:
                            # Refresh failed - browser might be dead, restart it
                            self._add_log(f"🔄 Session refresh failed ({type(refresh_err).__name__}), restarting browser...")
                            self._close_browser()
                            time.sleep(2)
                            try:
                                self._init_browser()
                                continue  # RETRY same survey with new browser
                            except Exception as init_err:
                                self._add_log(f"❌ Browser restart failed: {type(init_err).__name__}")
                                # TRACK THIS GAP - browser restart failed during session recovery
                                remaining_surveys = max_survey - survey_no
                                skip_record = {
                                    'village': village_name,
                                    'village_code': village_code,
                                    'survey_no': survey_no,
                                    'surnoc': '*',
                                    'hissa': '*',
                                    'period': '',
                                    'reason': f'Browser restart failed during session recovery - {remaining_surveys} surveys not processed',
                                    'timestamp': datetime.now().isoformat()
                                }
                                skipped_in_village.append(skip_record)
                                with self.state_lock:
                                    self.state.skipped_surveys.append(skip_record)
                                if self.db and self.session_id:
                                    try:
                                        self.db.save_skipped_item(
                                            session_id=self.session_id,
                                            village_name=village_name,
                                            survey_no=survey_no,
                                            surnoc='*',
                                            hissa='*',
                                            period='',
                                            error=f'Browser restart failed - surveys {survey_no}-{max_survey} not processed'
                                        )
                                    except Exception:
                                        pass
                                completion_reason = 'browser_restart_failure'
                                break  # Exit village loop
                    else:
                        # Max retries reached - try browser restart as last resort
                        self._add_log(f"⚠️ Max session retries reached, restarting browser...")
                        self._close_browser()
                        time.sleep(2)
                        try:
                            self._init_browser()
                            session_retries = 0
                            continue  # RETRY with fresh browser
                        except Exception as init_err:
                            self._add_log(f"❌ Browser restart failed after max retries: {type(init_err).__name__}")
                            # TRACK THIS GAP - session expired and browser restart failed
                            remaining_surveys = max_survey - survey_no
                            skip_record = {
                                'village': village_name,
                                'village_code': village_code,
                                'survey_no': survey_no,
                                'surnoc': '*',
                                'hissa': '*',
                                'period': '',
                                'reason': f'Session expired, browser restart failed - {remaining_surveys} surveys from {survey_no} to {max_survey} not processed',
                                'timestamp': datetime.now().isoformat()
                            }
                            skipped_in_village.append(skip_record)
                            with self.state_lock:
                                self.state.skipped_surveys.append(skip_record)
                            if self.db and self.session_id:
                                try:
                                    self.db.save_skipped_item(
                                        session_id=self.session_id,
                                        village_name=village_name,
                                        survey_no=survey_no,
                                        surnoc='*',
                                        hissa='*',
                                        period='',
                                        error=f'Session expired, browser restart failed - surveys {survey_no}-{max_survey} not processed'
                                    )
                                except Exception:
                                    pass
                            completion_reason = 'session_failure'
                            break
                
                # ═══════════════════════════════════════════════════════════════════════
                # 🔧 v6.2 FIX: NETWORK ERROR DETECTION - WAIT AND RETRY, DON'T SKIP!
                # ═══════════════════════════════════════════════════════════════════════
                elif any(net_err in error_str for net_err in [
                    'net::err_internet_disconnected',
                    'net::err_network_changed', 
                    'net::err_name_not_resolved',
                    'net::err_connection_refused',
                    'net::err_connection_reset',
                    'net::err_connection_closed',
                    'net::err_connection_timed_out',
                    'net::err_timed_out',
                    'net::err_address_unreachable',
                    'timeout 20000ms exceeded',  # Playwright page timeout
                    'target page, context or browser has been closed',
                    # v9.1 FIX: Cert errors should also wait+retry rather than skip.
                    # The browser is now configured with ignore_https_errors=True so
                    # these shouldn't normally hit, but kept here as defense in depth.
                    'net::err_cert_date_invalid',
                    'net::err_cert_common_name_invalid',
                    'net::err_cert_authority_invalid',
                    'net::err_cert_revoked',
                    'net::err_ssl_protocol_error',
                ]):
                    # NETWORK ERROR - WAIT FOR RECOVERY, DON'T SKIP!
                    self._add_log(f"🌐 NETWORK ERROR at survey {survey_no}: {error_str[:40]}")
                    self._add_log(f"⏳ Waiting for network connectivity...")
                    
                    network_wait_attempts = 0
                    max_network_wait = 30  # Max 30 attempts (5 minutes total)
                    network_check_interval = 10  # Check every 10 seconds
                    
                    network_recovered = False
                    while network_wait_attempts < max_network_wait:
                        if not self.state.running:
                            self._add_log(f"⏹️ Search stopped during network wait")
                            return
                        
                        network_wait_attempts += 1
                        
                        # Wait before checking
                        time.sleep(network_check_interval)
                        
                        # Try a simple network check
                        try:
                            import requests
                            response = requests.head('https://landrecords.karnataka.gov.in', timeout=5, verify=False)
                            if response.status_code < 500:
                                self._add_log(f"✅ Network restored after {network_wait_attempts * network_check_interval}s!")
                                network_recovered = True
                                break
                        except Exception as ping_err:
                            if network_wait_attempts % 3 == 0:  # Log every 30 seconds
                                self._add_log(f"🔄 Network still down ({network_wait_attempts * network_check_interval}s) - waiting...")
                    
                    if network_recovered:
                        # Network is back - restart browser and RETRY THIS SURVEY
                        self._add_log(f"🔄 Restarting browser and retrying survey {survey_no}...")
                        try:
                            self._close_browser()
                            time.sleep(2)
                            self._init_browser()
                            continue  # RETRY same survey - don't increment!
                        except Exception as restart_err:
                            self._add_log(f"❌ Browser restart after network recovery failed: {str(restart_err)[:30]}")
                            # Fall through to skip this survey only
                    else:
                        # Network didn't recover in 5 minutes
                        self._add_log(f"❌ Network did not recover after {max_network_wait * network_check_interval}s")
                        # Save as skipped with clear reason
                        skip_record = {
                            'village': village_name,
                            'village_code': village_code,
                            'survey_no': survey_no,
                            'surnoc': '*',
                            'hissa': '*',
                            'period': '',
                            'reason': f'Network error - did not recover after {max_network_wait * network_check_interval}s wait',
                            'timestamp': datetime.now().isoformat()
                        }
                        skipped_in_village.append(skip_record)
                        with self.state_lock:
                            self.state.skipped_surveys.append(skip_record)
                        if self.db and self.session_id:
                            try:
                                self.db.save_skipped_item(
                                    session_id=self.session_id,
                                    village_name=village_name,
                                    survey_no=survey_no,
                                    surnoc='*',
                                    hissa='*',
                                    period='',
                                    error=f'Network error - no recovery after {max_network_wait * network_check_interval}s'
                                )
                            except Exception:
                                pass
                        survey_no += 1  # Only skip after exhausting network wait
                
                else:
                    # Other error - log and continue to next survey
                    self.errors += 1
                    # v7.1 FIX: Do NOT increment empty_count here. Errors are
                    # not empty surveys — inflating empty_count caused premature
                    # SMART STOP that abandoned entire villages.
                    skip_record = {
                        'village': village_name,
                        'village_code': village_code,
                        'survey_no': survey_no,
                        'surnoc': '*',
                        'hissa': '*',
                        'period': '',
                        'reason': f'Unknown error: {error_str[:50]}',
                        'timestamp': datetime.now().isoformat()
                    }
                    skipped_in_village.append(skip_record)
                    with self.state_lock:
                        self.state.skipped_surveys.append(skip_record)
                    if self.db and self.session_id:
                        try:
                            self.db.save_skipped_item(
                                session_id=self.session_id,
                                village_name=village_name,
                                survey_no=survey_no,
                                surnoc='*',
                                hissa='*',
                                period='',
                                error=f'Unknown error: {error_str[:50]}'
                            )
                        except Exception:
                            pass
                    survey_no += 1  # Move to next survey
        
        # ═══════════════════════════════════════════════════════════════════════════════
        # RETRY QUEUE PROCESSING - Second chance for skipped surveys
        # ═══════════════════════════════════════════════════════════════════════════════
        if Config.ENABLE_RETRY_QUEUE and retry_queue:
            self._add_log(f"🔄 Retrying {len(retry_queue)} skipped surveys for {village_name}...")
            
            # Wait before retrying (portal might be in better state now)
            time.sleep(5)
            
            retry_successes = 0
            final_skipped = []
            
            for retry_item in retry_queue:
                retry_survey_no = retry_item['survey_no']
                
                try:
                    # Check portal health before retry
                    wait_time = portal_health.should_wait()
                    if wait_time > 0:
                        time.sleep(wait_time)
                    
                    # Navigate and retry (Playwright)
                    self.page.goto(Config.SERVICE2_URL)
                    self.page.wait_for_load_state('domcontentloaded')
                    time.sleep(2)
                    
                    self._pw_select(IDS['district'], self.params['district_code'])
                    time.sleep(Config.POST_SELECT_WAIT)
                    self._pw_select(IDS['taluk'], self.params['taluk_code'])
                    time.sleep(Config.POST_SELECT_WAIT)
                    self._pw_select(IDS['hobli'], hobli_code)
                    time.sleep(Config.POST_SELECT_WAIT)
                    self._pw_select(IDS['village'], village_code)
                    time.sleep(Config.POST_SELECT_WAIT)
                    
                    self.page.fill(f'#{IDS["survey_no"]}', str(retry_survey_no))
                    
                    _rate_gate()
                    self.page.click(f'#{IDS["go_btn"]}')
                    self.page.wait_for_load_state('domcontentloaded')
                    time.sleep(Config.POST_CLICK_WAIT + 2)
                    
                    # Check for alert
                    had_alert, alert_text, is_portal_issue = self._handle_alert()
                    
                    if not is_portal_issue:
                        # Check if surnoc populated (Playwright)
                        surnoc_opts = self._pw_get_dropdown_options(IDS['surnoc'])
                        
                        if surnoc_opts:
                            # SUCCESS! Survey is now accessible
                            # NOTE: For full accuracy, we should extract all surnocs/hissas/periods/owners
                            # Currently this is a simplified check - just verifies accessibility
                            # TODO: Refactor survey processing into reusable method
                            
                            self._add_log(f"⚠️ RETRY PARTIAL SUCCESS: Survey {retry_survey_no} accessible (not fully processed)")
                            
                            # Don't count as full success since we're not extracting data
                            # Add to final_skipped with note
                            final_skipped.append({
                                **retry_item,
                                'reason_updated': 'Accessible but not fully processed in retry pass'
                            })
                            continue
                    
                    # Still failing - add to final skipped
                    final_skipped.append(retry_item)
                    
                except Exception as retry_err:
                    self._add_log(f"⚠️ Retry failed for Sy:{retry_survey_no}: {str(retry_err)[:30]}")
                    final_skipped.append(retry_item)
            
            # Now add final skipped to the permanent skip list
            for item in final_skipped:
                skip_record = {
                    'village': village_name,
                    'village_code': village_code,
                    'survey_no': item['survey_no'],
                    'reason': f'RTC access issue after {Config.MAX_PORTAL_RETRIES} retries + 1 retry pass',
                    'timestamp': datetime.now().isoformat()
                }
                with self.state_lock:
                    self.state.skipped_surveys.append(skip_record)
                
                # PERSIST to database
                if self.db and self.session_id:
                    try:
                        self.db.save_skipped_item(
                            session_id=self.session_id,
                            village_name=village_name,
                            survey_no=item['survey_no'],
                            surnoc='',
                            hissa='',
                            period='',
                            error=f'RTC access issue after {Config.MAX_PORTAL_RETRIES} retries + retry pass'
                        )
                    except Exception as skip_err:
                        self.logger.debug(f"Failed to save final skipped item: {skip_err}")
            
            if retry_successes > 0:
                self._add_log(f"🎉 Retry pass: {retry_successes}/{len(retry_queue)} surveys recovered!")
            
            # Update skipped count with final results
            skipped_in_village = final_skipped
        
        # ═══════════════════════════════════════════════════════════════════════════════
        # VILLAGE COMPLETION STATS - Comprehensive tracking for user confidence
        # ═══════════════════════════════════════════════════════════════════════════════
        
        # Calculate confidence score
        confidence_score = self._calculate_village_confidence(
            surveys_checked=surveys_checked,
            surveys_with_data=surveys_with_data,
            last_survey_with_data=last_survey_with_data,
            stopped_at_survey=survey_no,
            skipped_count=len(skipped_in_village),
            completion_reason=completion_reason,
            max_survey=max_survey
        )
        
        # Build village stats
        village_completion = {
            'village_name': village_name,
            'village_code': village_code,
            'surveys_checked': surveys_checked,
            'surveys_with_data': surveys_with_data,
            'records_found': self.records_found,
            'matches_found': self.matches_found,
            'last_survey_with_data': last_survey_with_data,
            'stopped_at_survey': survey_no,
            'completion_reason': completion_reason,
            'skipped_count': len(skipped_in_village),
            'skipped_surveys': skipped_in_village[-10:] if skipped_in_village else [],  # Last 10
            'confidence_score': confidence_score,
            'confidence_level': 'HIGH' if confidence_score >= 80 else ('MEDIUM' if confidence_score >= 50 else 'LOW'),
            'time_saved_surveys': max_survey - survey_no if completion_reason == 'smart_stop' else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        # Store village stats
        with self.state_lock:
            self.state.village_stats[village_code] = village_completion
        
        # End of village summary with confidence
        confidence_emoji = '🟢' if confidence_score >= 80 else ('🟡' if confidence_score >= 50 else '🔴')
        self._add_log(f"✅ {village_name} COMPLETE: {surveys_checked} surveys, {surveys_with_data} with data, {self.records_found} records")
        self._add_log(f"   {confidence_emoji} Confidence: {confidence_score}% ({village_completion['confidence_level']})")
    
    def run(self):
        """Main worker execution with browser crash recovery"""
        self._update_status(status='running', villages_total=len(self.villages))
        self._add_log(f"Starting with {len(self.villages)} villages")
        
        browser_crashes = 0
        max_browser_crashes = 3
        
        try:
            self._init_browser()
            
            idx = 0
            while idx < len(self.villages):
                if not self.state.running:
                    self._add_log("Stopped by user")
                    break
                
                village_code, village_name, hobli_code, hobli_name = self.villages[idx]
                
                try:
                    self._add_log(f"🏘️ Village {idx+1}/{len(self.villages)}: {village_name}")
                    self._search_village(village_code, village_name, hobli_code, hobli_name)
                    
                    # ═══════════════════════════════════════════════════════════════════════
                    # SUCCESSFULLY PROCESSED - Track it!
                    # ═══════════════════════════════════════════════════════════════════════
                    with self.state_lock:
                        if village_name not in self.state.villages_processed:
                            self.state.villages_processed.append(village_name)
                    
                    self._update_status(villages_completed=idx + 1)
                    self._update_global_stats()
                    idx += 1  # Move to next village
                    browser_crashes = 0  # Reset crash count on success
                    
                except Exception as village_error:
                    error_str = str(village_error).lower()
                    self._add_log(f"⚠️ Village error: {str(village_error)[:80]}")
                    
                    # Check if it's a browser/session crash
                    if any(x in error_str for x in ['session', 'chrome', 'browser', 'expired', 'webdriver']):
                        browser_crashes += 1
                        self._add_log(f"🔄 Browser/session issue #{browser_crashes}/{max_browser_crashes}")
                        
                        # Track retried villages
                        with self.state_lock:
                            if village_name not in self.state.villages_retried:
                                self.state.villages_retried.append(village_name)
                            self.state.session_recoveries += 1
                        
                        # Try to restart browser and RETRY the same village
                        self._close_browser()
                        time.sleep(3)
                        
                        try:
                            self._init_browser()
                            
                            # Only skip village after max retries
                            if browser_crashes >= max_browser_crashes:
                                self._add_log(f"❌ Max retries reached for {village_name}, moving to next")
                                # Track failed village
                                with self.state_lock:
                                    if village_name not in self.state.villages_failed:
                                        self.state.villages_failed.append(village_name)
                                idx += 1
                                browser_crashes = 0
                            else:
                                self._add_log(f"🔁 Retrying village {village_name}...")
                                # Don't increment idx - retry same village
                                
                        except Exception as restart_error:
                            self._add_log(f"❌ Browser restart failed: {str(restart_error)[:50]}")
                            # Still retry same village with new browser attempt
                            time.sleep(5)
                    else:
                        # v12.2 (audit H4): a non-browser error previously skipped the
                        # village with only a log line — no villages_failed entry, no
                        # retry queue, invisible to the coverage audit. Now the village
                        # is tracked as failed AND queued for the coverage pass, which
                        # re-enumerates it after Phase 1.
                        self.errors += 1
                        self._add_log(
                            f"❌ Village error ({str(village_error)[:50]}) — "
                            f"{village_name} queued for coverage re-run"
                        )
                        with self.state_lock:
                            if village_name not in self.state.villages_failed:
                                self.state.villages_failed.append(village_name)
                            self.state.unfinished_villages.append(
                                (village_code, village_name, hobli_code, hobli_name)
                            )
                        if self.db and self.session_id:
                            try:
                                self.db.fail_village(self.session_id, f"{hobli_code}:{village_code}",
                                                     str(village_error)[:200])
                            except Exception:
                                pass
                        idx += 1
            
            self._update_status(status='completed')
            self._add_log(f"✅ Completed: {self.records_found} records, {self.matches_found} matches")
            
        except Exception as e:
            self._update_status(status='failed', errors=self.errors + 1)
            self._add_log(f"Error: {str(e)[:100]}")
            self.logger.error(f"Worker failed: {traceback.format_exc()}")
            
        finally:
            self._close_browser()
            self._update_global_stats()
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # PHASE 2: RETRY SKIPPED SURVEYS
    # Standalone retry method — does NOT modify Phase 1 code paths.
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def run_phase2(self, skipped_items: List[Dict], village_hobli_map: Dict):
        """
        Phase 2: Retry all skipped surveys from Phase 1.
        
        This is a self-contained retry loop that navigates the portal and
        re-attempts each skipped survey/hissa/period. It saves results to
        the same DB session and CSV files as Phase 1.
        
        Args:
            skipped_items: List of skip_record dicts from Phase 1
            village_hobli_map: {village_code: {'hobli_code', 'hobli_name', 'village_name'}}
        """
        from playwright.sync_api import TimeoutError as PlaywrightTimeout
        
        IDS = Config.ELEMENT_IDS
        owner_variants = self.state.owner_variants
        district_name = self.params.get('district_name', 'Unknown')
        taluk_name = self.params.get('taluk_name', 'Unknown')
        
        # Group skipped items by (village_code, survey_no) for efficient navigation
        village_surveys = {}
        for item in skipped_items:
            vc = item.get('village_code', '')
            if not vc or vc not in village_hobli_map:
                continue
            if vc not in village_surveys:
                village_surveys[vc] = {}
            sno = item.get('survey_no', 0)
            if sno not in village_surveys[vc]:
                village_surveys[vc][sno] = []
            village_surveys[vc][sno].append(item)
        
        total_surveys = sum(len(surveys) for surveys in village_surveys.values())
        self._add_log(f"Phase 2: {len(skipped_items)} skipped items -> {total_surveys} unique surveys across {len(village_surveys)} villages")
        
        try:
            self._init_browser()
        except Exception as e:
            self._add_log(f"Phase 2: Browser init failed: {str(e)[:50]}")
            return
        
        try:
            for village_code, surveys_dict in village_surveys.items():
                if not self.state.running:
                    self._add_log("Phase 2: Stopped by user")
                    break
                
                vinfo = village_hobli_map[village_code]
                village_name = vinfo['village_name']
                hobli_code = vinfo['hobli_code']
                hobli_name = vinfo['hobli_name']
                
                self._add_log(f"Phase 2: Retrying {len(surveys_dict)} surveys in {village_name}")
                
                for survey_no in sorted(surveys_dict.keys()):
                    if not self.state.running:
                        break
                    
                    items_for_survey = surveys_dict[survey_no]
                    is_full_retry = any(
                        item.get('surnoc', '') in ('', '*')
                        for item in items_for_survey
                    )
                    
                    for attempt in range(Config.PHASE2_MAX_ATTEMPTS):
                        try:
                            # Navigate to portal fresh
                            self.page.goto(Config.SERVICE2_URL)
                            self.page.wait_for_load_state('domcontentloaded')
                            time.sleep(Config.POST_SELECT_WAIT)
                            
                            # Select location dropdowns
                            self._pw_select(IDS['district'], self.params['district_code'])
                            time.sleep(Config.POST_SELECT_WAIT)
                            self._pw_select(IDS['taluk'], self.params['taluk_code'])
                            time.sleep(Config.POST_SELECT_WAIT)
                            self._pw_select(IDS['hobli'], hobli_code)
                            time.sleep(Config.POST_SELECT_WAIT)
                            self._pw_select(IDS['village'], village_code)
                            time.sleep(Config.POST_SELECT_WAIT)
                            
                            # Enter survey number and click GO
                            self.page.fill(f'#{IDS["survey_no"]}', str(survey_no))
                            _rate_gate()
                            self.page.evaluate(f'document.getElementById("{IDS["go_btn"]}").click()')
                            self.page.wait_for_load_state('domcontentloaded')
                            time.sleep(Config.POST_CLICK_WAIT)
                            
                            # Check for portal alerts
                            had_alert, alert_text, is_portal_issue = self._handle_alert()
                            if is_portal_issue:
                                wait_time = portal_health.should_wait()
                                if wait_time > 0:
                                    time.sleep(wait_time)
                                else:
                                    time.sleep(Config.PHASE2_DELAY_BETWEEN * (attempt + 1))
                                continue  # Retry this survey
                            
                            # Get surnoc options
                            surnoc_opts = self._pw_get_dropdown_options(IDS['surnoc'])
                            if not surnoc_opts:
                                break  # Genuinely empty survey — no point retrying
                            
                            # Determine what to process
                            if is_full_retry:
                                target_surnocs = surnoc_opts
                            else:
                                target_surnocs_set = set(item.get('surnoc', '') for item in items_for_survey)
                                target_surnocs = [s for s in surnoc_opts if s in target_surnocs_set] or surnoc_opts
                            
                            records_this_survey = 0
                            
                            for surnoc in target_surnocs:
                                if not self.state.running:
                                    break
                                try:
                                    self._pw_select_text(IDS['surnoc'], surnoc)
                                    time.sleep(Config.POST_SELECT_WAIT + 1)
                                    
                                    hissa_opts = self._pw_get_dropdown_options(IDS['hissa'])
                                    
                                    # Filter hissa if we have specific targets
                                    if not is_full_retry:
                                        target_hissas = set(
                                            item.get('hissa', '') for item in items_for_survey
                                            if item.get('surnoc', '') == surnoc and item.get('hissa', '') not in ('', '*')
                                        )
                                        if target_hissas:
                                            hissa_opts = [h for h in hissa_opts if h in target_hissas] or hissa_opts
                                    
                                    for hissa in hissa_opts:
                                        if not self.state.running:
                                            break
                                        try:
                                            self._pw_select_text(IDS['hissa'], hissa)
                                            time.sleep(Config.POST_SELECT_WAIT + 2)
                                            
                                            period_opts = self._pw_get_dropdown_options(IDS['period'])
                                            if not period_opts:
                                                continue
                                            
                                            # Process latest period only (consistent with Phase 1)
                                            max_periods = 1 if Config.LATEST_PERIOD_ONLY else len(period_opts)
                                            
                                            for pidx in range(min(max_periods, len(period_opts))):
                                                period = period_opts[pidx]
                                                try:
                                                    self._pw_select_text(IDS['period'], period)
                                                    time.sleep(1)
                                                    
                                                    _rate_gate()
                                                    self.page.evaluate(f'document.getElementById("{IDS["fetch_btn"]}").click()')
                                                    self.page.wait_for_load_state('domcontentloaded')
                                                    time.sleep(Config.POST_CLICK_WAIT)
                                                    
                                                    had_alert, alert_text, is_portal_issue = self._handle_alert()
                                                    if is_portal_issue:
                                                        continue
                                                    
                                                    page_source = self.page.content()
                                                    owners = self._extract_owners(page_source)
                                                    
                                                    for owner in owners:
                                                        record = LandRecord(
                                                            district=district_name,
                                                            taluk=taluk_name,
                                                            hobli=hobli_name,
                                                            village=village_name,
                                                            survey_no=survey_no,
                                                            surnoc=surnoc,
                                                            hissa=hissa,
                                                            period=period,
                                                            owner_name=owner['owner_name'],
                                                            extent=owner['extent'],
                                                            owner_seq=owner.get('owner_seq', 0),
                                                            worker_id=self.worker_id
                                                        )
                                                        record_dict = asdict(record)
                                                        is_match = any(
                                                            _norm_for_match(v) in _norm_for_match(owner['owner_name'])
                                                            for v in owner_variants if v
                                                        )
                                                        
                                                        # DB INSERT OR IGNORE handles dedup
                                                        try:
                                                            if self.db and self.session_id:
                                                                self.db.save_record(self.session_id, record_dict, is_match=is_match)
                                                        except Exception:
                                                            pass
                                                        
                                                        try:
                                                            self.all_records_writer.write_record(record_dict)
                                                        except Exception:
                                                            pass
                                                        
                                                        records_this_survey += 1
                                                        self.records_found += 1
                                                        
                                                        with self.state_lock:
                                                            self.state.phase2_records_added += 1
                                                        
                                                        if is_match:
                                                            try:
                                                                self.matches_writer.write_record(record_dict)
                                                            except Exception:
                                                                pass
                                                            self.matches_found += 1
                                                            self._add_log(f"Phase 2 MATCH: {owner['owner_name']} Sy:{survey_no}")
                                                    
                                                    if Config.LATEST_PERIOD_ONLY:
                                                        break
                                                
                                                except Exception as period_err:
                                                    self.logger.debug(f"Phase 2 period error: {period_err}")
                                        
                                        except Exception as hissa_err:
                                            self.logger.debug(f"Phase 2 hissa error: {hissa_err}")
                                
                                except Exception as surnoc_err:
                                    self.logger.debug(f"Phase 2 surnoc error: {surnoc_err}")
                            
                            # Mark outcome
                            with self.state_lock:
                                self.state.phase2_attempted += 1
                                if records_this_survey > 0:
                                    self.state.phase2_recovered += 1
                                    self._add_log(f"Phase 2 RECOVERED: Sy:{survey_no} in {village_name} ({records_this_survey} records)")
                                else:
                                    self.state.phase2_failed += 1
                            
                            break  # Success or genuinely empty — don't retry
                        
                        except Exception as retry_err:
                            self._add_log(f"Phase 2: Attempt {attempt+1} failed for Sy:{survey_no}: {str(retry_err)[:40]}")
                            if attempt == Config.PHASE2_MAX_ATTEMPTS - 1:
                                with self.state_lock:
                                    self.state.phase2_attempted += 1
                                    self.state.phase2_failed += 1
                            time.sleep(Config.PHASE2_DELAY_BETWEEN * (attempt + 1))
                    
                    time.sleep(Config.PHASE2_DELAY_BETWEEN)
        
        except Exception as phase2_err:
            self._add_log(f"Phase 2: Fatal error: {str(phase2_err)[:60]}")
            self.logger.error(f"Phase 2 fatal: {traceback.format_exc()}")
        
        finally:
            self._close_browser()
            self._add_log(f"Phase 2 worker done: {self.state.phase2_recovered} recovered, {self.state.phase2_failed} still failed")

# ═══════════════════════════════════════════════════════════════════════════════════════
# PARALLEL SEARCH COORDINATOR
# ═══════════════════════════════════════════════════════════════════════════════════════

class ParallelSearchCoordinator:
    """
    Coordinates parallel search across multiple workers.
    Handles village distribution, worker management, and result aggregation.
    
    NOW WITH PERSISTENT DATABASE:
    - Creates session at start
    - All records saved to SQLite in real-time
    - Supports resume from crashes
    """
    
    def __init__(self):
        self.state = SearchState()
        self.state_lock = threading.Lock()
        self.executor: Optional[ThreadPoolExecutor] = None
        self.workers: List[SearchWorker] = []
        self.all_records_writer: Optional[ThreadSafeCSVWriter] = None
        self.matches_writer: Optional[ThreadSafeCSVWriter] = None
        self.api = BhoomiAPI()
        
        # Database integration
        self.db = get_database()
        self.current_session_id: Optional[str] = None
        
        # Enterprise features
        self.state_manager: Optional[StateManager] = None
        self.portal_state_monitor_thread: Optional[threading.Thread] = None
        self._stop_portal_monitor = threading.Event()
        
        # v11.0: Service154 skeleton store ({village_code: [{survey_no, surnoc, ...}]})
        # Always initialized so workers can safely read it before any prefetch.
        self.skeletons: Dict[str, List[Dict]] = {}

        # v12: guard against concurrent stop_search invocations (e.g. the dashboard tab
        # and an API client both POSTing stop) — live-test showed two stops 5s apart each
        # running the full drain+export path, producing duplicate export files.
        self._stop_lock = threading.Lock()

        # v12.1: villages queued by the watchdog per reaped worker, so a worker that
        # turns out to be alive (heartbeat resumes) can be resurrected and its villages
        # un-queued from the coverage pass.
        self._reaped_villages: Dict[int, List[Tuple]] = {}
    
    def _prepare_villages(self, params: dict) -> List[Tuple[str, str, str, str]]:
        """
        Prepare list of all villages to search.
        Returns: List of (village_code, village_name, hobli_code, hobli_name)
        
        STABILITY: Uses guaranteed browser cleanup to prevent memory leaks.
        """
        import shutil
        import tempfile
        from playwright.sync_api import sync_playwright
        
        logger.info("Preparing village list (Playwright)...")
        
        # Use Playwright
        # v9.1 FIX: Bypass SSL errors — Bhoomi portal cert is sometimes invalid.
        pw = sync_playwright().start()
        browser = pw.chromium.launch(
            headless=True,
            args=['--ignore-certificate-errors', '--ignore-ssl-errors']
        )
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()
        page.set_default_timeout(30000)  # Allow more time for slow networks
        
        try:
            logger.info(f"Loading portal: {Config.SERVICE2_URL}")
            page.goto(Config.SERVICE2_URL)
            page.wait_for_load_state('domcontentloaded')
            
            IDS = Config.ELEMENT_IDS
            
            logger.info("Waiting for page to fully load...")
            page.wait_for_selector(f'#{IDS["district"]}', timeout=20000)
            time.sleep(3)
            
            # Select district (Playwright)
            logger.info(f"Selecting district: {params.get('district_code')}")
            time.sleep(1)
            
            # Get district options
            dist_options = page.locator(f'#{IDS["district"]} option').all()
            dist_opts = {}
            for opt in dist_options:
                val = opt.get_attribute('value')
                if val:
                    dist_opts[val] = opt.text_content()
            
            logger.info(f"Found {len(dist_opts)} districts")
            params['district_name'] = dist_opts.get(params['district_code'], 'Unknown')
            
            # Handle "2" vs "2.0" formats
            district_code = params['district_code']
            if district_code not in dist_opts:
                district_code_float = f"{district_code}.0" if '.' not in str(district_code) else district_code
                if district_code_float in dist_opts:
                    district_code = district_code_float
            
            page.select_option(f'#{IDS["district"]}', value=district_code)
            time.sleep(3)
            
            # Select taluk (Playwright)
            logger.info(f"Selecting taluk: {params.get('taluk_code')}")
            time.sleep(1)
            
            taluk_options = page.locator(f'#{IDS["taluk"]} option').all()
            taluk_opts = {}
            for opt in taluk_options:
                val = opt.get_attribute('value')
                if val:
                    taluk_opts[val] = opt.text_content()
            
            logger.info(f"Found {len(taluk_opts)} taluks")
            params['taluk_name'] = taluk_opts.get(params['taluk_code'], 'Unknown')
            
            # Handle "5" vs "5.0" formats
            taluk_code = params['taluk_code']
            if taluk_code not in taluk_opts:
                taluk_code_float = f"{taluk_code}.0" if '.' not in str(taluk_code) else taluk_code
                if taluk_code_float in taluk_opts:
                    taluk_code = taluk_code_float
            
            page.select_option(f'#{IDS["taluk"]}', value=taluk_code)
            time.sleep(3)
            
            # Get all hoblis (Playwright)
            hobli_options = page.locator(f'#{IDS["hobli"]} option').all()
            all_hoblis = []
            for opt in hobli_options:
                val = opt.get_attribute('value')
                text = opt.text_content()
                if val and 'Select' not in text:
                    all_hoblis.append((val, text))
            
            # Filter hoblis
            hobli_code_param = params.get('hobli_code', 'all')
            if hobli_code_param == 'all':
                hoblis_to_search = all_hoblis
            else:
                hoblis_to_search = [(h, n) for h, n in all_hoblis if h == hobli_code_param]
            
            # ═══════════════════════════════════════════════════════════════════
            # v12 FIX (F3 — silent hobli drop): the old code read the village
            # dropdown after a FIXED 2s sleep with no retry and no zero-check.
            # A slow AJAX response meant that hobli contributed 0 villages,
            # silently — live evidence: one run enumerated 236 villages, the
            # next 302 (an entire 66-village hobli lost). Now each hobli:
            #   1. polls the dropdown until populated (up to 15s),
            #   2. retries the whole selection chain up to 3 times,
            #   3. falls back to the echawadi HTTP API if still empty,
            # and the grand total is cross-checked against the HTTP API.
            # ═══════════════════════════════════════════════════════════════════
            def _read_village_options():
                out = []
                for opt in page.locator(f'#{IDS["village"]} option').all():
                    val = opt.get_attribute('value')
                    text = opt.text_content()
                    if val and 'Select' not in text:
                        out.append((val, text))
                return out

            all_villages = []
            hoblis_failed = []
            for hobli_code, hobli_name in hoblis_to_search:
                villages_hn = []
                for attempt in range(1, 4):
                    try:
                        page.goto(Config.SERVICE2_URL)
                        page.wait_for_load_state('domcontentloaded')
                        time.sleep(2)
                        page.select_option(f'#{IDS["district"]}', value=params['district_code'])
                        time.sleep(2)
                        page.select_option(f'#{IDS["taluk"]}', value=params['taluk_code'])
                        time.sleep(2)
                        page.select_option(f'#{IDS["hobli"]}', value=hobli_code)
                        # Poll until the village dropdown is actually populated.
                        waited = 0.0
                        while waited < 15.0:
                            time.sleep(0.5)
                            waited += 0.5
                            villages_hn = _read_village_options()
                            if villages_hn:
                                break
                    except Exception as sel_err:
                        logger.warning(f"Hobli {hobli_name}: selection error on attempt {attempt}: {str(sel_err)[:60]}")
                        villages_hn = []
                    if villages_hn:
                        break
                    logger.warning(f"Hobli {hobli_name} ({hobli_code}): 0 villages on attempt {attempt}/3 — retrying")

                if not villages_hn:
                    # Last resort: echawadi HTTP API enumeration for this hobli.
                    try:
                        api_villages = self.api.get_villages(
                            int(float(params['district_code'])),
                            int(float(params['taluk_code'])),
                            int(float(hobli_code)),
                        ) or []
                        for v in api_villages:
                            vcode = v.get('village_code') or v.get('code') or ''
                            vname = v.get('village_name_kn') or v.get('village_name') or v.get('name') or ''
                            if vcode != '':
                                vcode = str(int(float(vcode)))  # normalize '52.0' → '52'
                                villages_hn.append((vcode, vname))
                        if villages_hn:
                            logger.warning(
                                f"Hobli {hobli_name}: dropdown empty after 3 attempts — "
                                f"recovered {len(villages_hn)} villages via echawadi HTTP API"
                            )
                    except Exception as api_err:
                        logger.error(f"Hobli {hobli_name}: HTTP fallback failed too: {str(api_err)[:60]}")
                if not villages_hn:
                    hoblis_failed.append(f"{hobli_name} ({hobli_code})")
                    logger.error(f"❌ Hobli {hobli_name} ({hobli_code}) yielded ZERO villages — COVERAGE GAP")

                logger.info(f"Hobli {hobli_name} ({hobli_code}): {len(villages_hn)} villages")
                villages = [(v, vn, hobli_code, hobli_name) for v, vn in villages_hn]

                # Filter villages
                village_code_param = params.get('village_code', 'all')
                if village_code_param != 'all' and village_code_param:
                    villages = [(v, vn, h, hn) for v, vn, h, hn in villages if v == village_code_param]

                all_villages.extend(villages)

            # Cross-check the browser-enumerated total against the HTTP API total.
            if params.get('village_code', 'all') in ('all', '', None):
                try:
                    api_total = 0
                    for hobli_code, _hn in hoblis_to_search:
                        api_vs = self.api.get_villages(
                            int(float(params['district_code'])),
                            int(float(params['taluk_code'])),
                            int(float(hobli_code)),
                        ) or []
                        api_total += len(api_vs)
                    if api_total > len(all_villages):
                        logger.warning(
                            f"⚠️ VILLAGE COUNT MISMATCH: browser enumerated {len(all_villages)} "
                            f"but echawadi API reports {api_total} — some villages may be missing!"
                        )
                        self._log_event(
                            f"⚠️ Village count mismatch: browser={len(all_villages)} vs API={api_total}",
                            level='WARN', kind='coverage',
                        )
                except Exception as xchk_err:
                    logger.debug(f"Village-count cross-check skipped: {xchk_err}")

            if hoblis_failed:
                self._log_event(
                    f"❌ {len(hoblis_failed)} hobli(s) yielded no villages: {', '.join(hoblis_failed)}",
                    level='ERROR', kind='coverage',
                )
            logger.info(f"Found {len(all_villages)} villages to search")
            return all_villages
            
        finally:
            # Cleanup (Playwright) — v12: per-call guards. With one shared try, a failing
            # context.close() skipped browser.close()/pw.stop() and leaked the browser.
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
            try:
                pw.stop()
            except Exception:
                pass

    def _distribute_villages(self, villages: List[Tuple], num_workers: int) -> List[List[Tuple]]:
        """Distribute villages evenly across workers"""
        chunks = [[] for _ in range(num_workers)]
        for i, village in enumerate(villages):
            chunks[i % num_workers].append(village)
        return chunks
    
    def _log_event(self, message: str, level: str = 'INFO', kind: Optional[str] = None,
                   village: Optional[str] = None) -> None:
        """
        v11.0: Coordinator-level structured logging. Both updates the in-memory
        list (UI status snapshot) AND persists to DB (UI incremental fetch).
        Use this for new v11 code paths; existing 80+ direct .logs.append() calls
        continue to work but aren't DB-persisted (they're transient anyway).
        """
        with self.state_lock:
            self.state.logs.append(message)
            if len(self.state.logs) > Config.LOG_RETENTION_COUNT:
                self.state.logs = self.state.logs[-Config.LOG_RETENTION_COUNT:]
        if self.current_session_id and self.db:
            try:
                self.db.save_event_log(
                    session_id=self.current_session_id,
                    message=message,
                    level=level,
                    worker_id=None,
                    village=village,
                    kind=kind,
                )
            except Exception:
                pass
    
    def _prefetch_skeletons(
        self,
        villages: List[Tuple[str, str, str, str]],
        params: dict,
    ) -> None:
        """
        v11.0: Pre-fetch the (Survey, Surnoc, Hissa) skeleton from Service154 for
        each village in `villages`. Populates `self.skeletons` keyed by village_code.
        
        This is fire-and-forget for individual failures — a village whose skeleton
        fails will simply be searched via v10 enumeration logic (graceful fallback).
        Total time bounded by the slowest of N parallel HTTP calls (~5-30s typical).
        """
        client = get_service154_client()
        district_code = str(params.get('district_code', ''))
        taluk_code = str(params.get('taluk_code', ''))
        
        # Quick reachability check — if S154 is down, skip without retry storms
        try:
            if not client.is_reachable():
                self._log_event("⚠️ Service154 unreachable — skeleton pre-fetch skipped, using v10 enumeration",
                                level='WARN', kind='skeleton')
                return
        except Exception as e:
            self._log_event(f"⚠️ Service154 reachability check failed: {str(e)[:60]} — using v10 enumeration",
                            level='WARN', kind='skeleton')
            return
        
        self._log_event(f"🔎 Service154 reachable — fetching survey skeletons for {len(villages)} villages...",
                        level='PHASE', kind='skeleton')
        
        completed = [0]   # boxed counter for thread closure
        failures = [0]
        total_tuples = [0]
        completed_lock = threading.Lock()
        
        def _fetch_one(vt: Tuple[str, str, str, str]) -> None:
            village_code, village_name, hobli_code, _hobli_name = vt
            try:
                tuples = client.fetch_village_skeleton(
                    district_code=district_code,
                    taluk_code=taluk_code,
                    hobli_code=str(hobli_code),
                    village_code=str(village_code),
                )
            except Exception as e:
                logger.debug(f"Skeleton fetch failed for {village_name}: {e}")
                tuples = []
            
            with completed_lock:
                completed[0] += 1
                if not tuples:
                    failures[0] += 1
                else:
                    total_tuples[0] += len(tuples)
                done_n = completed[0]
            
            # Always store — empty list signals "fall back to enumeration"
            self.skeletons[str(village_code)] = tuples
            
            # Progress log every 20 villages or on completion
            if done_n % 20 == 0 or done_n == len(villages):
                self._log_event(
                    f"   📋 Skeleton: {done_n}/{len(villages)} villages "
                    f"({total_tuples[0]} tuples, {failures[0]} fallback)",
                    level='INFO', kind='skeleton',
                )
        
        # Bounded parallelism — Service154 is unmetered but be polite
        with ThreadPoolExecutor(max_workers=Config.SKELETON_PARALLEL_WORKERS) as ex:
            list(ex.map(_fetch_one, villages))
        
        # Final summary
        ok_count = len(villages) - failures[0]
        self._log_event(
            f"✅ Skeleton pre-fetch complete: "
            f"{ok_count}/{len(villages)} villages, "
            f"{total_tuples[0]} valid (Sy, Sn, Hi) tuples discovered",
            level='PHASE', kind='skeleton',
        )
        if failures[0]:
            self._log_event(
                f"   ⚠️ {failures[0]} villages will use enumeration fallback",
                level='WARN', kind='skeleton',
            )
    
    def _get_downloads_folder(self) -> str:
        """Get user's Downloads folder path"""
        import platform
        
        if platform.system() == 'Windows':
            # Windows Downloads folder
            downloads = os.path.join(os.environ.get('USERPROFILE', ''), 'Downloads')
        else:
            # macOS/Linux Downloads folder
            downloads = os.path.join(os.path.expanduser('~'), 'Downloads')
        
        # Fallback to current directory if Downloads doesn't exist
        if not os.path.exists(downloads):
            downloads = os.getcwd()
        
        return downloads
    
    def start_search(self, params: dict) -> bool:
        """Start parallel search with persistent database storage"""
        if self.state.running:
            logger.warning("Search already running")
            return False
        
        # Set running state immediately to prevent duplicate starts
        self.state.running = True
        
        # Run the actual search setup in a background thread to prevent blocking Flask
        search_thread = threading.Thread(target=self._run_search_async, args=(params,), daemon=True)
        search_thread.start()
        
        return True
    
    def _run_search_async(self, params: dict):
        """Run search asynchronously in background thread"""
        try:
            # Validate and normalize max_survey parameter
            max_survey_raw = params.get('max_survey', Config.DEFAULT_MAX_SURVEY)
            try:
                max_survey = int(max_survey_raw) if max_survey_raw else Config.DEFAULT_MAX_SURVEY
                if max_survey <= 0:
                    max_survey = Config.DEFAULT_MAX_SURVEY
            except (ValueError, TypeError):
                max_survey = Config.DEFAULT_MAX_SURVEY
            
            params['max_survey'] = max_survey
            
            # Initialize state
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            owner_name = params.get('owner_name', '')
            
            # Save CSVs to Downloads folder by default
            downloads_folder = self._get_downloads_folder()
            all_records_path = os.path.join(downloads_folder, f'bhoomi_all_records_{timestamp}.csv')
            matches_path = os.path.join(downloads_folder, f'bhoomi_matches_{timestamp}.csv')
            
            self.state = SearchState(
                running=True,
                completed=False,
                start_time=datetime.now().isoformat(),
                owner_name=owner_name,
                owner_variants=[owner_name, owner_name.upper(), owner_name.lower()],
                all_records_file=all_records_path,
                matches_file=matches_path,
                current_phase='phase1'
            )
            
            # ═══════════════════════════════════════════════════════════════════════
            # CREATE DATABASE SESSION - Records will be saved in real-time!
            # ═══════════════════════════════════════════════════════════════════════
            params['owner_variants'] = self.state.owner_variants
            self.current_session_id = self.db.create_session(params)
            with self.state_lock:
                self.state.logs.append(f"💾 Database session created: {self.current_session_id}")
                self.state.logs.append(f"📁 Data saved to: {self.db.db_path}")
                # v12.1: Bhoomi returns owner names in KANNADA. Warn loudly if the query
                # is Latin-only — it will be scraped fine but NEVER flagged as a match.
                if _is_latin_only(owner_name):
                    self.state.logs.append(
                        "⚠️ WARNING: owner name is in ENGLISH but Bhoomi records are in "
                        "KANNADA — matches will show 0. Enter the name in Kannada "
                        "(e.g. ಕುಮಾರಸ್ವಾಮಿ) for match highlighting."
                    )
            
            # ═══════════════════════════════════════════════════════════════════════
            # INITIALIZE STATE MANAGER - Enterprise state preservation
            # ═══════════════════════════════════════════════════════════════════════
            self.state_manager = StateManager(self.db, self.current_session_id)
            with self.state_lock:
                self.state.logs.append("📊 State Manager initialized - crash recovery enabled")
            
            # ═══════════════════════════════════════════════════════════════════════
            # START PORTAL HEALTH MONITORING - Proactive portal monitoring
            # ═══════════════════════════════════════════════════════════════════════
            portal_health.start_monitoring()
            with self.state_lock:
                self.state.logs.append("🏥 Portal Health Manager started - proactive monitoring active")
            
            # Start portal state response monitor (pause/resume based on portal health)
            self.portal_state_monitor_thread = threading.Thread(
                target=self._monitor_portal_state_and_respond, 
                daemon=True
            )
            self.portal_state_monitor_thread.start()
            
            # Initialize CSV writers (backup to database)
            # 🔧 v7.0: Removed khatah - portal no longer has Khata column
            fieldnames = ['district', 'taluk', 'hobli', 'village', 'survey_no',
                         'surnoc', 'hissa', 'period', 'owner_name', 'extent',
                         'owner_seq', 'timestamp', 'worker_id']
            
            self.all_records_writer = ThreadSafeCSVWriter(self.state.all_records_file, fieldnames)
            self.matches_writer = ThreadSafeCSVWriter(self.state.matches_file, fieldnames)
            
            # ═══════════════════════════════════════════════════════════════════════════
            # v10.0 FIX: Force-initialize CSV files with headers IMMEDIATELY so they
            # exist on disk even if the search finds 0 records / 0 matches. Without this,
            # the matches CSV file is never created (initialize() runs lazily on first
            # write), and the download endpoint returns 404 for matches when search
            # finds none.
            # ═══════════════════════════════════════════════════════════════════════════
            try:
                with self.all_records_writer.lock:
                    if not self.all_records_writer._initialized:
                        self.all_records_writer._initialize()
                with self.matches_writer.lock:
                    if not self.matches_writer._initialized:
                        self.matches_writer._initialize()
                with self.state_lock:
                    self.state.logs.append(f"📄 CSV files pre-created: {os.path.basename(self.state.all_records_file)}, {os.path.basename(self.state.matches_file)}")
            except Exception as init_err:
                logger.error(f"CSV pre-init failed: {init_err}")
            
            # Prepare villages
            with self.state_lock:
                self.state.logs.append("Preparing village list...")
            
            villages = self._prepare_villages(params)
            
            if not villages:
                with self.state_lock:
                    self.state.logs.append("No villages found to search")
                    self.state.running = False
                return False
            
            self.state.total_villages = len(villages)
            
            # ═══════════════════════════════════════════════════════════════════════
            # BULLETPROOF VILLAGE TRACKING - Log every single village
            # ═══════════════════════════════════════════════════════════════════════
            with self.state_lock:
                self.state.villages_all = [v[1] for v in villages]  # Store village names
                self.state.logs.append(f"📋 MASTER VILLAGE LIST: {len(villages)} villages to search")
                
                # Log first 10 and last 5 villages for verification
                village_names = [v[1] for v in villages]
                if len(village_names) > 15:
                    preview = village_names[:10] + ['...'] + village_names[-5:]
                else:
                    preview = village_names
                self.state.logs.append(f"📍 Villages: {', '.join(preview)}")
            
            # Register villages in database for resume capability
            self.db.register_villages(self.current_session_id, villages)
            self.db.update_session_status(self.current_session_id, 'running', total_villages=len(villages))
            
            # ═══════════════════════════════════════════════════════════════════════
            # v11.0: SKELETON PRE-FETCH (Service154 enumeration of valid surveys)
            # ───────────────────────────────────────────────────────────────────────
            # Before workers start, fetch the authoritative (Survey, Surnoc, Hissa)
            # list for each village from Service154's HTTP API. Workers will then
            # process these exact tuples instead of guessing 1..max_survey.
            # 
            # This is parallel (8 threads) and bounded — typical case completes in
            # <30s for 100 villages. If any individual village fetch fails, that
            # village simply falls back to v10 enumeration behavior (no failure).
            # ═══════════════════════════════════════════════════════════════════════
            # Reset skeletons for this run (also reset by /api/search/start, belt+braces)
            self.skeletons = {}
            if Config.SKELETON_FIRST_ENABLED:
                self._prefetch_skeletons(villages, params)
            else:
                self._log_event(
                    "⚙️ Skeleton-first mode disabled — using v10 enumeration",
                    level='WARN', kind='skeleton',
                )
            
            # Determine number of workers
            num_workers = min(Config.MAX_WORKERS, len(villages))
            self.state.total_workers = num_workers
            
            # Distribute villages
            village_chunks = self._distribute_villages(villages, num_workers)
            
            # Initialize worker statuses
            for i in range(num_workers):
                self.state.workers[i] = WorkerStatus(
                    worker_id=i,
                    villages_total=len(village_chunks[i])
                )
            
            with self.state_lock:
                self.state.logs.append(f"🚀 Starting {num_workers} workers for {len(villages)} villages")
            
            # Start workers with staggered startup to avoid Chrome conflicts
            self.executor = ThreadPoolExecutor(max_workers=num_workers)
            
            for i in range(num_workers):
                worker = SearchWorker(
                    worker_id=i,
                    search_params=params,
                    villages=village_chunks[i],
                    state=self.state,
                    all_records_writer=self.all_records_writer,
                    matches_writer=self.matches_writer,
                    state_lock=self.state_lock,
                    db=self.db,  # Persistent database
                    session_id=self.current_session_id,  # Current session ID
                    skeletons=getattr(self, 'skeletons', None),  # v11: pre-fetched (sy,sn,hi)
                )
                self.workers.append(worker)
                self.executor.submit(worker.run)
                
                # Staggered startup on Windows to prevent Chrome crashes
                if i < num_workers - 1:  # Don't wait after last worker
                    time.sleep(Config.WORKER_STARTUP_DELAY)
                    with self.state_lock:
                        self.state.logs.append(f"Worker {i} started, launching next...")
            
            # Start completion monitor
            threading.Thread(target=self._monitor_completion, daemon=True).start()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start search: {traceback.format_exc()}")
            with self.state_lock:
                self.state.running = False
                self.state.logs.append(f"❌ Search failed to start: {str(e)[:100]}")
    
    def _monitor_portal_state_and_respond(self):
        """
        Monitor portal health and intelligently pause/resume workers.
        This is the "brain" that responds to portal state changes.
        """
        logger.info("🧠 Portal state response monitor started")
        
        last_state = 'UNKNOWN'
        paused_workers = set()  # Track which workers are paused
        
        while not self._stop_portal_monitor.wait(5):  # Check every 5 seconds
            try:
                # Check if search is still running
                with self.state_lock:
                    if not self.state.running:
                        break
                
                # Get current portal state
                portal_state = portal_health.get_state()
                
                # React to state changes
                if portal_state != last_state:
                    with self.state_lock:
                        self.state.logs.append(f"🏥 Portal state: {last_state} → {portal_state}")
                    
                    # ═══════════════════════════════════════════════════════════════════════
                    # RESPONSE STRATEGY based on portal state
                    # ═══════════════════════════════════════════════════════════════════════
                    
                    if portal_state == 'DOWN':
                        # Portal completely down - pause ALL workers
                        with self.state_lock:
                            self.state.logs.append("⏸️ PORTAL DOWN - Pausing all workers until recovery...")
                        
                        if self.state_manager:
                            self.state_manager.pause_search("Portal is down")
                        
                        # Workers will naturally pause as they check portal_health.should_wait()
                        
                    elif portal_state == 'RATE_LIMITED':
                        # Being rate limited - initiate gradual scale-down
                        with self.state_lock:
                            self.state.logs.append("⚠️ RATE LIMITED - Throttling workers...")
                        
                        # TODO: Implement worker throttling (reduce active workers)
                        # For now, cooldown mechanism handles this
                        
                    elif portal_state == 'NETWORK_CONGESTION':
                        # Network issues - increase timeouts, don't stop
                        with self.state_lock:
                            self.state.logs.append("⚠️ NETWORK CONGESTION - Increasing timeouts...")
                        
                    elif portal_state == 'DEGRADED':
                        # Portal slow but working - continue with caution
                        with self.state_lock:
                            self.state.logs.append("⚠️ Portal DEGRADED - Continuing with longer waits...")
                        
                    elif portal_state == 'HEALTHY' and last_state in ('DOWN', 'RATE_LIMITED'):
                        # Recovery detected!
                        with self.state_lock:
                            self.state.logs.append(f"✅ Portal RECOVERED from {last_state} - Resuming operations...")
                        
                        if self.state_manager and self.state_manager.is_paused:
                            if Config.AUTO_RESUME_ON_RECOVERY:
                                self.state_manager.resume_search()
                                with self.state_lock:
                                    self.state.logs.append("▶️ Search AUTO-RESUMED")
                    
                    last_state = portal_state
                
                # Save periodic state snapshots (in background to avoid blocking)
                if self.state_manager:
                    now = time.time()
                    if now - self.state_manager.last_snapshot_time >= self.state_manager.snapshot_interval:
                        def save_snapshot_async():
                            try:
                                state_dict = self.get_state()
                                # Worker states already included in state_dict, no need to serialize again
                                self.state_manager.save_snapshot(state_dict, {})
                            except Exception as snap_err:
                                logger.debug(f"Snapshot save error: {snap_err}")
                        
                        # Run in background thread to avoid blocking portal monitor
                        threading.Thread(target=save_snapshot_async, daemon=True).start()
                
            except Exception as e:
                logger.error(f"Portal state monitor error: {e}")
        
        logger.info("🧠 Portal state response monitor stopped")
    
    def _monitor_completion(self):
        """Monitor workers and mark search as complete when all done"""
        # Wait a bit for workers to initialize before starting to monitor
        time.sleep(5)
        
        while True:
            # Check running state with lock to avoid race conditions
            with self.state_lock:
                if not self.state.running:
                    break
            
            time.sleep(2)
            
            with self.state_lock:
                # Don't check completion if no workers exist yet (race condition prevention)
                if not self.state.workers or len(self.state.workers) == 0:
                    continue

                # ═══════════════════════════════════════════════════════════════════════
                # v12: STALL WATCHDOG — reap wedged workers so completion can proceed.
                # A worker stuck in a Playwright greenlet spin never returns and never
                # raises, so it stays 'running' forever. If its heartbeat is older than
                # STALL_TIMEOUT_SECONDS we declare it failed, hand its not-yet-finished
                # villages to the coverage pass, and kill its browser. This is the fix
                # for "the app always gets stuck at the end of a search".
                # ═══════════════════════════════════════════════════════════════════════
                now_ts = time.time()

                # v12.1: RESURRECTION — a reaped worker whose heartbeat resumed was a
                # false positive (slow-and-silent, not wedged; seen live with W18's
                # 17-minute hissa-recovery chain). Restore it and un-queue its villages
                # so the coverage pass doesn't duplicate its work.
                for wid in list(self.state.stalled_workers):
                    ws = self.state.workers.get(wid)
                    if ws is None or ws.status != 'failed':
                        continue
                    if now_ts - (ws.last_heartbeat or 0) < 120:
                        ws.status = 'running'
                        self.state.stalled_workers.remove(wid)
                        for v in self._reaped_villages.pop(wid, []):
                            try:
                                self.state.unfinished_villages.remove(v)
                            except ValueError:
                                pass
                        self.state.logs.append(
                            f"💓 WATCHDOG: W{wid} heartbeat resumed — resurrected "
                            f"(was slow, not wedged); villages un-queued from coverage"
                        )
                        logger.warning(f"Watchdog resurrected worker {wid} (false-positive reap)")

                for wid, ws in self.state.workers.items():
                    if ws.status != 'running':
                        continue
                    idle = now_ts - (ws.last_heartbeat or 0)
                    if idle < Config.STALL_TIMEOUT_SECONDS:
                        continue
                    # This worker is wedged. Mark it failed and salvage its remaining work.
                    ws.status = 'failed'
                    if wid not in self.state.stalled_workers:
                        self.state.stalled_workers.append(wid)
                    remaining = []
                    try:
                        worker_obj = self.workers[wid] if wid < len(self.workers) else None
                        if worker_obj is not None:
                            done_idx = ws.villages_completed or 0
                            remaining = list(worker_obj.villages[done_idx:])
                    except Exception as wd_err:
                        logger.error(f"Watchdog: could not read W{wid} villages: {wd_err}")
                    if remaining:
                        self.state.unfinished_villages.extend(remaining)
                        self._reaped_villages[wid] = list(remaining)  # v12.1: for resurrection
                    self.state.logs.append(
                        f"⏱️ WATCHDOG: W{wid} wedged {int(idle)}s "
                        f"(last at '{ws.current_village}' Sy:{ws.current_survey}) → marked "
                        f"failed; {len(remaining)} unfinished village(s) queued for coverage"
                    )
                    logger.warning(
                        f"Watchdog reaped worker {wid}: idle={int(idle)}s, "
                        f"unfinished={len(remaining)}"
                    )
                    # Best-effort: free the wedged browser's memory (the greenlet spin
                    # itself can't be killed, but the child Chromium can).
                    try:
                        BrowserCleanup.kill_chrome_for_worker(str(wid))
                    except Exception:
                        pass

                # Check if all workers have completed or failed
                all_done = all(
                    ws.status in ('completed', 'failed')
                    for ws in self.state.workers.values()
                )
                
                # Double check: ensure at least some work was attempted
                total_villages_assigned = sum(ws.villages_total for ws in self.state.workers.values())
                if total_villages_assigned == 0:
                    continue  # Workers haven't been assigned villages yet
                
                if all_done:
                    # Phase 1 workers are done — set state directly (already inside lock)
                    self.state.current_phase = 'phase1_done'
                    self.state.phase1_completed_at = datetime.now().isoformat()
                    
                    # ═══════════════════════════════════════════════════════════════════════════
                    # PHASE 1 COMPLETION SUMMARY
                    # ═══════════════════════════════════════════════════════════════════════════
                    total_villages = len(self.state.villages_all)
                    processed = len(self.state.villages_processed)
                    retried = len(self.state.villages_retried)
                    failed = len(self.state.villages_failed)
                    
                    village_stats = self.state.village_stats or {}
                    high_conf = sum(1 for v in village_stats.values() if v.get('confidence_score', 0) >= 80)
                    med_conf = sum(1 for v in village_stats.values() if 50 <= v.get('confidence_score', 0) < 80)
                    low_conf = sum(1 for v in village_stats.values() if v.get('confidence_score', 0) < 50)
                    
                    if village_stats:
                        avg_confidence = sum(v.get('confidence_score', 0) for v in village_stats.values()) / len(village_stats)
                    else:
                        avg_confidence = 0
                    
                    self.state.logs.append("")
                    self.state.logs.append("╔" + "═" * 62 + "╗")
                    self.state.logs.append("║" + "  PHASE 1 COMPLETE".center(62) + "║")
                    self.state.logs.append("╠" + "═" * 62 + "╣")
                    
                    self.state.logs.append("║  VILLAGE PROCESSING:".ljust(63) + "║")
                    self.state.logs.append(f"║    Total villages: {total_villages}".ljust(63) + "║")
                    self.state.logs.append(f"║    Successfully processed: {processed}".ljust(63) + "║")
                    self.state.logs.append(f"║    Retried (session recovery): {retried}".ljust(63) + "║")
                    self.state.logs.append(f"║    Failed: {failed}".ljust(63) + "║")
                    self.state.logs.append("║".ljust(63) + "║")
                    self.state.logs.append("║  DATA EXTRACTED:".ljust(63) + "║")
                    self.state.logs.append(f"║    Total records: {self.state.total_records}".ljust(63) + "║")
                    self.state.logs.append(f"║    Owner matches: {self.state.total_matches}".ljust(63) + "║")
                    self.state.logs.append(f"║    Periods processed: {self.state.total_periods_processed}".ljust(63) + "║")
                    self.state.logs.append("║".ljust(63) + "║")
                    self.state.logs.append("║  SMART STOP:".ljust(63) + "║")
                    self.state.logs.append(f"║    Villages with smart stop: {self.state.smart_stops}".ljust(63) + "║")
                    self.state.logs.append(f"║    Surveys saved: {self.state.surveys_saved}".ljust(63) + "║")
                    self.state.logs.append("║".ljust(63) + "║")
                    self.state.logs.append("║  ACCURACY:".ljust(63) + "║")
                    self.state.logs.append(f"║    High confidence: {high_conf}".ljust(63) + "║")
                    self.state.logs.append(f"║    Medium confidence: {med_conf}".ljust(63) + "║")
                    self.state.logs.append(f"║    Low confidence: {low_conf}".ljust(63) + "║")
                    self.state.logs.append(f"║    Average confidence: {avg_confidence:.1f}%".ljust(63) + "║")
                    
                    skipped_count = len(self.state.skipped_surveys) if self.state.skipped_surveys else 0
                    self.state.logs.append(f"║    Skipped surveys: {skipped_count}".ljust(63) + "║")
                    self.state.logs.append("╚" + "═" * 62 + "╝")
                    
                    # Post-search validation warnings
                    validation_warnings = []
                    for vcode, vstats in village_stats.items():
                        if vstats.get('confidence_score', 100) < 50:
                            validation_warnings.append(f"Low confidence: {vstats.get('village_name', vcode)}")
                        if vstats.get('skipped_count', 0) > 20:
                            validation_warnings.append(f"High skip rate: {vstats.get('village_name', vcode)}")
                    
                    if validation_warnings:
                        self.state.logs.append("")
                        self.state.logs.append("POST-SEARCH VALIDATION WARNINGS:")
                        for warn in validation_warnings[:5]:
                            self.state.logs.append(f"   - {warn}")
                        if len(validation_warnings) > 5:
                            self.state.logs.append(f"   ... and {len(validation_warnings) - 5} more")
                    
                    break

        # ═══════════════════════════════════════════════════════════════════════════
        # POST-COMPLETION: Phase 2 + finalization — OUTSIDE lock to prevent deadlocks
        # ═══════════════════════════════════════════════════════════════════════════

        with self.state_lock:
            is_phase1_done = self.state.current_phase == 'phase1_done'

        if not is_phase1_done:
            return

        # ═══════════════════════════════════════════════════════════════════════
        # v10.0: FLUSH + SNAPSHOT CSV FILES after Phase 1 - guarantees download
        # works immediately regardless of Phase 2 state. We:
        #   1. Force flush in-memory buffer to disk
        #   2. Export current DB rows to a separate "_phase1.csv" snapshot
        #      (independent of the still-open writer file)
        #   3. Log clear download paths
        # ═══════════════════════════════════════════════════════════════════════
        try:
            if self.all_records_writer:
                self.all_records_writer.flush()
            if self.matches_writer:
                self.matches_writer.flush()
            
            # Snapshot DB → standalone Phase-1 CSV files (immune to writer state)
            phase1_records_path = ''
            phase1_matches_path = ''
            try:
                if self.current_session_id:
                    base = self.state.all_records_file
                    phase1_records_path = base.replace('.csv', '_phase1.csv') if base.endswith('.csv') else base + '_phase1.csv'
                    phase1_matches_path = self.state.matches_file.replace('.csv', '_phase1.csv') if self.state.matches_file.endswith('.csv') else self.state.matches_file + '_phase1.csv'
                    
                    self.db.export_to_csv(self.current_session_id, phase1_records_path, matches_only=False)
                    self.db.export_to_csv(self.current_session_id, phase1_matches_path, matches_only=True)
            except Exception as snap_err:
                logger.error(f"Phase 1 CSV snapshot failed: {snap_err}")
            
            with self.state_lock:
                self.state.phase1_records_file = phase1_records_path
                self.state.phase1_matches_file = phase1_matches_path
                self.state.logs.append(f"📥 PHASE 1 CSV READY (downloadable now):")
                self.state.logs.append(f"   📊 All records: {os.path.basename(self.state.all_records_file)}")
                self.state.logs.append(f"   🎯 Matches:     {os.path.basename(self.state.matches_file)}")
                if phase1_records_path:
                    self.state.logs.append(f"   📸 Snapshot:    {os.path.basename(phase1_records_path)}")
        except Exception as flush_err:
            logger.error(f"CSV flush after Phase 1 failed: {flush_err}")

        # ═══════════════════════════════════════════════════════════════════════
        # v12: COVERAGE PASS — re-enumerate villages a wedged/failed worker abandoned.
        # Runs before Phase 2 so any surveys these villages skip are themselves retried.
        # Reuses the exact _search_village enumeration path (skeleton-verified smart-stop
        # included), so coverage villages get identical treatment to Phase 1 villages.
        # ═══════════════════════════════════════════════════════════════════════
        with self.state_lock:
            coverage_targets = list(self.state.unfinished_villages)
            still_running = self.state.running is not False
        if coverage_targets and still_running:
            self._run_phase_with_watchdog(
                98, lambda: self._run_coverage_pass(coverage_targets), 'Coverage pass'
            )

        # ═══════════════════════════════════════════════════════════════════════
        # PHASE 2: AUTO-RETRY SKIPPED SURVEYS (outside lock — _run_phase2 manages its own)
        # v12: also runs when the DB holds skipped_items even if the in-memory list is
        # empty (e.g. after a restart-resume), because _run_phase2 now sources from DB.
        # ═══════════════════════════════════════════════════════════════════════
        should_run_phase2 = False
        with self.state_lock:
            has_db_skips = False
            if Config.PHASE2_FROM_DB and self.current_session_id and self.db:
                try:
                    has_db_skips = self.db.get_skipped_count(self.current_session_id) > 0
                except Exception:
                    has_db_skips = False
            if (Config.PHASE2_ENABLED and (self.state.skipped_surveys or has_db_skips)
                    and self.state.running is not False):
                self.state.running = True
                should_run_phase2 = True

        if should_run_phase2:
            self._run_phase_with_watchdog(99, self._run_phase2, 'Phase 2')

        # ═══════════════════════════════════════════════════════════════════════
        # FINAL COMPLETION
        # ═══════════════════════════════════════════════════════════════════════
        with self.state_lock:
            self.state.running = False
            self.state.completed = True
            self.state.current_phase = 'completed'

        if self.current_session_id:
            try:
                with self.state_lock:
                    final_records = self.state.total_records
                    final_matches = self.state.total_matches
                    # v12: recompute after the coverage pass (which appends to
                    # villages_processed) so the DB count isn't the stale pre-coverage value.
                    final_villages = len(self.state.villages_processed)
                self.db.update_session_status(
                    self.current_session_id,
                    'completed',
                    villages_completed=final_villages,
                    total_records=final_records,
                    total_matches=final_matches
                )
                with self.state_lock:
                    self.state.logs.append(f"Search saved to database: {self.current_session_id}")
            except Exception as e:
                logger.error(f"Failed to update DB on completion: {e}")

        # Auto-export skipped surveys CSV — v12: source the FULL, still-unresolved list
        # from the DB (skipped_items whose status isn't 'recovered'), not the truncated
        # in-memory list, so the file reflects every genuine gap after Phase 2.
        remaining_skipped = []
        try:
            if self.current_session_id and self.db:
                remaining_skipped = self.db.get_skipped_items(self.current_session_id)
        except Exception as e:
            logger.error(f"Failed to read skipped_items for final export: {e}")
        if not remaining_skipped and self.state.skipped_surveys:
            remaining_skipped = list(self.state.skipped_surveys)

        if remaining_skipped:
            try:
                downloads = os.path.join(os.path.expanduser('~'), 'Downloads')
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"skipped_surveys_{timestamp}.csv"
                filepath = os.path.join(downloads, filename)
                fieldnames = ['village', 'village_name', 'village_code', 'survey_no',
                              'surnoc', 'hissa', 'period', 'reason', 'error_message',
                              'status', 'created_at', 'timestamp']
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(remaining_skipped)
                with self.state_lock:
                    self.state.logs.append(f"Skipped surveys exported ({len(remaining_skipped)} rows): {filename}")
            except Exception as e:
                logger.error(f"Failed to auto-export skipped surveys: {e}")
        else:
            with self.state_lock:
                self.state.logs.append("No skipped surveys - 100% coverage!")

        # ═══════════════════════════════════════════════════════════════════════
        # v12: COVERAGE AUDIT — make gaps visible instead of silent. Report, per the
        # run, how many villages were processed vs. abandoned and how many skipped
        # tuples remain. This is the completeness scorecard for the objective
        # "every survey, every hissa, every name".
        # ═══════════════════════════════════════════════════════════════════════
        try:
            with self.state_lock:
                total_v = len(self.state.villages_all)
                processed_v = len(self.state.villages_processed)
                stalled = list(self.state.stalled_workers)
                cov_done = self.state.coverage_villages_done
                unfinished_left = len(self.state.unfinished_villages) - cov_done
            skip_left = len(remaining_skipped)
            with self.state_lock:
                self.state.logs.append("")
                self.state.logs.append("╔" + "═" * 62 + "╗")
                self.state.logs.append("║" + "  COVERAGE AUDIT".center(62) + "║")
                self.state.logs.append("╠" + "═" * 62 + "╣")
                self.state.logs.append(f"║  Villages processed: {processed_v}/{total_v}".ljust(63) + "║")
                self.state.logs.append(f"║  Watchdog-reaped workers: {len(stalled)} {stalled}".ljust(63) + "║")
                self.state.logs.append(f"║  Coverage-pass villages done: {cov_done}".ljust(63) + "║")
                self.state.logs.append(f"║  Unfinished villages remaining: {max(0, unfinished_left)}".ljust(63) + "║")
                self.state.logs.append(f"║  Skipped tuples still unresolved: {skip_left}".ljust(63) + "║")
                verdict = "COMPLETE ✅" if (processed_v >= total_v and skip_left == 0 and unfinished_left <= 0) else "GAPS REMAIN ⚠️"
                self.state.logs.append(f"║  Verdict: {verdict}".ljust(63) + "║")
                self.state.logs.append("╚" + "═" * 62 + "╝")
        except Exception as audit_err:
            logger.error(f"Coverage audit failed: {audit_err}")

        logger.info("Search completed (Phase 1 + Coverage + Phase 2)")
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # PHASE 2: COORDINATOR ORCHESTRATION
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def _run_phase_with_watchdog(self, worker_id: int, fn, label: str):
        """
        v12: Run a post-Phase-1 sub-phase (coverage or Phase 2) under the same stall
        watchdog that protects Phase 1. These phases use a single worker, so if IT wedges
        in a Playwright greenlet spin there is no pool of peers to carry on — this guard
        watches that worker's heartbeat and, if it goes stale beyond STALL_TIMEOUT_SECONDS,
        abandons the wedged thread (left as a harmless zombie) and lets the search finalize.
        Without this, one wedge in coverage/Phase 2 would reintroduce the endgame hang.
        """
        done = threading.Event()

        def _wrap():
            try:
                fn()
            except Exception as e:
                logger.error(f"{label} raised: {e}")
            finally:
                done.set()

        t = threading.Thread(target=_wrap, daemon=True)
        t.start()
        while not done.wait(timeout=Config.WATCHDOG_CHECK_INTERVAL):
            with self.state_lock:
                ws = self.state.workers.get(worker_id)
                hb = ws.last_heartbeat if ws else None
            idle = time.time() - (hb if hb else time.time())
            if idle > Config.STALL_TIMEOUT_SECONDS:
                with self.state_lock:
                    self.state.logs.append(
                        f"⏱️ WATCHDOG: {label} wedged {int(idle)}s — abandoning and finalizing"
                    )
                logger.warning(f"{label} wedged {int(idle)}s; abandoning to finalize search")
                try:
                    BrowserCleanup.kill_chrome_for_worker(str(worker_id))
                except Exception:
                    pass
                return
        logger.info(f"{label} finished cleanly")

    def _run_coverage_pass(self, coverage_villages: List[Tuple]):
        """
        v12: Re-enumerate villages that a stalled/failed worker never finished.

        Uses one dedicated worker (id 98) running the SAME _search_village path as
        Phase 1 — full enumeration with skeleton-verified smart-stop — so these villages
        get identical coverage. Records/skips flow to the same DB session and CSV writers;
        any surveys skipped here are picked up by Phase 2 afterwards.
        """
        if not coverage_villages:
            return
        if not self.workers:
            with self.state_lock:
                self.state.logs.append("Coverage pass: no base worker params available — skipping")
            return

        # De-dup while preserving order (a village could be queued twice if two workers
        # somehow shared it, or across repeated watchdog passes).
        seen = set()
        targets = []
        for v in coverage_villages:
            k = (v[2], v[0])  # (hobli_code, village_code)
            if k in seen:
                continue
            seen.add(k)
            targets.append(v)

        with self.state_lock:
            self.state.current_phase = 'coverage'
            self.state.logs.append("")
            self.state.logs.append("=" * 64)
            self.state.logs.append("  COVERAGE PASS: re-enumerating unfinished villages")
            self.state.logs.append(f"  {len(targets)} village(s) abandoned by wedged worker(s)")
            self.state.logs.append("=" * 64)
            # Register a status card for the coverage worker so the UI + heartbeat work.
            self.state.workers[98] = WorkerStatus(worker_id=98, villages_total=len(targets))

        cov = SearchWorker(
            worker_id=98,
            search_params=self.workers[0].params,
            villages=targets,
            state=self.state,
            all_records_writer=self.all_records_writer,
            matches_writer=self.matches_writer,
            state_lock=self.state_lock,
            db=self.db,
            session_id=self.current_session_id,
            skeletons=getattr(self, 'skeletons', None),
        )

        try:
            cov._init_browser()
        except Exception as e:
            with self.state_lock:
                self.state.logs.append(f"Coverage pass: browser init failed — {str(e)[:60]}")
            return

        done = 0
        try:
            for (vc, vn, hc, hn) in targets:
                with self.state_lock:
                    if self.state.running is False:
                        break
                try:
                    cov._search_village(vc, vn, hc, hn)
                    with self.state_lock:
                        if vn not in self.state.villages_processed:
                            self.state.villages_processed.append(vn)
                        self.state.coverage_villages_done += 1
                    done += 1
                    cov._update_status(villages_completed=done)
                    cov._update_global_stats()
                except Exception as ve:
                    cov._add_log(f"Coverage: {vn} error — {str(ve)[:60]}")
                    # Best-effort browser recovery between villages.
                    try:
                        cov._close_browser()
                        time.sleep(2)
                        cov._init_browser()
                    except Exception:
                        pass
            cov._update_status(status='completed')
        finally:
            cov._close_browser()
            cov._update_global_stats()

        with self.state_lock:
            self.state.logs.append(f"Coverage pass complete: {done}/{len(targets)} village(s) enumerated")

    def _run_phase2(self):
        """
        Orchestrate Phase 2: create a retry worker and process all skipped surveys.
        Called inline from _monitor_completion after Phase 1 completes.

        v12: the retry set is sourced from the DB `skipped_items` table (deduplicated)
        rather than the in-memory list. This survives restarts and removes the old
        20-item truncation, so Phase 2 actually retries every skipped tuple.
        """
        # Build village name/code -> hobli mapping from Phase 1 workers first, so DB rows
        # (which store village_name but not village_code) can be resolved for navigation.
        village_hobli_map = {}
        name_to_code = {}
        for worker in self.workers:
            for vc, vn, hc, hn in worker.villages:
                village_hobli_map[vc] = {
                    'hobli_code': hc,
                    'hobli_name': hn,
                    'village_name': vn
                }
                name_to_code.setdefault(vn, vc)

        skipped = []
        if Config.PHASE2_FROM_DB and self.current_session_id and self.db:
            try:
                db_rows = self.db.get_skipped_items(self.current_session_id)
                seen = set()
                for r in db_rows:
                    vn = r.get('village_name', '')
                    vc = name_to_code.get(vn, '')
                    key = (vc or vn, r.get('survey_no'), r.get('surnoc', ''), r.get('hissa', ''))
                    if key in seen:
                        continue
                    seen.add(key)
                    skipped.append({
                        'village': vn,
                        'village_code': vc,
                        'survey_no': r.get('survey_no', 0),
                        'surnoc': r.get('surnoc', ''),
                        'hissa': r.get('hissa', ''),
                        'period': r.get('period', ''),
                        'reason': r.get('error_message', ''),
                    })
            except Exception as db_err:
                logger.error(f"Phase 2: DB skipped load failed, falling back to memory: {db_err}")
                skipped = list(self.state.skipped_surveys)
        else:
            skipped = list(self.state.skipped_surveys)

        # Drop rows we cannot navigate to (no resolvable village_code) but count them.
        unresolved = [s for s in skipped if not s.get('village_code')]
        skipped = [s for s in skipped if s.get('village_code')]
        if not skipped:
            if unresolved:
                with self.state_lock:
                    self.state.logs.append(
                        f"Phase 2: {len(unresolved)} skipped item(s) had no matching village "
                        f"in this run's list — nothing to retry"
                    )
            return

        with self.state_lock:
            self.state.current_phase = 'phase2'
            self.state.phase2_started_at = datetime.now().isoformat()
            self.state.phase2_total = len(skipped)
            self.state.logs.append("")
            self.state.logs.append("=" * 64)
            self.state.logs.append("  PHASE 2: AUTO-RETRY OF SKIPPED SURVEYS")
            self.state.logs.append(f"  {len(skipped)} deduplicated items to retry"
                                   + (f" ({len(unresolved)} unresolved, skipped)" if unresolved else ""))
            self.state.logs.append("=" * 64)
        
        # Wait for portal to stabilize after Phase 1
        with self.state_lock:
            self.state.logs.append(f"Waiting {Config.PHASE2_COOLDOWN_BEFORE}s for portal to stabilize...")
        time.sleep(Config.PHASE2_COOLDOWN_BEFORE)
        
        # Check portal health before starting
        if portal_health.is_down():
            with self.state_lock:
                self.state.logs.append("Portal is DOWN — waiting for recovery before Phase 2...")
            
            max_health_wait = 300  # 5 minutes
            waited = 0
            while waited < max_health_wait and portal_health.is_down():
                time.sleep(10)
                waited += 10
            
            if portal_health.is_down():
                with self.state_lock:
                    self.state.logs.append("Portal did not recover — skipping Phase 2")
                    self.state.current_phase = 'completed'
                return
        
        # Get search params from first worker
        search_params = self.workers[0].params if self.workers else {}
        
        # Create a dedicated Phase 2 worker
        phase2_worker = SearchWorker(
            worker_id=99,  # Special ID for Phase 2
            search_params=search_params,
            villages=[],   # Phase 2 doesn't use the village list
            state=self.state,
            all_records_writer=self.all_records_writer,
            matches_writer=self.matches_writer,
            state_lock=self.state_lock,
            db=self.db,
            session_id=self.current_session_id,
            skeletons=getattr(self, 'skeletons', None),  # v11
        )
        
        with self.state_lock:
            # v12: register a status card so the sub-phase watchdog can track worker 99's
            # heartbeat (run_phase2's _add_log/_update_status stamp it).
            self.state.workers[99] = WorkerStatus(worker_id=99, villages_total=1)
            self.state.logs.append(f"Phase 2 worker initialized — starting retries...")

        # Run Phase 2 (blocking — runs in _monitor_completion thread)
        phase2_worker.run_phase2(skipped, village_hobli_map)
        
        # Phase 2 complete — log summary
        with self.state_lock:
            self.state.phase2_completed_at = datetime.now().isoformat()
            
            self.state.logs.append("")
            self.state.logs.append("+" + "-" * 62 + "+")
            self.state.logs.append("|" + "  PHASE 2 COMPLETE".center(62) + "|")
            self.state.logs.append("+" + "-" * 62 + "+")
            self.state.logs.append(f"|  Total skipped items: {self.state.phase2_total}".ljust(63) + "|")
            self.state.logs.append(f"|  Attempted: {self.state.phase2_attempted}".ljust(63) + "|")
            self.state.logs.append(f"|  RECOVERED: {self.state.phase2_recovered}".ljust(63) + "|")
            self.state.logs.append(f"|  Still failed: {self.state.phase2_failed}".ljust(63) + "|")
            self.state.logs.append(f"|  New records added: {self.state.phase2_records_added}".ljust(63) + "|")
            
            recovery_pct = (self.state.phase2_recovered / max(1, self.state.phase2_attempted)) * 100
            self.state.logs.append(f"|  Recovery rate: {recovery_pct:.1f}%".ljust(63) + "|")
            self.state.logs.append("+" + "-" * 62 + "+")
    
    def stop_search(self):
        """
        v12: GRACEFUL stop.

        v11's stop set running=False, fired the CSV exports, and returned — but it left
        wedged workers alive, never reset the coordinator, and skipped Phase 2. This
        version:
          1. Signals stop and gives workers a bounded window to finish their current
             survey and exit cleanly (they poll state.running at every loop top).
          2. Force-fails and kills any worker still alive after the window (a greenlet
             spin can't be interrupted, but its Chromium child is killed to free memory).
          3. Exports all CSVs + the FULL skipped list from the DB.
          4. Resets the coordinator (workers/executor/skeletons/phase) so a new search
             can start immediately without a server restart.
        """
        logger.info("Stop search requested (graceful)")

        # v12: single-flight guard — a second concurrent stop returns immediately
        # instead of running a duplicate drain + export pass.
        if not self._stop_lock.acquire(blocking=False):
            logger.info("Stop already in progress — ignoring duplicate request")
            return
        try:
            self._stop_search_inner()
        finally:
            self._stop_lock.release()

    def _stop_search_inner(self):
        """The actual stop sequence — only ever runs single-flight via stop_search()."""
        # Set running to False immediately — every worker loop checks this at its top.
        self.state.running = False

        # Stop portal monitoring
        self._stop_portal_monitor.set()

        with self.state_lock:
            self.state.logs.append("⏹️ Stop requested — waiting for workers to finish current survey...")

        # Force shutdown executor (don't block; workers exit on their own via the flag)
        if self.executor:
            try:
                self.executor.shutdown(wait=False, cancel_futures=True)
            except Exception as e:
                logger.warning(f"Executor shutdown warning: {e}")

        # ═══════════════════════════════════════════════════════════════════════
        # v12: GRACEFUL DRAIN — give workers up to GRACEFUL_STOP_TIMEOUT to notice
        # the stop flag and exit their current survey cleanly, then reap stragglers.
        # ═══════════════════════════════════════════════════════════════════════
        deadline = time.time() + Config.GRACEFUL_STOP_TIMEOUT
        while time.time() < deadline:
            with self.state_lock:
                still_running = [
                    wid for wid, ws in self.state.workers.items()
                    if ws.status == 'running'
                ]
            if not still_running:
                break
            time.sleep(1.0)

        with self.state_lock:
            stragglers = [
                wid for wid, ws in self.state.workers.items()
                if ws.status == 'running'
            ]
            for wid in stragglers:
                self.state.workers[wid].status = 'failed'
            if stragglers:
                self.state.logs.append(
                    f"⚠️ {len(stragglers)} worker(s) did not stop in "
                    f"{Config.GRACEFUL_STOP_TIMEOUT}s — force-failing and killing browsers"
                )
        # Kill any browser still held by a straggler (best-effort, outside the lock).
        for wid in stragglers:
            try:
                BrowserCleanup.kill_chrome_for_worker(str(wid))
            except Exception:
                pass

        # Update database session status (in background to not block)
        def update_db_async():
            try:
                if self.current_session_id:
                    self.db.update_session_status(
                        self.current_session_id, 
                        'stopped',
                        villages_completed=len(self.state.villages_processed),
                        total_records=self.state.total_records,
                        total_matches=self.state.total_matches
                    )
            except Exception as e:
                logger.error(f"Failed to update DB on stop: {e}")
        
        threading.Thread(target=update_db_async, daemon=True).start()
        
        # ═══════════════════════════════════════════════════════════════════════
        # v11.1: AUTO-EXPORT ALL CSVs ON STOP (not just skipped surveys)
        # ───────────────────────────────────────────────────────────────────────
        # Earlier behavior only exported skipped_surveys.csv; users had no
        # all_records.csv / matches.csv after a manual stop. Now we:
        #  1. Flush in-memory CSV writers (writes pending buffer to disk)
        #  2. Export from DB to a dated snapshot in Downloads (independent of
        #     writer state — works even if writer was never opened)
        #  3. Export skipped surveys (existing behavior, retained)
        # All operations are best-effort — failures don't break the stop flow.
        # ═══════════════════════════════════════════════════════════════════════
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        downloads = os.path.join(os.path.expanduser('~'), 'Downloads')
        os.makedirs(downloads, exist_ok=True)
        sid = self.current_session_id
        
        # 1. Flush live CSV writers to disk
        try:
            if self.all_records_writer:
                self.all_records_writer.flush()
            if self.matches_writer:
                self.matches_writer.flush()
        except Exception as e:
            logger.warning(f"CSV writer flush on stop: {e}")
        
        # 2. Export DB-snapshot CSVs (always succeeds if session has rows)
        if sid:
            try:
                all_path = os.path.join(downloads, f'bhoomi_all_records_{timestamp}_stop.csv')
                exported_all = self.db.export_to_csv(sid, all_path, matches_only=False)
                if exported_all:
                    with self.state_lock:
                        self.state.logs.append(f"📥 All records exported on stop: {os.path.basename(all_path)}")
                    logger.info(f"Auto-exported all records on stop: {all_path}")
            except Exception as e:
                logger.error(f"Failed to auto-export all records on stop: {e}")
            
            try:
                matches_path = os.path.join(downloads, f'bhoomi_matches_{timestamp}_stop.csv')
                exported_matches = self.db.export_to_csv(sid, matches_path, matches_only=True)
                if exported_matches:
                    with self.state_lock:
                        self.state.logs.append(f"📥 Matches exported on stop: {os.path.basename(matches_path)}")
                    logger.info(f"Auto-exported matches on stop: {matches_path}")
            except Exception as e:
                logger.error(f"Failed to auto-export matches on stop: {e}")
        
        # 3. Skipped surveys — v12: export the FULL list from the DB (v11 wrote the
        #    in-memory list, which the UI path had truncated to the last 20 rows).
        try:
            skip_rows = []
            if sid and self.db:
                skip_rows = self.db.get_skipped_items(sid)
            if not skip_rows and self.state.skipped_surveys:
                skip_rows = self.state.skipped_surveys  # fallback
            if skip_rows:
                filename = f"bhoomi_skipped_surveys_{timestamp}_stop.csv"
                filepath = os.path.join(downloads, filename)
                fieldnames = ['village', 'village_name', 'village_code', 'survey_no',
                              'surnoc', 'hissa', 'period', 'reason', 'error_message',
                              'status', 'created_at', 'timestamp']
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(skip_rows)
                with self.state_lock:
                    self.state.logs.append(f"📥 Skipped surveys exported ({len(skip_rows)} rows): {filename}")
                logger.info(f"Auto-exported {len(skip_rows)} skipped surveys to {filepath}")
        except Exception as e:
            logger.error(f"Failed to auto-export skipped surveys on stop: {e}")

        # ═══════════════════════════════════════════════════════════════════════
        # v12: RESET COORDINATOR so a fresh search can start without a server restart.
        # (v11 left workers/executor/phase populated; combined with the stuck-running
        # bug this meant /api/search/start returned 409 forever.)
        # ═══════════════════════════════════════════════════════════════════════
        with self.state_lock:
            self.state.running = False
            self.state.completed = True
            self.state.current_phase = 'stopped'
        self.workers = []
        self.skeletons = {}
        self.executor = None
        logger.info("Stop search completed (graceful, coordinator reset)")
    
    def get_state(self) -> dict:
        """
        Get current search state as dict - SIMPLIFIED for stability.
        
        STABILITY FIXES:
        - All external calls OUTSIDE of lock
        - Lock timeout to prevent deadlocks
        - Minimal work inside lock
        """
        try:
            # Get portal health OUTSIDE of state_lock to avoid lock contention
            try:
                portal_health_stats = portal_health.get_stats() if portal_health else {
                    'current_state': 'UNKNOWN',
                    'ping_success_rate': 0,
                    'is_cooling_down': False
                }
            except Exception:
                portal_health_stats = {
                    'current_state': 'UNKNOWN',
                    'ping_success_rate': 0,
                    'is_cooling_down': False
                }
            
            # Get state manager info OUTSIDE of state_lock
            try:
                state_mgmt_info = {
                    'is_paused': self.state_manager.is_paused if self.state_manager else False,
                    'pause_reason': self.state_manager.pause_reason if self.state_manager else '',
                    'can_resume': self.state_manager is not None and self.state_manager.is_paused
                }
            except Exception:
                state_mgmt_info = {'is_paused': False, 'pause_reason': '', 'can_resume': False}
            
            # STABILITY: Use timeout lock acquisition
            lock_acquired = self.state_lock.acquire(timeout=2.0)
            if not lock_acquired:
                logger.warning("get_state() lock timeout - returning minimal state")
                return {
                    'running': getattr(self.state, 'running', False),
                    'completed': getattr(self.state, 'completed', False),
                    'start_time': '',
                    'owner_name': '',
                    'total_workers': Config.MAX_WORKERS,
                    'active_workers': 0,
                    'total_villages': 0,
                    'villages_completed': 0,
                    'total_records': getattr(self.state, 'total_records', 0),
                    'total_matches': getattr(self.state, 'total_matches', 0),
                    'progress': 0,
                    'all_records_file': '',
                    'matches_file': '',
                    'logs': ['⚠️ State lock timeout - retrying...'],
                    'all_records': [],
                    'matches': [],
                    'village_tracking': {'total_to_search': 0, 'processed': 0, 'retried': 0, 'failed': 0, 'session_recoveries': 0, 'failed_villages': []},
                    'smart_stop_metrics': {'enabled': Config.SMART_STOP_ENABLED, 'threshold': 50, 'smart_stops': 0, 'surveys_saved': 0, 'estimated_time_saved': '0 min'},
                    'accuracy_metrics': {'skipped_surveys_count': 0, 'skipped_surveys': [], 'villages_high_confidence': 0, 'villages_medium_confidence': 0, 'villages_low_confidence': 0, 'village_stats': {}},
                    'database': {'session_id': self.current_session_id, 'db_path': None, 'persistent': True},
                    'portal_health': portal_health_stats,
                    'state_management': state_mgmt_info,
                    'workers': {}
                }
            
            try:
                # Build workers dict safely
                workers_dict = {}
                if self.state.workers:
                    for wid, ws in self.state.workers.items():
                        try:
                            workers_dict[str(wid)] = {
                                'status': ws.status or 'idle',
                                'current_village': ws.current_village or '',
                                'current_survey': ws.current_survey or 0,
                                'max_survey': ws.max_survey or 0,
                                'villages_completed': ws.villages_completed or 0,
                                'villages_total': ws.villages_total or 0,
                                'records_found': ws.records_found or 0,
                                'matches_found': ws.matches_found or 0,
                                'progress': int((ws.villages_completed / max(ws.villages_total, 1)) * 100) if ws.villages_total else 0
                            }
                        except Exception as e:
                            logger.warning(f"Error getting worker {wid} state: {e}")
                            workers_dict[str(wid)] = {'status': 'error', 'current_village': '', 'progress': 0}
                
                state_dict = {
                    'running': self.state.running,
                    'completed': self.state.completed,
                    'start_time': self.state.start_time or '',
                    'owner_name': self.state.owner_name or '',
                    'total_workers': self.state.total_workers or 0,
                    'active_workers': self.state.active_workers or 0,
                    'total_villages': self.state.total_villages or 0,
                    'villages_completed': self.state.villages_completed or 0,
                    'total_records': self.state.total_records or 0,
                    'total_matches': self.state.total_matches or 0,
                    'progress': int((self.state.villages_completed / max(self.state.total_villages, 1)) * 100) if self.state.total_villages else 0,
                    'all_records_file': self.state.all_records_file or '',
                    'matches_file': self.state.matches_file or '',
                    # v11.0: bumped retention from 30 to 500 — survives browser refresh
                    'logs': list(self.state.logs[-Config.LOG_RETENTION_COUNT:]) if self.state.logs else [],
                    # Real-time records for UI (last 100)
                    # v11.0: bumped buffers — UI shows more on reconnect
                    'all_records': list(self.state.all_records[-Config.RECORDS_BUFFER_COUNT:]) if self.state.all_records else [],
                    'matches': list(self.state.matches[-Config.MATCHES_BUFFER_COUNT:]) if self.state.matches else [],
                    # BULLETPROOF VILLAGE TRACKING
                    'village_tracking': {
                        'total_to_search': len(self.state.villages_all) if self.state.villages_all else 0,
                        'processed': len(self.state.villages_processed) if self.state.villages_processed else 0,
                        'retried': len(self.state.villages_retried) if self.state.villages_retried else 0,
                        'failed': len(self.state.villages_failed) if self.state.villages_failed else 0,
                        'session_recoveries': self.state.session_recoveries or 0,
                        'failed_villages': list(self.state.villages_failed[-10:]) if self.state.villages_failed else [],
                    },
                    # ═══════════════════════════════════════════════════════════════════════
                    # SMART STOP & ACCURACY METRICS - For user confidence
                    # ═══════════════════════════════════════════════════════════════════════
                    'smart_stop_metrics': {
                        'enabled': Config.SMART_STOP_ENABLED,
                        'threshold': Config.EMPTY_SURVEY_THRESHOLD,
                        'smart_stops': self.state.smart_stops or 0,
                        'surveys_saved': self.state.surveys_saved or 0,
                        'estimated_time_saved': f"{(self.state.surveys_saved or 0) * 3 // 60} min",
                    },
                    'accuracy_metrics': {
                        'skipped_surveys_count': len(self.state.skipped_surveys) if self.state.skipped_surveys else 0,
                        'skipped_surveys': list(self.state.skipped_surveys[-20:]) if self.state.skipped_surveys else [],
                        'villages_high_confidence': sum(1 for v in (self.state.village_stats or {}).values() if v.get('confidence_score', 0) >= 80),
                        'villages_medium_confidence': sum(1 for v in (self.state.village_stats or {}).values() if 50 <= v.get('confidence_score', 0) < 80),
                        'villages_low_confidence': sum(1 for v in (self.state.village_stats or {}).values() if v.get('confidence_score', 0) < 50),
                        'village_stats': dict(list((self.state.village_stats or {}).items())[-10:]),
                    },
                    # Phase tracking
                    'current_phase': self.state.current_phase or 'idle',
                    'phase2': {
                        'enabled': Config.PHASE2_ENABLED,
                        'total': self.state.phase2_total,
                        'attempted': self.state.phase2_attempted,
                        'recovered': self.state.phase2_recovered,
                        'failed': self.state.phase2_failed,
                        'records_added': self.state.phase2_records_added,
                        'started_at': self.state.phase2_started_at or '',
                        'completed_at': self.state.phase2_completed_at or '',
                    },
                    # Database info
                    'database': {
                        'session_id': self.current_session_id,
                        'db_path': self.db.db_path if self.db else None,
                        'persistent': True  # Records are saved in real-time
                    },
                    # ═══════════════════════════════════════════════════════════════════════
                    # PORTAL HEALTH STATUS - Real-time portal monitoring (fetched outside lock)
                    # ═══════════════════════════════════════════════════════════════════════
                    'portal_health': portal_health_stats,
                    # State management (fetched outside lock)
                    'state_management': state_mgmt_info,
                    'workers': workers_dict
                }
                return state_dict
            finally:
                self.state_lock.release()
                
        except Exception as e:
            logger.error(f"Error getting state: {e}")
            # Return a safe default state
            return {
                'running': False,
                'completed': False,
                'start_time': '',
                'owner_name': '',
                'total_workers': 0,
                'active_workers': 0,
                'total_villages': 0,
                'villages_completed': 0,
                'total_records': 0,
                'total_matches': 0,
                'progress': 0,
                'all_records_file': '',
                'matches_file': '',
                'logs': [f'Error getting state: {str(e)}'],
                'all_records': [],
                'matches': [],
                'village_tracking': {'total_to_search': 0, 'processed': 0, 'retried': 0, 'failed': 0, 'session_recoveries': 0, 'failed_villages': []},
                'smart_stop_metrics': {'enabled': False, 'threshold': 50, 'smart_stops': 0, 'surveys_saved': 0, 'estimated_time_saved': '0 min'},
                'accuracy_metrics': {'skipped_surveys_count': 0, 'skipped_surveys': [], 'villages_high_confidence': 0, 'villages_medium_confidence': 0, 'villages_low_confidence': 0, 'village_stats': {}},
                'database': {'session_id': None, 'db_path': None, 'persistent': True},
                'portal_health': {'current_state': 'UNKNOWN', 'ping_success_rate': 0, 'is_cooling_down': False},
                'state_management': {'is_paused': False, 'pause_reason': '', 'can_resume': False},
                'workers': {}
            }

# ═══════════════════════════════════════════════════════════════════════════════════════
# FLASK APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)
CORS(app)

# Global instances
api = BhoomiAPI()

# v11.0: SINGLETON coordinator. Created ONCE per process, reused across all
# searches. This means:
#   - Server stays warm between searches (no DB-pool re-init, no S154 client re-init)
#   - State of the LAST search is queryable until a new one starts
#   - On server restart, mark stale 'running' DB sessions as 'interrupted' so
#     the UI doesn't lie about live work
coordinator = ParallelSearchCoordinator()
try:
    _interrupted_count = coordinator.db.mark_interrupted_sessions()
    if _interrupted_count > 0:
        logger.info(f"⚙️ v11.0 startup: marked {_interrupted_count} stale 'running' sessions as 'interrupted'")
except Exception as _e:
    logger.warning(f"v11.0 startup housekeeping failed: {_e}")

# ═══════════════════════════════════════════════════════════════════════════════════════
# HTML TEMPLATE (Enhanced with parallel worker visualization)
# ═══════════════════════════════════════════════════════════════════════════════════════

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>POWER-BHOOMI v12.0 | Completion-Hardened Edition</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=Noto+Sans+Kannada:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0e17;
            --bg-secondary: #111827;
            --bg-card: #1a2332;
            --bg-input: #0d1421;
            --accent-primary: #f59e0b;
            --accent-secondary: #d97706;
            --accent-glow: rgba(245, 158, 11, 0.3);
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
            --text-muted: #6b7280;
            --border-color: #374151;
            --success: #10b981;
            --error: #ef4444;
            --warning: #f59e0b;
            --info: #3b82f6;
        }
        
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Outfit', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            background-image: 
                radial-gradient(ellipse at 20% 20%, rgba(245, 158, 11, 0.08) 0%, transparent 50%),
                radial-gradient(ellipse at 80% 80%, rgba(217, 119, 6, 0.05) 0%, transparent 50%);
        }
        
        .kannada { font-family: 'Noto Sans Kannada', sans-serif; }
        .mono { font-family: 'JetBrains Mono', monospace; }
        
        /* Header */
        .header {
            padding: 1rem 2rem;
            background: linear-gradient(180deg, rgba(26, 35, 50, 0.95) 0%, transparent 100%);
            border-bottom: 1px solid rgba(245, 158, 11, 0.1);
            position: sticky;
            top: 0;
            z-index: 100;
            backdrop-filter: blur(20px);
        }
        
        .header-content {
            max-width: 1600px;
            margin: 0 auto;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        
        .logo {
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        
        .logo-icon {
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.5rem;
            box-shadow: 0 4px 20px var(--accent-glow);
        }
        
        .logo-text h1 {
            font-size: 1.4rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-primary), #fcd34d);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .logo-text p {
            font-size: 0.7rem;
            color: var(--text-secondary);
            letter-spacing: 2px;
            text-transform: uppercase;
        }
        
        .version-badge {
            padding: 0.35rem 0.75rem;
            background: rgba(59, 130, 246, 0.15);
            border: 1px solid rgba(59, 130, 246, 0.3);
            border-radius: 6px;
            font-size: 0.75rem;
            color: var(--info);
            font-weight: 600;
        }
        
        /* Main Layout */
        .main-container {
            max-width: 1600px;
            margin: 0 auto;
            padding: 1.5rem;
            display: grid;
            grid-template-columns: 380px 1fr;
            gap: 1.5rem;
        }
        
        /* Cards */
        .card {
            background: var(--bg-card);
            border-radius: 16px;
            border: 1px solid var(--border-color);
            padding: 1.5rem;
        }
        
        .card-title {
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 1.25rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .card-title::before {
            content: '';
            width: 3px;
            height: 20px;
            background: var(--accent-primary);
            border-radius: 2px;
        }
        
        /* Form Elements */
        .form-group { margin-bottom: 1rem; }
        
        .form-label {
            display: block;
            font-size: 0.8rem;
            font-weight: 500;
            color: var(--text-secondary);
            margin-bottom: 0.4rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .form-select, .form-input {
            width: 100%;
            padding: 0.75rem 1rem;
            background: var(--bg-input);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            color: var(--text-primary);
            font-family: inherit;
            font-size: 0.9rem;
            transition: all 0.2s;
        }
        
        .form-select {
            appearance: none;
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='20' height='20' viewBox='0 0 24 24' fill='none' stroke='%239ca3af' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 0.75rem center;
            padding-right: 2.5rem;
            cursor: pointer;
        }
        
        .form-select:focus, .form-input:focus {
            outline: none;
            border-color: var(--accent-primary);
            box-shadow: 0 0 0 3px var(--accent-glow);
        }
        
        .form-select:disabled { opacity: 0.5; cursor: not-allowed; }
        
        /* Buttons */
        .btn {
            padding: 0.875rem 1.5rem;
            border: none;
            border-radius: 10px;
            font-family: inherit;
            font-size: 0.95rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
        }
        
        .btn-primary {
            width: 100%;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
            color: var(--bg-primary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-top: 1rem;
        }
        
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 25px var(--accent-glow);
        }
        
        .btn-primary:disabled { opacity: 0.6; cursor: not-allowed; transform: none; }
        
        .btn-stop {
            background: linear-gradient(135deg, var(--error), #dc2626);
        }
        
        .btn-sm {
            padding: 0.5rem 1rem;
            font-size: 0.85rem;
        }
        
        /* Workers Panel */
        .workers-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 0.75rem;
            margin-bottom: 1rem;
        }
        
        .worker-card {
            background: var(--bg-input);
            border-radius: 10px;
            padding: 1rem;
            border: 1px solid var(--border-color);
        }
        
        .worker-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.75rem;
        }
        
        .worker-id {
            font-weight: 600;
            font-size: 0.85rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .worker-status {
            font-size: 0.7rem;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            text-transform: uppercase;
            font-weight: 600;
        }
        
        .worker-status.running { background: rgba(16, 185, 129, 0.2); color: var(--success); }
        .worker-status.completed { background: rgba(59, 130, 246, 0.2); color: var(--info); }
        .worker-status.failed { background: rgba(239, 68, 68, 0.2); color: var(--error); }
        .worker-status.idle { background: rgba(107, 114, 128, 0.2); color: var(--text-muted); }
        
        .worker-village {
            font-size: 0.8rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        
        .worker-progress {
            height: 4px;
            background: var(--bg-secondary);
            border-radius: 2px;
            overflow: hidden;
            margin-bottom: 0.5rem;
        }
        
        .worker-progress-fill {
            height: 100%;
            background: var(--accent-primary);
            transition: width 0.3s;
        }
        
        .worker-stats {
            display: flex;
            justify-content: space-between;
            font-size: 0.75rem;
            color: var(--text-muted);
        }
        
        .worker-survey-progress {
            font-size: 0.7rem;
            color: var(--accent-primary);
            margin-bottom: 0.5rem;
            font-family: 'JetBrains Mono', monospace;
        }
        
        .worker-records-count {
            font-size: 0.75rem;
            color: var(--success);
            font-weight: 600;
        }
        
        /* Heartbeat Indicator - UI Health Monitor */
        .heartbeat-container {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 1rem;
            background: var(--bg-input);
            border-radius: 8px;
            font-size: 0.8rem;
            margin-bottom: 1rem;
        }
        
        .heartbeat-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: var(--text-muted);
            transition: all 0.3s;
        }
        
        .heartbeat-dot.alive {
            background: var(--success);
            box-shadow: 0 0 8px var(--success);
            animation: pulse 1s ease-in-out;
        }
        
        .heartbeat-dot.stale {
            background: var(--warning);
            box-shadow: 0 0 8px var(--warning);
        }
        
        .heartbeat-dot.dead {
            background: var(--error);
            box-shadow: 0 0 8px var(--error);
            animation: blink 0.5s ease-in-out infinite;
        }
        
        @keyframes pulse {
            0% { transform: scale(1); opacity: 1; }
            50% { transform: scale(1.3); opacity: 0.7; }
            100% { transform: scale(1); opacity: 1; }
        }
        
        @keyframes blink {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }
        
        .heartbeat-text {
            color: var(--text-secondary);
        }
        
        .heartbeat-time {
            color: var(--text-primary);
            font-family: 'JetBrains Mono', monospace;
        }
        
        .heartbeat-status {
            margin-left: auto;
            font-weight: 500;
        }
        
        .heartbeat-status.ok { color: var(--success); }
        .heartbeat-status.warning { color: var(--warning); }
        .heartbeat-status.error { color: var(--error); }
        
        /* Portal Alert Banner */
        .portal-alert {
            background: linear-gradient(135deg, rgba(239, 68, 68, 0.15), rgba(220, 38, 38, 0.1));
            border: 1px solid var(--error);
            border-radius: 12px;
            padding: 1.25rem;
            margin-bottom: 1rem;
            display: flex;
            align-items: center;
            gap: 1rem;
            animation: slideDown 0.3s ease;
        }
        
        .portal-alert.warning {
            background: linear-gradient(135deg, rgba(245, 158, 11, 0.15), rgba(217, 119, 6, 0.1));
            border-color: var(--warning);
        }
        
        .portal-alert.success {
            background: linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(5, 150, 105, 0.1));
            border-color: var(--success);
        }
        
        @keyframes slideDown {
            from { opacity: 0; transform: translateY(-20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .alert-icon {
            font-size: 2rem;
            flex-shrink: 0;
        }
        
        .alert-content {
            flex: 1;
        }
        
        .alert-content h4 {
            margin: 0 0 0.25rem 0;
            font-size: 1rem;
            font-weight: 600;
            color: var(--text-primary);
        }
        
        .alert-content p {
            margin: 0;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }
        
        .alert-timer {
            padding: 0.5rem 1rem;
            background: var(--bg-secondary);
            border-radius: 8px;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }
        
        .alert-timer strong {
            color: var(--text-primary);
            font-family: 'JetBrains Mono', monospace;
            margin-left: 0.5rem;
        }
        
        .alert-dismiss {
            background: none;
            border: none;
            color: var(--text-muted);
            font-size: 1.5rem;
            cursor: pointer;
            padding: 0;
            line-height: 1;
            flex-shrink: 0;
        }
        
        .alert-dismiss:hover { color: var(--text-primary); }
        
        .btn-warning {
            background: linear-gradient(135deg, #f59e0b, #d97706);
            color: white;
        }
        
        .btn-success {
            background: linear-gradient(135deg, #10b981, #059669);
            color: white;
        }
        
        /* Overall Progress */
        .overall-progress {
            background: var(--bg-input);
            border-radius: 12px;
            padding: 1.25rem;
            margin-bottom: 1rem;
        }
        
        .progress-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 0.75rem;
        }
        
        .progress-label { font-size: 0.9rem; font-weight: 500; }
        .progress-percent { font-size: 1.5rem; font-weight: 700; color: var(--accent-primary); }
        
        .progress-bar {
            height: 10px;
            background: var(--bg-secondary);
            border-radius: 5px;
            overflow: hidden;
            margin-bottom: 0.75rem;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary));
            border-radius: 5px;
            transition: width 0.5s;
        }
        
        .progress-stats {
            display: flex;
            justify-content: space-around;
            text-align: center;
        }
        
        .progress-stat-value {
            font-size: 1.25rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        
        .progress-stat-label {
            font-size: 0.7rem;
            color: var(--text-muted);
            text-transform: uppercase;
        }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 0.75rem;
            margin-bottom: 1rem;
        }
        
        .stat-card {
            background: var(--bg-input);
            border-radius: 10px;
            padding: 1rem;
            text-align: center;
        }
        
        .stat-value {
            font-size: 1.75rem;
            font-weight: 700;
            color: var(--accent-primary);
        }
        
        .stat-label {
            font-size: 0.7rem;
            color: var(--text-muted);
            text-transform: uppercase;
            margin-top: 0.25rem;
        }
        
        /* Logs */
        .logs-container {
            background: var(--bg-input);
            border-radius: 10px;
            padding: 1rem;
            max-height: 200px;
            overflow-y: auto;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.75rem;
        }
        
        .log-entry {
            padding: 0.3rem 0;
            border-bottom: 1px solid rgba(255,255,255,0.03);
            color: var(--text-muted);
        }
        
        .log-entry:last-child { border-bottom: none; }
        
        /* Scrollbar */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: var(--bg-input); }
        ::-webkit-scrollbar-thumb { background: var(--border-color); border-radius: 3px; }
        
        /* Spinner */
        .spinner {
            width: 18px;
            height: 18px;
            border: 2px solid transparent;
            border-top-color: currentColor;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }
        
        @keyframes spin { to { transform: rotate(360deg); } }
        
        /* Tab Buttons */
        .tab-btn {
            padding: 0.6rem 1rem;
            background: var(--bg-input);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            color: var(--text-secondary);
            font-family: inherit;
            font-size: 0.85rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .tab-btn:hover { border-color: var(--accent-primary); color: var(--text-primary); }
        .tab-btn.active { background: var(--accent-primary); border-color: var(--accent-primary); color: var(--bg-primary); }
        
        .badge {
            background: var(--bg-secondary);
            padding: 0.15rem 0.5rem;
            border-radius: 10px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        
        .tab-btn.active .badge { background: rgba(0,0,0,0.2); color: var(--bg-primary); }
        .match-badge { background: var(--success) !important; color: white !important; }
        
        /* Data Table */
        .table-container {
            max-height: 350px;
            overflow-y: auto;
            border-radius: 10px;
            border: 1px solid var(--border-color);
        }
        
        .data-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85rem;
        }
        
        .data-table th {
            background: var(--bg-secondary);
            padding: 0.75rem 1rem;
            text-align: left;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            font-size: 0.7rem;
            letter-spacing: 0.5px;
            position: sticky;
            top: 0;
            z-index: 1;
        }
        
        .data-table td {
            padding: 0.6rem 1rem;
            border-bottom: 1px solid var(--border-color);
            color: var(--text-primary);
        }
        
        .data-table tr:hover td { background: rgba(245, 158, 11, 0.05); }
        .data-table tr.match-row td { background: rgba(16, 185, 129, 0.1); }
        .data-table tr.match-row:hover td { background: rgba(16, 185, 129, 0.15); }
        
        .empty-row {
            text-align: center;
            color: var(--text-muted);
            padding: 2rem !important;
        }
        
        .owner-cell { font-weight: 500; }
        .owner-cell.match { color: var(--success); }
        
        /* Responsive */
        @media (max-width: 1400px) {
            .workers-grid { grid-template-columns: repeat(3, 1fr); }
        }
        
        @media (max-width: 1200px) {
            .main-container { grid-template-columns: 1fr; }
            .workers-grid { grid-template-columns: repeat(2, 1fr); }
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
        }
        
        @media (max-width: 768px) {
            .workers-grid { grid-template-columns: 1fr; }
            #accuracySection { grid-template-columns: 1fr !important; }
            .portal-alert { flex-direction: column; text-align: center; }
            .alert-timer { margin-top: 0.5rem; }
        }
        
        /* Phase Banner */
        .phase-banner {
            display: none;
            padding: 0.75rem 1.25rem;
            border-radius: 10px;
            margin-bottom: 1rem;
            font-weight: 600;
            font-size: 0.95rem;
            text-align: center;
            transition: all 0.4s ease;
        }
        .phase-banner.phase1 {
            display: block;
            background: linear-gradient(90deg, rgba(59,130,246,0.15), rgba(59,130,246,0.05));
            border: 1px solid rgba(59,130,246,0.3);
            color: #3b82f6;
        }
        .phase-banner.phase1_done {
            display: block;
            background: linear-gradient(90deg, rgba(245,158,11,0.15), rgba(245,158,11,0.05));
            border: 1px solid rgba(245,158,11,0.3);
            color: #f59e0b;
            animation: pulse 1.5s ease-in-out infinite;
        }
        .phase-banner.phase2 {
            display: block;
            background: linear-gradient(90deg, rgba(249,115,22,0.15), rgba(249,115,22,0.05));
            border: 1px solid rgba(249,115,22,0.3);
            color: #f97316;
        }
        .phase-banner.completed {
            display: block;
            background: linear-gradient(90deg, rgba(16,185,129,0.15), rgba(16,185,129,0.05));
            border: 1px solid rgba(16,185,129,0.3);
            color: #10b981;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        .phase2-stats {
            display: none;
            background: var(--bg-input);
            border-radius: 10px;
            padding: 1rem 1.25rem;
            margin-bottom: 1rem;
            border: 1px solid rgba(249,115,22,0.2);
        }
        .phase2-stats .p2-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 0.75rem;
            text-align: center;
        }
        .phase2-stats .p2-grid .p2-item {
            padding: 0.5rem;
            border-radius: 8px;
            background: var(--bg-secondary);
        }
        .phase2-stats .p2-grid .p2-value {
            font-size: 1.5rem;
            font-weight: 700;
        }
        .phase2-stats .p2-grid .p2-label {
            font-size: 0.75rem;
            color: var(--text-secondary);
            margin-top: 0.25rem;
        }
        .p2-recovered { color: #10b981; }
        .p2-failed { color: #ef4444; }
        .p2-attempted { color: #f59e0b; }
        .p2-records { color: #3b82f6; }
    </style>
</head>
<body>
    <header class="header">
        <div class="header-content">
            <div class="logo">
                <div class="logo-icon">⚡</div>
                <div class="logo-text">
                    <h1>POWER-BHOOMI</h1>
                    <p>Parallel Search Engine</p>
                </div>
            </div>
            <div class="version-badge">v12.0 COMPLETION-HARDENED • Stall Watchdog • Graceful Stop • Coverage Pass • DB Phase 2</div>
        </div>
    </header>
    
    <main class="main-container">
        <aside class="card">
            <h2 class="card-title">Search Configuration</h2>
            
            <div class="form-group">
                <label class="form-label">Owner Name <span class="kannada">(ಮಾಲೀಕರ ಹೆಸರು)</span></label>
                <input type="text" id="ownerName" class="form-input kannada" placeholder="Enter owner name...">
            </div>
            
            <div class="form-group">
                <label class="form-label">District <span class="kannada">(ಜಿಲ್ಲೆ)</span></label>
                <select id="district" class="form-select">
                    <option value="">Loading...</option>
                </select>
            </div>
            
            <div class="form-group">
                <label class="form-label">Taluk <span class="kannada">(ತಾಲೂಕು)</span></label>
                <select id="taluk" class="form-select" disabled>
                    <option value="">Select district first</option>
                </select>
            </div>
            
            <div class="form-group">
                <label class="form-label">Hobli <span class="kannada">(ಹೋಬಳಿ)</span></label>
                <select id="hobli" class="form-select" disabled>
                    <option value="">Select taluk first</option>
                </select>
            </div>
            
            <div class="form-group">
                <label class="form-label">Village <span class="kannada">(ಗ್ರಾಮ)</span></label>
                <select id="village" class="form-select" disabled>
                    <option value="">Select hobli first</option>
                </select>
            </div>
            
            <div class="form-group">
                <label class="form-label">Max Survey Number <span style="font-size:0.7rem; color:var(--text-muted); font-weight:normal;">(fallback only — overridden by Service154 when reachable)</span></label>
                <input type="number" id="maxSurvey" class="form-input" value="800" min="1" max="2000">
                <div style="font-size: 0.7rem; color: var(--text-muted); margin-top: 0.3rem; line-height: 1.4;">
                    With Service154 skeleton mode (default), this number is <strong>ignored</strong>. The exact list of valid surveys per village is fetched up-front. This value is used only when Service154 is unreachable for a specific village (rare).
                </div>
            </div>
            
            <button id="searchBtn" class="btn btn-primary">
                <span>⚡</span>
                <span>Start Parallel Search</span>
            </button>
            
            <!-- Pause/Resume Controls -->
            <div id="searchControls" style="display: none; margin-top: 0.75rem; display: flex; gap: 0.5rem;">
                <button id="pauseBtn" class="btn btn-warning" style="flex: 1; background: linear-gradient(135deg, #f59e0b, #d97706); padding: 0.7rem;">
                    <span>⏸️</span>
                    <span>Pause</span>
                </button>
                <button id="resumeBtn" class="btn btn-success" style="flex: 1; background: linear-gradient(135deg, #10b981, #059669); padding: 0.7rem; display: none;">
                    <span>▶️</span>
                    <span>Resume</span>
                </button>
            </div>
        </aside>
        
        <section>
            <!-- Portal Health Alert Banner -->
            <div id="portalAlert" class="portal-alert" style="display: none;">
                <div class="alert-icon" id="alertIcon">⚠️</div>
                <div class="alert-content">
                    <h4 id="alertTitle">Portal Issue Detected</h4>
                    <p id="alertMessage">Monitoring portal status...</p>
                </div>
                <div class="alert-timer" id="alertTimer" style="display: none;">
                    <span id="timerLabel">Paused:</span>
                    <strong id="timerValue">0s</strong>
                </div>
                <button class="alert-dismiss" onclick="dismissPortalAlert()">×</button>
            </div>
            
            <!-- Heartbeat Indicator - Shows if UI is updating -->
            <div class="heartbeat-container" id="heartbeatContainer" style="display: none;">
                <div class="heartbeat-dot" id="heartbeatDot"></div>
                <span class="heartbeat-text">Last update:</span>
                <span class="heartbeat-time" id="heartbeatTime">--:--:--</span>
                <span class="heartbeat-status ok" id="heartbeatStatus">● Live</span>
            </div>
            
            <!-- Phase Banner -->
            <div class="phase-banner" id="phaseBanner"></div>
            
            <!-- Phase 2 Stats (shown only during/after Phase 2) -->
            <div class="phase2-stats" id="phase2Stats">
                <div style="font-weight:600; margin-bottom:0.75rem; color:var(--text-primary);">Phase 2: Skipped Survey Retry</div>
                <div class="p2-grid">
                    <div class="p2-item"><div class="p2-value p2-attempted" id="p2Attempted">0</div><div class="p2-label">Attempted</div></div>
                    <div class="p2-item"><div class="p2-value p2-recovered" id="p2Recovered">0</div><div class="p2-label">Recovered</div></div>
                    <div class="p2-item"><div class="p2-value p2-failed" id="p2Failed">0</div><div class="p2-label">Still Failed</div></div>
                    <div class="p2-item"><div class="p2-value p2-records" id="p2Records">0</div><div class="p2-label">New Records</div></div>
                </div>
                <div style="margin-top:0.5rem; text-align:center;">
                    <div class="progress-bar" style="height:6px;">
                        <div class="progress-fill" id="p2ProgressFill" style="width:0%; background:linear-gradient(90deg,#f97316,#ea580c);"></div>
                    </div>
                </div>
            </div>
            
            <!-- Overall Progress -->
            <div class="card" style="margin-bottom: 1rem;">
                <div class="overall-progress" id="progressSection" style="display: none;">
                    <div class="progress-header">
                        <span class="progress-label">Overall Progress</span>
                        <span class="progress-percent" id="progressPercent">0%</span>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" id="progressFill" style="width: 0%"></div>
                    </div>
                    <div class="progress-stats">
                        <div>
                            <div class="progress-stat-value" id="villagesCompleted">0</div>
                            <div class="progress-stat-label">Villages Done</div>
                        </div>
                        <div>
                            <div class="progress-stat-value" id="totalRecords">0</div>
                            <div class="progress-stat-label">Records</div>
                        </div>
                        <div>
                            <div class="progress-stat-value" id="totalMatches">0</div>
                            <div class="progress-stat-label">Matches</div>
                        </div>
                        <div>
                            <div class="progress-stat-value" id="activeWorkers">0</div>
                            <div class="progress-stat-label">Active Workers</div>
                        </div>
                    </div>
                </div>
                
                <!-- Confidence Score & Skipped Surveys -->
                <div style="display: none; grid-template-columns: 2fr 1fr; gap: 1rem; margin-top: 1rem;" id="accuracySection">
                    <!-- Confidence Score Meter -->
                    <div style="background: var(--bg-input); border-radius: 10px; padding: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                            <span style="font-size: 0.85rem; font-weight: 600; color: var(--text-secondary);">📊 SEARCH CONFIDENCE</span>
                            <span id="confidencePercent" style="font-size: 1.25rem; font-weight: 700; color: var(--accent-primary);">--</span>
                        </div>
                        <div style="height: 8px; background: var(--bg-secondary); border-radius: 4px; overflow: hidden; margin-bottom: 0.5rem;">
                            <div id="confidenceFill" style="height: 100%; background: linear-gradient(90deg, #10b981, #059669); width: 0%; transition: width 0.5s;"></div>
                        </div>
                        <div style="display: flex; justify-content: space-between; font-size: 0.7rem; color: var(--text-muted);">
                            <span>🟢 High: <strong id="highConfCount">0</strong></span>
                            <span>🟡 Medium: <strong id="medConfCount">0</strong></span>
                            <span>🔴 Low: <strong id="lowConfCount">0</strong></span>
                        </div>
                    </div>
                    
                    <!-- Skipped Surveys Counter -->
                    <div style="background: var(--bg-input); border-radius: 10px; padding: 1rem; display: flex; flex-direction: column; justify-content: center; align-items: center; border: 1px solid var(--border-color);" id="skippedCard">
                        <div style="font-size: 2rem; font-weight: 700; color: var(--warning);" id="skippedCount">0</div>
                        <div style="font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.5rem;">Skipped Surveys</div>
                        <button id="exportSkippedBtn" class="btn btn-sm" style="background: var(--warning); color: var(--bg-primary); padding: 0.4rem 0.8rem; font-size: 0.75rem; display: none;" onclick="exportSkippedSurveys()">
                            📥 Export List
                        </button>
                    </div>
                </div>
                
                <!-- Workers Grid (v10.0: dynamically populated based on actual worker count) -->
                <h3 class="card-title" style="margin-top: 1rem;">Browser Workers</h3>
                <div class="workers-grid" id="workersGrid"></div>
                
                <!-- Portal Health Status -->
                <div style="margin-top: 1rem; padding: 1rem; background: var(--bg-input); border-radius: 10px; border: 1px solid var(--border-color);">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem;">
                        <span style="font-size: 0.85rem; font-weight: 600; color: var(--text-secondary);">🏥 PORTAL HEALTH</span>
                        <span id="portalState" class="worker-status idle">UNKNOWN</span>
                    </div>
                    <div style="display: flex; gap: 1rem; font-size: 0.75rem; color: var(--text-muted);">
                        <span>📡 Response: <strong id="portalResponseTime">--</strong></span>
                        <span>✓ Success Rate: <strong id="portalSuccessRate">--</strong></span>
                        <span id="portalCooldown" style="display: none;">⏸️ Cooldown: <strong id="portalCooldownTime">0s</strong></span>
                    </div>
                </div>
            </div>
            
            <!-- Records Table with Tabs -->
            <div class="card" style="margin-bottom: 1rem;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                    <div style="display: flex; gap: 0.5rem;">
                        <button class="tab-btn active" id="tabRecords" onclick="switchTab('records')">
                            📋 All Records <span class="badge" id="recordsBadge">0</span>
                        </button>
                        <button class="tab-btn" id="tabMatches" onclick="switchTab('matches')">
                            🎯 Matches <span class="badge match-badge" id="matchesBadge">0</span>
                        </button>
                    </div>
                    <button id="exportBtn" class="btn btn-sm" style="background: var(--bg-input); border: 1px solid var(--border-color);" onclick="showDownloadModal()">
                        📥 Download CSV
                    </button>
                </div>
                
                <!-- Records Table -->
                <div class="table-container" id="recordsTable">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Village</th>
                                <th>Survey</th>
                                <th>Hissa</th>
                                <th>Owner Name</th>
                                <th>Extent</th>
                                <th>Worker</th>
                            </tr>
                        </thead>
                        <tbody id="recordsBody">
                            <tr><td colspan="6" class="empty-row">No records yet. Start a search to see results.</td></tr>
                        </tbody>
                    </table>
                </div>
                
                <!-- Matches Table (hidden by default) -->
                <div class="table-container" id="matchesTable" style="display: none;">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Village</th>
                                <th>Survey</th>
                                <th>Hissa</th>
                                <th>Owner Name</th>
                                <th>Extent</th>
                            </tr>
                        </thead>
                        <tbody id="matchesBody">
                            <tr><td colspan="5" class="empty-row">No matches found yet.</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
            
            <!-- Activity Log -->
            <div class="card">
                <h3 class="card-title">Activity Log</h3>
                <div class="logs-container" id="logsContainer">
                    <div class="log-entry">Ready to start parallel search...</div>
                </div>
            </div>
        </section>
    </main>
    
    <!-- Download Modal -->
    <div id="downloadModal" class="modal" style="display: none;">
        <div class="modal-overlay" onclick="hideDownloadModal()"></div>
        <div class="modal-content">
            <div class="modal-header">
                <h3>📥 Download CSV Files</h3>
                <button class="modal-close" onclick="hideDownloadModal()">×</button>
            </div>
            <div class="modal-body">
                <div class="download-section">
                    <div class="download-card" id="recordsDownloadCard">
                        <div class="download-icon">📋</div>
                        <div class="download-info">
                            <h4>All Records</h4>
                            <p id="recordsCount">0 records</p>
                            <p id="recordsPath" class="file-path"></p>
                        </div>
                        <div class="download-actions">
                            <input type="text" id="recordsFilename" placeholder="all_records.csv" class="filename-input">
                            <button class="btn btn-download" onclick="downloadFile('records')">
                                ⬇️ Download
                            </button>
                        </div>
                    </div>
                    
                    <div class="download-card match-card" id="matchesDownloadCard">
                        <div class="download-icon">🎯</div>
                        <div class="download-info">
                            <h4>Matches Only</h4>
                            <p id="matchesCount">0 matches</p>
                            <p id="matchesPath" class="file-path"></p>
                        </div>
                        <div class="download-actions">
                            <input type="text" id="matchesFilename" placeholder="owner_matches.csv" class="filename-input">
                            <button class="btn btn-download match-btn" onclick="downloadFile('matches')">
                                ⬇️ Download
                            </button>
                        </div>
                    </div>
                </div>
                
                <div class="modal-note">
                    <p>💡 Files are saved in the project directory. Click download to save a copy with your custom filename.</p>
                </div>
            </div>
        </div>
    </div>
    
    <style>
        /* Modal Styles */
        .modal {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            z-index: 1000;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        .modal-overlay {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.7);
            backdrop-filter: blur(4px);
        }
        
        .modal-content {
            position: relative;
            background: var(--bg-card);
            border-radius: 16px;
            border: 1px solid var(--border-color);
            width: 90%;
            max-width: 550px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
            animation: modalSlideIn 0.3s ease;
        }
        
        @keyframes modalSlideIn {
            from { opacity: 0; transform: translateY(-20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1.25rem 1.5rem;
            border-bottom: 1px solid var(--border-color);
        }
        
        .modal-header h3 {
            margin: 0;
            font-size: 1.2rem;
            color: var(--text-primary);
        }
        
        .modal-close {
            background: none;
            border: none;
            color: var(--text-muted);
            font-size: 1.5rem;
            cursor: pointer;
            padding: 0;
            line-height: 1;
        }
        
        .modal-close:hover { color: var(--text-primary); }
        
        .modal-body {
            padding: 1.5rem;
        }
        
        .download-section {
            display: flex;
            flex-direction: column;
            gap: 1rem;
        }
        
        .download-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.25rem;
            display: grid;
            grid-template-columns: auto 1fr auto;
            gap: 1rem;
            align-items: center;
        }
        
        .download-card.match-card {
            border-color: var(--success);
            background: rgba(16, 185, 129, 0.05);
        }
        
        .download-icon {
            font-size: 2rem;
            width: 50px;
            height: 50px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: var(--bg-input);
            border-radius: 10px;
        }
        
        .download-info h4 {
            margin: 0 0 0.25rem 0;
            color: var(--text-primary);
            font-size: 1rem;
        }
        
        .download-info p {
            margin: 0;
            color: var(--text-secondary);
            font-size: 0.85rem;
        }
        
        .file-path {
            font-size: 0.75rem !important;
            color: var(--text-muted) !important;
            font-family: monospace;
            word-break: break-all;
        }
        
        .download-actions {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }
        
        .filename-input {
            padding: 0.5rem 0.75rem;
            background: var(--bg-input);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            font-size: 0.85rem;
            width: 160px;
        }
        
        .filename-input:focus {
            outline: none;
            border-color: var(--accent-primary);
        }
        
        .btn-download {
            padding: 0.5rem 1rem;
            background: var(--accent-primary);
            color: var(--bg-primary);
            border: none;
            border-radius: 6px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .btn-download:hover { background: var(--accent-hover); }
        .btn-download.match-btn { background: var(--success); }
        .btn-download.match-btn:hover { background: #059669; }
        .btn-download:disabled { opacity: 0.5; cursor: not-allowed; }
        
        .modal-note {
            margin-top: 1rem;
            padding: 0.75rem 1rem;
            background: rgba(245, 158, 11, 0.1);
            border-radius: 8px;
            border-left: 3px solid var(--accent-primary);
        }
        
        .modal-note p {
            margin: 0;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }
    </style>
    
    <script>
        // State
        let searchRunning = false;
        let pollInterval = null;
        
        // Elements
        const districtSelect = document.getElementById('district');
        const talukSelect = document.getElementById('taluk');
        const hobliSelect = document.getElementById('hobli');
        const villageSelect = document.getElementById('village');
        const searchBtn = document.getElementById('searchBtn');
        const ownerInput = document.getElementById('ownerName');
        const maxSurveyInput = document.getElementById('maxSurvey');
        
        // ═══════════════════════════════════════════════════════════════════
        // v11.0: SESSION STATE REPLAY
        // ───────────────────────────────────────────────────────────────────
        // Track last-seen log id so we can fetch only NEW logs on each poll.
        // Track active session id so refreshing the browser restores context.
        // ═══════════════════════════════════════════════════════════════════
        let v11LastLogId = 0;
        let v11ActiveSessionId = null;
        let v11RecordOffset = 0;
        let v11RecordTotal = 0;
        let v11MatchOffset = 0;
        let v11MatchTotal = 0;
        
        async function v11RestoreSessionState() {
            // Called once on page load. If there's an active or recent session
            // in the database, populate the UI with its full backlog so the
            // user sees the same state they saw before refreshing.
            try {
                const res = await fetch('/api/session/current');
                if (!res.ok) return;
                const info = await res.json();
                
                if (!info.has_session || !info.session_id) {
                    return;  // no session yet — fresh UI is correct
                }
                
                v11ActiveSessionId = info.session_id;
                console.log('[v11] Restoring session:', v11ActiveSessionId,
                            'logs=', info.log_count, 'records=', info.record_count,
                            'matches=', info.match_count, 'active=', info.is_active);
                
                // Update top-line metrics from session metadata
                const totalRecords = document.getElementById('totalRecords');
                const totalMatches = document.getElementById('totalMatches');
                const recordsBadge = document.getElementById('recordsBadge');
                const matchesBadge = document.getElementById('matchesBadge');
                if (totalRecords) totalRecords.textContent = info.record_count || 0;
                if (totalMatches) totalMatches.textContent = info.match_count || 0;
                if (recordsBadge) recordsBadge.textContent = info.record_count || 0;
                if (matchesBadge) matchesBadge.textContent = info.match_count || 0;
                
                // Load all logs (paginated)
                await v11LoadAllLogs(v11ActiveSessionId);
                
                // Load first page of records + matches
                await v11LoadRecordsPage(v11ActiveSessionId, 0, false);
                await v11LoadRecordsPage(v11ActiveSessionId, 0, true);
                
                // Show a detailed banner when the prior session was interrupted
                if (info.session && info.session.status === 'interrupted') {
                    const sid = info.session_id || 'unknown';
                    const recs = (info.record_count || 0).toLocaleString();
                    const matches = (info.match_count || 0).toLocaleString();
                    const owner = (info.session.owner_name || '').trim();
                    const ownerStr = owner ? ` for owner '${owner}'` : '';
                    
                    addLog(`⚠️ Found previous session '${sid}' (interrupted on server restart):`);
                    addLog(`   ${recs} records and ${matches} matches preserved in database${ownerStr}.`);
                    addLog(`   Click 'Start Search' to begin fresh, or use Records/Matches tabs to inspect.`);
                }
                // Inform user when a session is currently active (running or completed)
                else if (info.session && info.session.status === 'running' && info.is_active) {
                    // ═══════════════════════════════════════════════════════════
                    // v12.2 FIX (session resurrection): restoring the DATA is not
                    // enough — the page must also reconnect the live CONTROLS,
                    // exactly as startSearch() does after a successful start.
                    // Without this, a browser crash/refresh during a run showed a
                    // dead idle form over a live search: Start button (→ 409 on
                    // click), no polling, frozen panels, while claiming "live
                    // updates streaming". Mirrors startSearch() lines post-start.
                    // ═══════════════════════════════════════════════════════════
                    searchRunning = true;
                    searchBtn.innerHTML = '<span class="spinner"></span><span>Stop Search</span>';
                    searchBtn.classList.add('btn-stop');
                    const progressSection = document.getElementById('progressSection');
                    if (progressSection) progressSection.style.display = 'block';
                    const controls = document.getElementById('searchControls');
                    if (controls) {
                        controls.style.display = 'flex';
                        document.getElementById('pauseBtn').style.display = 'block';
                        document.getElementById('resumeBtn').style.display = 'none';
                    }
                    const accuracySection = document.getElementById('accuracySection');
                    if (accuracySection) accuracySection.style.display = 'grid';
                    const heartbeatContainer = document.getElementById('heartbeatContainer');
                    if (heartbeatContainer) heartbeatContainer.style.display = 'flex';
                    // Restore the owner-name field so the visible form matches
                    // the run this page just reattached to.
                    if (info.session.owner_name && ownerInput && !ownerInput.value.trim()) {
                        ownerInput.value = info.session.owner_name;
                    }
                    // Start polling + heartbeat — guarded so a double invocation
                    // can never stack intervals.
                    if (!pollInterval) pollInterval = setInterval(pollStatus, 1500);
                    if (!heartbeatCheckInterval) heartbeatCheckInterval = setInterval(checkHeartbeat, 2000);
                    notRunningCount = 0;
                    lastUpdateTime = null;
                    addLog(`▶️ Reattached to running session '${info.session_id}' — live updates resumed.`);
                }
                else if (info.session && info.session.status === 'completed') {
                    const recs = (info.record_count || 0).toLocaleString();
                    const matches = (info.match_count || 0).toLocaleString();
                    addLog(`✅ Last session '${info.session_id}' completed: ${recs} records, ${matches} matches.`);
                }
                else if (info.session && info.session.status === 'stopped') {
                    addLog(`⏹️ Last session '${info.session_id}' was stopped manually.`);
                }
            } catch (e) {
                console.warn('[v11] Session restore failed:', e);
            }
        }
        
        async function v11LoadAllLogs(sessionId) {
            // Walks the DB in 500-row pages until exhausted, keeps v11LastLogId in sync
            const container = document.getElementById('logsContainer');
            if (!container) return;
            
            container.innerHTML = '';
            let since = 0;
            let totalLoaded = 0;
            const MAX_BACKFILL = 5000;  // soft cap so a 50,000-log session doesn't crush the browser
            
            while (totalLoaded < MAX_BACKFILL) {
                const r = await fetch(`/api/session/${encodeURIComponent(sessionId)}/logs?since_id=${since}&limit=500`);
                if (!r.ok) break;
                const data = await r.json();
                if (!data.logs || data.logs.length === 0) break;
                
                // Render in chronological order at top (newest at top in our UI)
                const fragment = document.createDocumentFragment();
                data.logs.forEach(row => {
                    const div = document.createElement('div');
                    div.className = `log-entry log-${(row.level || 'INFO').toLowerCase()}`;
                    div.dataset.logId = row.id;
                    div.textContent = row.message;
                    fragment.appendChild(div);
                });
                // Newest-on-top means we PREPEND
                if (container.firstChild) {
                    container.insertBefore(fragment, container.firstChild);
                } else {
                    container.appendChild(fragment);
                }
                
                since = data.next_since_id;
                totalLoaded += data.logs.length;
                v11LastLogId = Math.max(v11LastLogId, since);
                
                if (!data.has_more) break;
            }
            
            console.log(`[v11] Restored ${totalLoaded} logs from DB`);
        }
        
        async function v11FetchIncrementalLogs() {
            // Called every poll. Fetches only NEW log rows since v11LastLogId.
            if (!v11ActiveSessionId) return;
            try {
                const r = await fetch(`/api/session/${encodeURIComponent(v11ActiveSessionId)}/logs?since_id=${v11LastLogId}&limit=500`);
                if (!r.ok) return;
                const data = await r.json();
                if (!data.logs || data.logs.length === 0) return;
                
                const container = document.getElementById('logsContainer');
                if (!container) return;
                
                const fragment = document.createDocumentFragment();
                data.logs.forEach(row => {
                    const div = document.createElement('div');
                    div.className = `log-entry log-${(row.level || 'INFO').toLowerCase()}`;
                    div.dataset.logId = row.id;
                    div.textContent = row.message;
                    fragment.appendChild(div);
                });
                
                // Newest at top
                if (container.firstChild) {
                    container.insertBefore(fragment, container.firstChild);
                } else {
                    container.appendChild(fragment);
                }
                
                v11LastLogId = Math.max(v11LastLogId, data.next_since_id);
                
                // Cap DOM size to prevent memory blowup over long runs
                while (container.children.length > 5000) {
                    container.removeChild(container.lastChild);
                }
            } catch (e) {
                console.warn('[v11] Incremental log fetch failed:', e);
            }
        }
        
        async function v11LoadRecordsPage(sessionId, offset, matchesOnly) {
            try {
                const r = await fetch(`/api/session/${encodeURIComponent(sessionId)}/records?offset=${offset}&limit=200&matches_only=${matchesOnly}`);
                if (!r.ok) return;
                const data = await r.json();
                
                if (matchesOnly) {
                    if (offset === 0 && typeof updateMatchesTable === 'function') {
                        updateMatchesTable(data.records);
                    }
                    v11MatchTotal = data.total;
                    v11MatchOffset = offset + (data.records || []).length;
                } else {
                    if (offset === 0 && typeof updateRecordsTable === 'function') {
                        updateRecordsTable(data.records);
                    }
                    v11RecordTotal = data.total;
                    v11RecordOffset = offset + (data.records || []).length;
                }
                
                // Show pagination info on the badge
                const recordsBadge = document.getElementById('recordsBadge');
                const matchesBadge = document.getElementById('matchesBadge');
                if (matchesOnly && matchesBadge) matchesBadge.textContent = data.total || 0;
                if (!matchesOnly && recordsBadge) recordsBadge.textContent = data.total || 0;
            } catch (e) {
                console.warn('[v11] Record page load failed:', e);
            }
        }
        
        // Initialize
        document.addEventListener('DOMContentLoaded', () => {
            loadDistricts();
            setupEventListeners();
            // v11.0: Restore previous session state if any.
            // Runs in background — doesn't block the form.
            v11RestoreSessionState();
        });
        
        function setupEventListeners() {
            districtSelect.addEventListener('change', () => {
                const code = districtSelect.value;
                if (code) loadTaluks(code);
                else resetDropdowns(['taluk', 'hobli', 'village']);
            });
            
            talukSelect.addEventListener('change', () => {
                const distCode = districtSelect.value;
                const talukCode = talukSelect.value;
                if (talukCode) loadHoblis(distCode, talukCode);
                else resetDropdowns(['hobli', 'village']);
            });
            
            hobliSelect.addEventListener('change', () => {
                const distCode = districtSelect.value;
                const talukCode = talukSelect.value;
                const hobliCode = hobliSelect.value;
                if (hobliCode === 'all') {
                    villageSelect.innerHTML = '<option value="all">🔍 All Villages (All Hoblis)</option>';
                    villageSelect.disabled = false;
                } else if (hobliCode) {
                    loadVillages(distCode, talukCode, hobliCode);
                } else {
                    resetDropdowns(['village']);
                }
            });
            
            searchBtn.addEventListener('click', toggleSearch);
            
            // Pause/Resume button handlers
            const pauseBtn = document.getElementById('pauseBtn');
            const resumeBtn = document.getElementById('resumeBtn');
            if (pauseBtn) pauseBtn.addEventListener('click', pauseSearch);
            if (resumeBtn) resumeBtn.addEventListener('click', resumeSearch);
        }
        
        async function pauseSearch() {
            try {
                await fetch('/api/search/pause', {method: 'POST'});
                document.getElementById('pauseBtn').style.display = 'none';
                document.getElementById('resumeBtn').style.display = 'block';
                addLog('⏸️ Search paused by user');
            } catch (e) {
                console.error('Pause failed:', e);
            }
        }
        
        async function resumeSearch() {
            try {
                await fetch('/api/search/resume', {method: 'POST'});
                document.getElementById('resumeBtn').style.display = 'none';
                document.getElementById('pauseBtn').style.display = 'block';
                addLog('▶️ Search resumed by user');
            } catch (e) {
                console.error('Resume failed:', e);
            }
        }
        
        function dismissPortalAlert() {
            const alert = document.getElementById('portalAlert');
            if (alert) alert.style.display = 'none';
        }
        
        async function exportSkippedSurveys() {
            try {
                window.location.href = '/api/skipped/current/export';
                addLog('📥 Exporting skipped surveys...');
            } catch (e) {
                console.error('Export failed:', e);
            }
        }
        
        async function loadDistricts() {
            try {
                const res = await fetch('/api/districts');
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}`);
                }
                const data = await res.json();
                
                // Check if data is valid array
                if (!data || !Array.isArray(data) || data.length === 0) {
                    districtSelect.innerHTML = '<option value="">⚠️ No districts found (API error)</option>';
                    console.error('Invalid districts data:', data);
                    addLog('⚠️ Failed to load districts - API returned empty data');
                    return;
                }
                
                districtSelect.innerHTML = '<option value="">Select District</option>';
                data.forEach(d => {
                    const name = d.district_name_kn || d.district_code;
                    districtSelect.innerHTML += `<option value="${d.district_code}">${name}</option>`;
                });
                console.log(`Loaded ${data.length} districts`);
            } catch (e) {
                districtSelect.innerHTML = '<option value="">⚠️ Error loading districts</option>';
                console.error('Error loading districts:', e);
                addLog('❌ Failed to load districts: ' + e.message);
            }
        }
        
        async function loadTaluks(distCode) {
            resetDropdowns(['taluk', 'hobli', 'village']);
            talukSelect.innerHTML = '<option value="">Loading...</option>';
            try {
                const res = await fetch(`/api/taluks/${distCode}`);
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}`);
                }
                const data = await res.json();
                
                // Check if data is valid array
                if (!data || !Array.isArray(data) || data.length === 0) {
                    talukSelect.innerHTML = '<option value="">⚠️ No taluks found (API error)</option>';
                    console.error('Invalid taluks data:', data);
                    addLog('⚠️ Failed to load taluks - API returned empty data');
                    return;
                }
                
                talukSelect.innerHTML = '<option value="">Select Taluk</option>';
                data.forEach(t => {
                    const name = t.taluka_name_kn || t.taluka_code;
                    talukSelect.innerHTML += `<option value="${t.taluka_code}">${name}</option>`;
                });
                talukSelect.disabled = false;
                console.log(`Loaded ${data.length} taluks`);
            } catch (e) {
                talukSelect.innerHTML = '<option value="">⚠️ Error loading taluks</option>';
                console.error('Error loading taluks:', e);
                addLog('❌ Failed to load taluks: ' + e.message);
            }
        }
        
        async function loadHoblis(distCode, talukCode) {
            resetDropdowns(['hobli', 'village']);
            hobliSelect.innerHTML = '<option value="">Loading...</option>';
            try {
                const res = await fetch(`/api/hoblis/${distCode}/${talukCode}`);
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}`);
                }
                const data = await res.json();
                
                // Check if data is valid array
                if (!data || !Array.isArray(data) || data.length === 0) {
                    hobliSelect.innerHTML = '<option value="">⚠️ No hoblis found (API error)</option>';
                    console.error('Invalid hoblis data:', data);
                    addLog('⚠️ Failed to load hoblis - API returned empty data');
                    return;
                }
                
                hobliSelect.innerHTML = '<option value="">Select Hobli</option>';
                hobliSelect.innerHTML += '<option value="all">🔍 All Hoblis (Search Entire Taluk)</option>';
                data.forEach(h => {
                    const name = h.hobli_name_kn || h.hobli_code;
                    hobliSelect.innerHTML += `<option value="${h.hobli_code}">${name}</option>`;
                });
                hobliSelect.disabled = false;
                console.log(`Loaded ${data.length} hoblis`);
            } catch (e) {
                hobliSelect.innerHTML = '<option value="">⚠️ Error loading hoblis</option>';
                console.error('Error loading hoblis:', e);
                addLog('❌ Failed to load hoblis: ' + e.message);
            }
        }
        
        async function loadVillages(distCode, talukCode, hobliCode) {
            resetDropdowns(['village']);
            villageSelect.innerHTML = '<option value="">Loading...</option>';
            try {
                const res = await fetch(`/api/villages/${distCode}/${talukCode}/${hobliCode}`);
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}`);
                }
                const data = await res.json();
                
                // Check if data is valid array
                if (!data || !Array.isArray(data) || data.length === 0) {
                    villageSelect.innerHTML = '<option value="">⚠️ No villages found (API error)</option>';
                    console.error('Invalid villages data:', data);
                    addLog('⚠️ Failed to load villages - API returned empty data');
                    return;
                }
                
                villageSelect.innerHTML = '<option value="">Select Village</option>';
                villageSelect.innerHTML += '<option value="all">🔍 All Villages (in this Hobli)</option>';
                data.forEach(v => {
                    const name = v.village_name_kn || v.village_code;
                    villageSelect.innerHTML += `<option value="${v.village_code}">${name}</option>`;
                });
                villageSelect.disabled = false;
                console.log(`Loaded ${data.length} villages`);
            } catch (e) {
                villageSelect.innerHTML = '<option value="">⚠️ Error loading villages</option>';
                console.error('Error loading villages:', e);
                addLog('❌ Failed to load villages: ' + e.message);
            }
        }
        
        function resetDropdowns(ids) {
            ids.forEach(id => {
                const el = document.getElementById(id);
                el.innerHTML = `<option value="">Select ${id} first</option>`;
                el.disabled = true;
            });
        }
        
        async function toggleSearch() {
            if (searchRunning) {
                await stopSearch();
            } else {
                await startSearch();
            }
        }
        
        async function startSearch() {
            const ownerName = ownerInput.value.trim();
            if (!ownerName) {
                alert('Please enter an owner name');
                return;
            }
            
            const districtCode = districtSelect.value;
            const talukCode = talukSelect.value;
            const hobliCode = hobliSelect.value || 'all';
            const villageCode = villageSelect.value || 'all';
            const maxSurvey = parseInt(maxSurveyInput.value) || 800;
            
            if (!districtCode || !talukCode) {
                alert('Please select District and Taluk');
                return;
            }
            
            // Validate max survey number (fallback bound only — skeleton overrides)
            if (maxSurvey <= 0 || maxSurvey > 2000) {
                alert('Max Survey must be between 1 and 2000. Using default: 800');
                maxSurveyInput.value = '800';
                return;
            }
            
            searchRunning = true;
            searchBtn.innerHTML = '<span class="spinner"></span><span>Stop Search</span>';
            searchBtn.classList.add('btn-stop');
            document.getElementById('progressSection').style.display = 'block';
            
            // Show search controls (pause button)
            const controls = document.getElementById('searchControls');
            if (controls) {
                controls.style.display = 'flex';
                document.getElementById('pauseBtn').style.display = 'block';
                document.getElementById('resumeBtn').style.display = 'none';
            }
            
            // Show accuracy section
            const accuracySection = document.getElementById('accuracySection');
            if (accuracySection) accuracySection.style.display = 'grid';
            
            addLog('🚀 Starting parallel search...');
            
            try {
                const startRes = await fetch('/api/search/start', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        owner_name: ownerName,
                        district_code: districtCode,
                        taluk_code: talukCode,
                        hobli_code: hobliCode,
                        village_code: villageCode,
                        max_survey: maxSurvey
                    })
                });
                // v12.1 FIX: surface start failures. Previously a 409 ("search already
                // running") was silently swallowed — the UI flipped to searching mode
                // and displayed the EXISTING run, which users read as stale data.
                if (!startRes.ok) {
                    let errMsg = 'Failed to start search (HTTP ' + startRes.status + ')';
                    try {
                        const errBody = await startRes.json();
                        if (errBody && errBody.error) errMsg = errBody.error;
                    } catch (e) { /* non-JSON error body */ }
                    alert('⚠️ ' + errMsg);
                    addLog('❌ Start rejected: ' + errMsg);
                    searchRunning = false;
                    searchBtn.innerHTML = '<span>Start Search</span>';
                    searchBtn.classList.remove('btn-stop');
                    return;
                }

                // Show heartbeat indicator
                const heartbeatContainer = document.getElementById('heartbeatContainer');
                if (heartbeatContainer) heartbeatContainer.style.display = 'flex';
                
                // Start polling and heartbeat monitoring
                pollInterval = setInterval(pollStatus, 1500);
                heartbeatCheckInterval = setInterval(checkHeartbeat, 2000);
                
                // Reset heartbeat state
                notRunningCount = 0;
                lastUpdateTime = null;
                
            } catch (e) {
                addLog('❌ Error starting search');
                stopSearch();
            }
        }
        
        async function stopSearch() {
            try {
                await fetch('/api/search/stop', {method: 'POST'});
            } catch (e) {}
            
            searchRunning = false;
            searchBtn.innerHTML = '<span>⚡</span><span>Start Parallel Search</span>';
            searchBtn.classList.remove('btn-stop');
            
            // Hide search controls
            const controls = document.getElementById('searchControls');
            if (controls) controls.style.display = 'none';
            
            // Hide portal alert
            const portalAlert = document.getElementById('portalAlert');
            if (portalAlert) portalAlert.style.display = 'none';
            
            if (pollInterval) {
                clearInterval(pollInterval);
                pollInterval = null;
            }
            
            // Clear heartbeat monitoring
            if (heartbeatCheckInterval) {
                clearInterval(heartbeatCheckInterval);
                heartbeatCheckInterval = null;
            }
            
            // Update heartbeat display to show stopped state
            const heartbeatStatus = document.getElementById('heartbeatStatus');
            const heartbeatDot = document.getElementById('heartbeatDot');
            if (heartbeatStatus) {
                heartbeatStatus.textContent = '■ Stopped';
                heartbeatStatus.className = 'heartbeat-status';
            }
            if (heartbeatDot) {
                heartbeatDot.className = 'heartbeat-dot';
            }
        }
        
        // Track consecutive "not running" states to prevent false positives
        let notRunningCount = 0;
        const STOP_THRESHOLD = 3; // Require 3 consecutive "not running" polls before stopping
        
        // Heartbeat tracking - detects if UI is frozen
        let lastUpdateTime = null;
        let heartbeatCheckInterval = null;
        
        function updateHeartbeat() {
            lastUpdateTime = new Date();
            const timeStr = lastUpdateTime.toLocaleTimeString();
            
            const dot = document.getElementById('heartbeatDot');
            const time = document.getElementById('heartbeatTime');
            const status = document.getElementById('heartbeatStatus');
            
            if (time) time.textContent = timeStr;
            if (dot) {
                dot.className = 'heartbeat-dot alive';
                // Remove animation class after it completes to allow re-triggering
                setTimeout(() => {
                    if (dot) dot.classList.remove('alive');
                }, 1000);
            }
            if (status) {
                status.textContent = '● Live';
                status.className = 'heartbeat-status ok';
            }
        }
        
        function checkHeartbeat() {
            if (!lastUpdateTime || !searchRunning) return;
            
            const now = new Date();
            const diffSeconds = (now - lastUpdateTime) / 1000;
            
            const dot = document.getElementById('heartbeatDot');
            const status = document.getElementById('heartbeatStatus');
            
            if (diffSeconds > 10) {
                // UI is frozen - no update for 10+ seconds
                if (dot) dot.className = 'heartbeat-dot dead';
                if (status) {
                    status.textContent = '⚠️ FROZEN (' + Math.floor(diffSeconds) + 's ago)';
                    status.className = 'heartbeat-status error';
                }
                console.error('UI FROZEN: No update for ' + Math.floor(diffSeconds) + ' seconds');
            } else if (diffSeconds > 5) {
                // UI is stale - no update for 5+ seconds
                if (dot) dot.className = 'heartbeat-dot stale';
                if (status) {
                    status.textContent = '● Slow (' + Math.floor(diffSeconds) + 's)';
                    status.className = 'heartbeat-status warning';
                }
            }
        }
        
        async function pollStatus() {
            // Don't poll if we've already stopped locally
            if (!searchRunning) return;
            
            try {
                const res = await fetch('/api/search/status');
                if (!res.ok) {
                    console.error('Poll status failed:', res.status);
                    return; // Don't stop polling on network errors
                }
                
                const status = await res.json();
                
                // Defensive: check if status object is valid
                if (!status || typeof status !== 'object') {
                    console.error('Invalid status response');
                    return;
                }
                
                // Update overall progress (with null checks)
                const progressPercent = document.getElementById('progressPercent');
                const progressFill = document.getElementById('progressFill');
                const villagesCompleted = document.getElementById('villagesCompleted');
                const totalRecords = document.getElementById('totalRecords');
                const totalMatches = document.getElementById('totalMatches');
                const activeWorkers = document.getElementById('activeWorkers');
                
                if (progressPercent) progressPercent.textContent = (status.progress || 0) + '%';
                if (progressFill) progressFill.style.width = (status.progress || 0) + '%';
                if (villagesCompleted) villagesCompleted.textContent = `${status.villages_completed || 0}/${status.total_villages || 0}`;
                if (totalRecords) totalRecords.textContent = status.total_records || 0;
                if (totalMatches) totalMatches.textContent = status.total_matches || 0;
                if (activeWorkers) activeWorkers.textContent = status.active_workers || 0;
                
                // Update badges (with null checks)
                const recordsBadge = document.getElementById('recordsBadge');
                const matchesBadge = document.getElementById('matchesBadge');
                if (recordsBadge) recordsBadge.textContent = status.total_records || 0;
                if (matchesBadge) matchesBadge.textContent = status.total_matches || 0;
                
                // v10.0: Dynamically create worker cards if missing.
                // Worker count comes from server (status.total_workers); this means
                // the UI auto-adjusts when MAX_WORKERS changes — no hardcoded count.
                if (status.workers) {
                    const workersGrid = document.getElementById('workersGrid');
                    Object.keys(status.workers).forEach((id) => {
                        let card = document.getElementById(`worker-${id}`);
                        if (!card && workersGrid) {
                            // Create missing worker card
                            card = document.createElement('div');
                            card.className = 'worker-card';
                            card.id = `worker-${id}`;
                            card.innerHTML = `
                                <div class="worker-header">
                                    <span class="worker-id">🖥️ Worker ${parseInt(id, 10) + 1}</span>
                                    <span class="worker-status idle">Idle</span>
                                </div>
                                <div class="worker-village">Waiting to start...</div>
                                <div class="worker-survey-progress" style="display: none;">Survey: <span class="worker-survey-num">--</span></div>
                                <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                                <div class="worker-stats"><span>0/0 villages</span><span class="worker-records-count">0 records</span></div>
                            `;
                            // Insert in numeric order so cards stay sorted
                            const idNum = parseInt(id, 10);
                            const existing = Array.from(workersGrid.children);
                            const insertBefore = existing.find(c => parseInt((c.id || '').replace('worker-', ''), 10) > idNum);
                            if (insertBefore) {
                                workersGrid.insertBefore(card, insertBefore);
                            } else {
                                workersGrid.appendChild(card);
                            }
                        }
                    });
                    Object.entries(status.workers).forEach(([id, w]) => {
                        const card = document.getElementById(`worker-${id}`);
                        if (card && w) {
                            const statusEl = card.querySelector('.worker-status');
                            const villageEl = card.querySelector('.worker-village');
                            const surveyProgressEl = card.querySelector('.worker-survey-progress');
                            const surveyNumEl = card.querySelector('.worker-survey-num');
                            const progressEl = card.querySelector('.worker-progress-fill');
                            const statsEl = card.querySelector('.worker-stats');
                            const recordsCountEl = card.querySelector('.worker-records-count');
                            
                            if (statusEl) {
                                statusEl.textContent = w.status || 'idle';
                                statusEl.className = `worker-status ${w.status || 'idle'}`;
                            }
                            if (villageEl) villageEl.textContent = w.current_village || 'Waiting...';
                            
                            // Show survey progress if worker is running
                            if (surveyProgressEl && surveyNumEl) {
                                if (w.status === 'running' && w.current_survey > 0) {
                                    surveyProgressEl.style.display = 'block';
                                    surveyNumEl.textContent = `${w.current_survey || 0}/${w.max_survey || 0}`;
                                } else {
                                    surveyProgressEl.style.display = 'none';
                                }
                            }
                            
                            if (progressEl) progressEl.style.width = (w.progress || 0) + '%';
                            if (statsEl) statsEl.innerHTML = 
                                `<span>${w.villages_completed || 0}/${w.villages_total || 0} villages</span><span class="worker-records-count">${w.records_found || 0} records</span>`;
                        }
                    });
                }
                
                // Update Confidence Score
                if (status.accuracy_metrics) {
                    const am = status.accuracy_metrics;
                    const confidencePercent = document.getElementById('confidencePercent');
                    const confidenceFill = document.getElementById('confidenceFill');
                    const highConfCount = document.getElementById('highConfCount');
                    const medConfCount = document.getElementById('medConfCount');
                    const lowConfCount = document.getElementById('lowConfCount');
                    
                    // v11.0: Calculate average confidence if we have village stats.
                    // Confidence is a per-village metric (calculated when village
                    // completes), so it stays empty until the first village finishes.
                    const completedVillages = (am.villages_high_confidence || 0) + (am.villages_medium_confidence || 0) + (am.villages_low_confidence || 0);
                    
                    if (completedVillages > 0) {
                        // Weighted average: High=90%, Med=65%, Low=30%
                        const avgConf = Math.round(
                            ((am.villages_high_confidence || 0) * 90 + 
                             (am.villages_medium_confidence || 0) * 65 + 
                             (am.villages_low_confidence || 0) * 30) / completedVillages
                        );
                        if (confidencePercent) confidencePercent.textContent = avgConf + '%';
                        if (confidenceFill) {
                            confidenceFill.style.width = avgConf + '%';
                            confidenceFill.style.opacity = '1';
                            if (avgConf >= 80) {
                                confidenceFill.style.background = 'linear-gradient(90deg, #10b981, #059669)';
                            } else if (avgConf >= 50) {
                                confidenceFill.style.background = 'linear-gradient(90deg, #f59e0b, #d97706)';
                            } else {
                                confidenceFill.style.background = 'linear-gradient(90deg, #ef4444, #dc2626)';
                            }
                        }
                    } else if (status.running) {
                        // Search active but no village completed yet — show calculating state
                        if (confidencePercent) confidencePercent.textContent = 'calculating…';
                        if (confidenceFill) {
                            confidenceFill.style.width = '100%';
                            confidenceFill.style.opacity = '0.3';
                            confidenceFill.style.background = 'linear-gradient(90deg, #6366f1, #818cf8)';
                        }
                    } else {
                        // Idle / no session yet
                        if (confidencePercent) confidencePercent.textContent = '--';
                        if (confidenceFill) {
                            confidenceFill.style.width = '0%';
                            confidenceFill.style.opacity = '1';
                        }
                    }
                    
                    if (highConfCount) highConfCount.textContent = am.villages_high_confidence || 0;
                    if (medConfCount) medConfCount.textContent = am.villages_medium_confidence || 0;
                    if (lowConfCount) lowConfCount.textContent = am.villages_low_confidence || 0;
                }
                
                // Update Skipped Surveys Counter
                if (status.accuracy_metrics) {
                    const skippedCount = status.accuracy_metrics.skipped_surveys_count || 0;
                    const skippedCountEl = document.getElementById('skippedCount');
                    const exportSkippedBtn = document.getElementById('exportSkippedBtn');
                    const skippedCard = document.getElementById('skippedCard');
                    
                    if (skippedCountEl) skippedCountEl.textContent = skippedCount;
                    
                    // Show export button if there are skipped surveys
                    if (exportSkippedBtn) {
                        exportSkippedBtn.style.display = skippedCount > 0 ? 'block' : 'none';
                    }
                    
                    // Change card border color if high skip count
                    if (skippedCard) {
                        if (skippedCount > 20) {
                            skippedCard.style.borderColor = 'var(--error)';
                        } else if (skippedCount > 5) {
                            skippedCard.style.borderColor = 'var(--warning)';
                        } else {
                            skippedCard.style.borderColor = 'var(--border-color)';
                        }
                    }
                }
                
                // ═══════════════════════════════════════════════════
                // PHASE BANNER & PHASE 2 STATS
                // ═══════════════════════════════════════════════════
                const phaseBanner = document.getElementById('phaseBanner');
                const phase2Stats = document.getElementById('phase2Stats');
                const phase = status.current_phase || 'idle';
                
                if (phaseBanner) {
                    phaseBanner.className = 'phase-banner ' + phase;
                    if (phase === 'phase1') {
                        phaseBanner.textContent = 'PHASE 1: Primary Search In Progress';
                    } else if (phase === 'phase1_done') {
                        phaseBanner.textContent = 'Phase 1 Complete — Preparing Phase 2 Retry...';
                    } else if (phase === 'phase2') {
                        const p2 = status.phase2 || {};
                        phaseBanner.textContent = `PHASE 2: Retrying ${p2.total || 0} Skipped Surveys (${p2.attempted || 0}/${p2.total || 0})`;
                    } else if (phase === 'completed') {
                        const p2 = status.phase2 || {};
                        if (p2.total > 0) {
                            phaseBanner.textContent = `COMPLETE — Recovered ${p2.recovered || 0} of ${p2.total || 0} skipped surveys`;
                        } else {
                            phaseBanner.textContent = 'SEARCH COMPLETE — 100% Coverage';
                        }
                    }
                }
                
                if (phase2Stats && status.phase2) {
                    const p2 = status.phase2;
                    if (phase === 'phase2' || (phase === 'completed' && p2.total > 0)) {
                        phase2Stats.style.display = 'block';
                        const p2a = document.getElementById('p2Attempted');
                        const p2r = document.getElementById('p2Recovered');
                        const p2f = document.getElementById('p2Failed');
                        const p2rec = document.getElementById('p2Records');
                        const p2fill = document.getElementById('p2ProgressFill');
                        if (p2a) p2a.textContent = p2.attempted || 0;
                        if (p2r) p2r.textContent = p2.recovered || 0;
                        if (p2f) p2f.textContent = p2.failed || 0;
                        if (p2rec) p2rec.textContent = p2.records_added || 0;
                        if (p2fill && p2.total > 0) {
                            p2fill.style.width = Math.round((p2.attempted / p2.total) * 100) + '%';
                        }
                    } else {
                        phase2Stats.style.display = 'none';
                    }
                }
                
                // Update records tables (real-time)
                if (status.all_records && Array.isArray(status.all_records)) {
                    updateRecordsTable(status.all_records);
                }
                if (status.matches && Array.isArray(status.matches)) {
                    updateMatchesTable(status.matches);
                }
                
                // ═══════════════════════════════════════════════════════════════
                // v11.0: LOG RENDERING
                // ─────────────────────────────────────────────────────────────
                // If we have a session id (active or restored), do INCREMENTAL
                // log fetch from DB (only new rows since last poll). Otherwise
                // fall back to the v10 in-memory snapshot.
                // ═══════════════════════════════════════════════════════════════
                if (status.database && status.database.session_id) {
                    if (v11ActiveSessionId !== status.database.session_id) {
                        // New session detected — reset trackers
                        v11ActiveSessionId = status.database.session_id;
                        v11LastLogId = 0;
                        v11RecordOffset = 0;
                        v11MatchOffset = 0;
                        const c = document.getElementById('logsContainer');
                        if (c) c.innerHTML = '';
                    }
                    v11FetchIncrementalLogs();
                } else if (status.logs && Array.isArray(status.logs)) {
                    // Fallback: pre-DB session, use snapshot
                    const container = document.getElementById('logsContainer');
                    if (container) {
                        container.innerHTML = status.logs.map(log => 
                            `<div class="log-entry">${log}</div>`
                        ).reverse().join('');
                    }
                }
                
                // FIXED: Only stop when BOTH completed AND not running
                // AND require multiple consecutive "not running" states to prevent race conditions
                if (status.completed && !status.running) {
                    notRunningCount++;
                    if (notRunningCount >= STOP_THRESHOLD) {
                        addLog('✅ Search completed!');
                        stopSearch();
                    }
                } else if (status.running) {
                    // Reset counter when search is confirmed running
                    notRunningCount = 0;
                }
                
                // Update pause/resume button state
                if (status.state_management) {
                    const pauseBtn = document.getElementById('pauseBtn');
                    const resumeBtn = document.getElementById('resumeBtn');
                    
                    if (status.state_management.is_paused) {
                        // Search is paused - show resume button
                        if (pauseBtn) pauseBtn.style.display = 'none';
                        if (resumeBtn) resumeBtn.style.display = 'block';
                    } else if (status.running) {
                        // Search is running - show pause button
                        if (pauseBtn) pauseBtn.style.display = 'block';
                        if (resumeBtn) resumeBtn.style.display = 'none';
                    }
                }
                
                // Update heartbeat on every successful poll
                updateHeartbeat();
                
                // ═══════════════════════════════════════════════════════════════════════
                // UPDATE PORTAL HEALTH STATUS in UI
                // ═══════════════════════════════════════════════════════════════════════
                if (status.portal_health) {
                    const ph = status.portal_health;
                    const stateEl = document.getElementById('portalState');
                    const responseEl = document.getElementById('portalResponseTime');
                    const successEl = document.getElementById('portalSuccessRate');
                    const cooldownEl = document.getElementById('portalCooldown');
                    const cooldownTimeEl = document.getElementById('portalCooldownTime');
                    
                    if (stateEl) {
                        stateEl.textContent = ph.current_state || 'UNKNOWN';
                        stateEl.className = 'worker-status ' + 
                            (ph.current_state === 'HEALTHY' ? 'running' : 
                             ph.current_state === 'DEGRADED' ? 'idle' :
                             ph.current_state === 'DOWN' ? 'failed' : 'idle');
                    }
                    
                    if (responseEl) responseEl.textContent = ph.avg_response_time ? ph.avg_response_time + 's' : '--';
                    if (successEl) successEl.textContent = ph.ping_success_rate ? (ph.ping_success_rate * 100).toFixed(0) + '%' : '--';
                    
                    if (cooldownEl && cooldownTimeEl) {
                        if (ph.is_cooling_down && ph.cooldown_seconds_remaining > 0) {
                            cooldownEl.style.display = 'inline';
                            cooldownTimeEl.textContent = ph.cooldown_seconds_remaining + 's';
                        } else {
                            cooldownEl.style.display = 'none';
                        }
                    }
                    
                    // ═══════════════════════════════════════════════════════════════════════
                    // PORTAL HEALTH ALERT BANNER - Show alerts for critical states
                    // ═══════════════════════════════════════════════════════════════════════
                    const alertBanner = document.getElementById('portalAlert');
                    const alertIcon = document.getElementById('alertIcon');
                    const alertTitle = document.getElementById('alertTitle');
                    const alertMessage = document.getElementById('alertMessage');
                    const alertTimer = document.getElementById('alertTimer');
                    const timerLabel = document.getElementById('timerLabel');
                    const timerValue = document.getElementById('timerValue');
                    
                    if (alertBanner && ph.current_state) {
                        if (ph.current_state === 'DOWN') {
                            alertBanner.style.display = 'flex';
                            alertBanner.className = 'portal-alert';
                            alertIcon.textContent = '🔴';
                            alertTitle.textContent = 'Portal Down - Search Paused';
                            alertMessage.textContent = 'The Bhoomi portal is not responding. Search will resume automatically when portal recovers.';
                            if (alertTimer && ph.cooldown_seconds_remaining > 0) {
                                alertTimer.style.display = 'block';
                                timerLabel.textContent = 'Checking again in:';
                                timerValue.textContent = ph.cooldown_seconds_remaining + 's';
                            }
                        } else if (ph.current_state === 'RATE_LIMITED') {
                            alertBanner.style.display = 'flex';
                            alertBanner.className = 'portal-alert warning';
                            alertIcon.textContent = '⚠️';
                            alertTitle.textContent = 'Rate Limited - Throttling';
                            alertMessage.textContent = 'Portal is limiting requests. Workers are operating at reduced speed.';
                            if (alertTimer && ph.cooldown_seconds_remaining > 0) {
                                alertTimer.style.display = 'block';
                                timerLabel.textContent = 'Cooldown:';
                                timerValue.textContent = ph.cooldown_seconds_remaining + 's';
                            }
                        } else if (ph.current_state === 'DEGRADED') {
                            alertBanner.style.display = 'flex';
                            alertBanner.className = 'portal-alert warning';
                            alertIcon.textContent = '🐢';
                            alertTitle.textContent = 'Portal Slow';
                            alertMessage.textContent = 'Portal is responding slowly. Search continues with extended timeouts.';
                            alertTimer.style.display = 'none';
                        } else if (ph.current_state === 'NETWORK_CONGESTION') {
                            alertBanner.style.display = 'flex';
                            alertBanner.className = 'portal-alert warning';
                            alertIcon.textContent = '📶';
                            alertTitle.textContent = 'Network Issues';
                            alertMessage.textContent = 'Intermittent network issues detected. Search continues with retries.';
                            alertTimer.style.display = 'none';
                        } else if (ph.current_state === 'HEALTHY') {
                            // Hide alert on healthy state
                            alertBanner.style.display = 'none';
                        }
                    }
                }
                
            } catch (e) {
                // Log errors instead of silently ignoring them
                console.error('Poll status error:', e);
                // Don't stop polling on errors - let it retry
            }
        }
        
        function addLog(message) {
            const container = document.getElementById('logsContainer');
            const entry = document.createElement('div');
            entry.className = 'log-entry';
            entry.textContent = new Date().toLocaleTimeString() + ' - ' + message;
            container.insertBefore(entry, container.firstChild);
        }
        
        // Tab switching
        let currentTab = 'records';
        
        function switchTab(tab) {
            currentTab = tab;
            
            // Update tab buttons
            document.getElementById('tabRecords').classList.toggle('active', tab === 'records');
            document.getElementById('tabMatches').classList.toggle('active', tab === 'matches');
            
            // Show/hide tables
            document.getElementById('recordsTable').style.display = tab === 'records' ? 'block' : 'none';
            document.getElementById('matchesTable').style.display = tab === 'matches' ? 'block' : 'none';
        }
        
        // Update records table
        function updateRecordsTable(records) {
            const tbody = document.getElementById('recordsBody');
            if (!records || records.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty-row">No records yet. Start a search to see results.</td></tr>';
                return;
            }
            
            // Show last 50 records (most recent first)
            const recentRecords = records.slice(-50).reverse();
            tbody.innerHTML = recentRecords.map(r => `
                <tr>
                    <td>${r.village || ''}</td>
                    <td>${r.survey_no || ''}</td>
                    <td>${r.hissa || ''}</td>
                    <td class="owner-cell kannada">${r.owner_name || ''}</td>
                    <td>${r.extent || ''}</td>
                    <td>W${r.worker_id || 0}</td>
                </tr>
            `).join('');
        }
        
        // Update matches table
        function updateMatchesTable(matches) {
            const tbody = document.getElementById('matchesBody');
            if (!matches || matches.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty-row">No matches found yet.</td></tr>';
                return;
            }
            
            tbody.innerHTML = matches.map(r => `
                <tr class="match-row">
                    <td>${r.village || ''}</td>
                    <td>${r.survey_no || ''}</td>
                    <td>${r.hissa || ''}</td>
                    <td class="owner-cell match kannada">${r.owner_name || ''}</td>
                    <td>${r.extent || ''}</td>
                </tr>
            `).join('');
        }
        
        // Download Modal Functions
        async function showDownloadModal() {
            // Fetch file info
            try {
                const res = await fetch('/api/files/info');
                const info = await res.json();
                
                // Update records card
                document.getElementById('recordsCount').textContent = `${info.all_records.count} records`;
                document.getElementById('recordsPath').textContent = info.all_records.filename || 'No file yet';
                document.getElementById('recordsFilename').value = info.all_records.filename || 'all_records.csv';
                
                // Update matches card
                document.getElementById('matchesCount').textContent = `${info.matches.count} matches`;
                document.getElementById('matchesPath').textContent = info.matches.filename || 'No file yet';
                document.getElementById('matchesFilename').value = info.matches.filename || 'owner_matches.csv';
                
                // Enable/disable download buttons
                const recordsBtn = document.querySelector('#recordsDownloadCard .btn-download');
                const matchesBtn = document.querySelector('#matchesDownloadCard .btn-download');
                
                recordsBtn.disabled = !info.all_records.exists;
                matchesBtn.disabled = !info.matches.exists;
                
            } catch (e) {
                console.error('Error fetching file info:', e);
            }
            
            // Show modal
            document.getElementById('downloadModal').style.display = 'flex';
        }
        
        function hideDownloadModal() {
            document.getElementById('downloadModal').style.display = 'none';
        }
        
        function downloadFile(fileType) {
            let filename;
            if (fileType === 'records') {
                filename = document.getElementById('recordsFilename').value || 'all_records.csv';
            } else {
                filename = document.getElementById('matchesFilename').value || 'owner_matches.csv';
            }
            
            // Ensure .csv extension
            if (!filename.endsWith('.csv')) {
                filename += '.csv';
            }
            
            // Trigger download
            const url = `/api/download/${fileType}?filename=${encodeURIComponent(filename)}`;
            window.location.href = url;
            
            addLog(`📥 Downloaded: ${filename}`);
        }
        
        // Close modal on Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                hideDownloadModal();
            }
        });
    </script>
</body>
</html>
'''

# ═══════════════════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/districts')
def get_districts():
    return jsonify(api.get_districts())

@app.route('/api/taluks/<int:district_code>')
def get_taluks(district_code):
    return jsonify(api.get_taluks(district_code))

@app.route('/api/hoblis/<int:district_code>/<int:taluk_code>')
def get_hoblis(district_code, taluk_code):
    return jsonify(api.get_hoblis(district_code, taluk_code))

@app.route('/api/villages/<int:district_code>/<int:taluk_code>/<int:hobli_code>')
def get_villages(district_code, taluk_code, hobli_code):
    return jsonify(api.get_villages(district_code, taluk_code, hobli_code))

@app.route('/api/search/start', methods=['POST'])
def start_search():
    """
    v11.0: Reuses the singleton coordinator. If a search is already running,
    returns 409 instead of silently overwriting. The coordinator's start_search
    creates a fresh SearchState internally — no stale data leaks.
    """
    data = request.json
    if coordinator.state.running:
        return jsonify({
            'status': 'failed',
            'error': 'A search is already running. Stop it first or wait for completion.'
        }), 409
    
    # Reset transient lists (skeletons, workers) before new search
    coordinator.workers = []
    coordinator.skeletons = {}
    
    success = coordinator.start_search(data)
    return jsonify({'status': 'started' if success else 'failed'})

@app.route('/api/search/status')
def search_status():
    return jsonify(coordinator.get_state())


# ═══════════════════════════════════════════════════════════════════════════════════
# v11.0: SESSION INSPECTION ENDPOINTS — solves "close browser, lose state"
# ───────────────────────────────────────────────────────────────────────────────────
# /api/session/current        - current/last session metadata + total log/record counts
# /api/session/<id>/logs      - paginated logs from DB (incremental via since_id)
# /api/session/<id>/records   - paginated records from DB (offset+limit)
# ═══════════════════════════════════════════════════════════════════════════════════

@app.route('/api/session/current')
def get_current_session():
    """
    Returns metadata for the active or most-recent session. The UI calls this on
    page load to know which session to populate the view with.
    """
    sid = coordinator.current_session_id
    if not sid:
        # No active session — fall back to most recent in DB
        try:
            recent = coordinator.db.get_recent_sessions(limit=1)
            sid = recent[0]['session_id'] if recent else None
        except Exception:
            sid = None
    
    if not sid:
        return jsonify({'session_id': None, 'has_session': False})
    
    try:
        session = coordinator.db.get_session(sid)
        log_count = coordinator.db.count_logs(sid)
        # Quick record count via the same DB
        with coordinator.db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM land_records WHERE session_id = ?', (sid,))
            rec_count = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM land_records WHERE session_id = ? AND is_match = 1', (sid,))
            match_count = cur.fetchone()[0]
        return jsonify({
            'session_id': sid,
            'has_session': True,
            'session': session,
            'log_count': log_count,
            'record_count': rec_count,
            'match_count': match_count,
            'is_active': sid == coordinator.current_session_id and coordinator.state.running,
        })
    except Exception as e:
        return jsonify({'session_id': sid, 'has_session': True, 'error': str(e)[:120]})


@app.route('/api/session/<session_id>/logs')
def get_session_logs(session_id):
    """
    Incremental log fetch. UI calls this with `since_id=<last_seen_log_id>` to
    get only NEW log rows since the last poll. Returns at most `limit` rows.
    Optional `level` filter (INFO/WARN/ERROR/MATCH/PHASE).
    
    Response format:
        {logs: [{id, ts, level, worker_id, village, kind, message}, ...],
         next_since_id: <highest id in this batch, for next poll>,
         has_more: bool}
    """
    try:
        since_id = int(request.args.get('since_id', 0))
        limit = min(int(request.args.get('limit', 500)), 2000)
    except (TypeError, ValueError):
        return jsonify({'error': 'invalid since_id or limit'}), 400
    
    level = request.args.get('level') or None
    
    rows = coordinator.db.get_logs_after(session_id, since_id=since_id, limit=limit, level_filter=level)
    next_since = rows[-1]['id'] if rows else since_id
    has_more = len(rows) >= limit
    
    return jsonify({
        'logs': rows,
        'next_since_id': next_since,
        'count': len(rows),
        'has_more': has_more,
    })


@app.route('/api/session/<session_id>/records')
def get_session_records_paginated(session_id):
    """
    Paginated records from DB. UI uses this to load any chunk of a session's
    records without going through the in-memory snapshot (which is capped at
    Config.RECORDS_BUFFER_COUNT).
    
    Query params:
        offset       - default 0
        limit        - default 200, max 1000
        matches_only - 'true' to filter is_match=1
    
    Response: {records: [...], total: <session total>, offset, limit, matches_only}
    """
    try:
        offset = max(0, int(request.args.get('offset', 0)))
        limit = min(int(request.args.get('limit', 200)), 1000)
    except (TypeError, ValueError):
        return jsonify({'error': 'invalid offset or limit'}), 400
    
    matches_only = request.args.get('matches_only', 'false').lower() == 'true'
    
    records, total = coordinator.db.get_records_paginated(
        session_id, offset=offset, limit=limit, matches_only=matches_only
    )
    return jsonify({
        'records': records,
        'total': total,
        'offset': offset,
        'limit': limit,
        'matches_only': matches_only,
        'has_more': (offset + len(records)) < total,
    })

@app.route('/api/search/stop', methods=['POST'])
def stop_search():
    coordinator.stop_search()
    return jsonify({'status': 'stopped'})

@app.route('/api/download/<file_type>')
def download_csv(file_type):
    """
    Download CSV file. file_type:
      - 'records'         : live all-records CSV (writer file)
      - 'matches'         : live matches CSV (writer file)
      - 'records_phase1'  : Phase 1 snapshot of all records (DB-exported, immune to Phase 2)
      - 'matches_phase1'  : Phase 1 snapshot of matches (DB-exported, immune to Phase 2)
    """
    from flask import send_file, request
    
    # v9.0 FIX: Read file path directly from state to avoid get_state() lock timeout
    # during Phase 2. Falls back to get_state() if direct access fails.
    filepath = ''
    try:
        if file_type == 'records':
            filepath = getattr(coordinator.state, 'all_records_file', '')
            default_name = 'all_records.csv'
        elif file_type == 'matches':
            filepath = getattr(coordinator.state, 'matches_file', '')
            default_name = 'owner_matches.csv'
        elif file_type == 'records_phase1':
            filepath = getattr(coordinator.state, 'phase1_records_file', '')
            default_name = 'all_records_phase1.csv'
        elif file_type == 'matches_phase1':
            filepath = getattr(coordinator.state, 'phase1_matches_file', '')
            default_name = 'owner_matches_phase1.csv'
        else:
            return jsonify({'error': 'Invalid file type'}), 400
    except Exception:
        state = coordinator.get_state()
        if file_type == 'records':
            filepath = state.get('all_records_file', '')
            default_name = 'all_records.csv'
        else:
            filepath = state.get('matches_file', '')
            default_name = 'owner_matches.csv'
    
    # Flush writer before serving to ensure all data is on disk
    try:
        if file_type == 'records' and coordinator.all_records_writer:
            coordinator.all_records_writer.flush()
        elif file_type == 'matches' and coordinator.matches_writer:
            coordinator.matches_writer.flush()
    except Exception:
        pass
    
    if not filepath or not os.path.exists(filepath):
        return jsonify({'error': 'File not found. Run a search first.'}), 404
    
    # Get custom filename from query param or use default
    custom_name = request.args.get('filename', default_name)
    if not custom_name.endswith('.csv'):
        custom_name += '.csv'
    
    return send_file(
        filepath,
        mimetype='text/csv',
        as_attachment=True,
        download_name=custom_name
    )

@app.route('/api/files/info')
def get_files_info():
    """Get info about saved CSV files"""
    state = coordinator.get_state()
    
    all_records_file = state.get('all_records_file', '')
    matches_file = state.get('matches_file', '')
    
    result = {
        'all_records': {
            'exists': os.path.exists(all_records_file) if all_records_file else False,
            'filename': os.path.basename(all_records_file) if all_records_file else '',
            'filepath': all_records_file,
            'count': state.get('total_records', 0)
        },
        'matches': {
            'exists': os.path.exists(matches_file) if matches_file else False,
            'filename': os.path.basename(matches_file) if matches_file else '',
            'filepath': matches_file,
            'count': state.get('total_matches', 0)
        }
    }
    
    # Get file sizes if they exist
    if result['all_records']['exists']:
        result['all_records']['size'] = os.path.getsize(all_records_file)
    if result['matches']['exists']:
        result['matches']['size'] = os.path.getsize(matches_file)
    
    return jsonify(result)

# ═══════════════════════════════════════════════════════════════════════════════════════
# PORTAL HEALTH API - Real-time portal status
# ═══════════════════════════════════════════════════════════════════════════════════════

@app.route('/api/portal/health')
def get_portal_health():
    """Get current portal health status"""
    return jsonify(portal_health.get_stats())

@app.route('/api/search/pause', methods=['POST'])
def pause_search():
    """Manually pause search"""
    if coordinator.state_manager:
        coordinator.state_manager.pause_search("Manual pause requested by user")
        return jsonify({'status': 'paused'})
    return jsonify({'error': 'No active search to pause'}), 400

@app.route('/api/search/resume', methods=['POST'])
def resume_search():
    """Manually resume paused search"""
    if coordinator.state_manager:
        success = coordinator.state_manager.resume_search()
        return jsonify({'status': 'resumed' if success else 'failed'})
    return jsonify({'error': 'No paused search to resume'}), 400


# ═══════════════════════════════════════════════════════════════════════════════════════
# DATABASE API ENDPOINTS - Search History & Resume
# ═══════════════════════════════════════════════════════════════════════════════════════

@app.route('/api/db/info')
def get_database_info():
    """Get database information and statistics"""
    db = get_database()
    return jsonify({
        'db_path': db.db_path,
        'db_folder': db.db_folder,
        'total_records': db.get_all_records_count(),
        'exists': os.path.exists(db.db_path),
        'size_mb': round(os.path.getsize(db.db_path) / (1024 * 1024), 2) if os.path.exists(db.db_path) else 0
    })

@app.route('/api/db/sessions')
def get_search_sessions():
    """Get recent search sessions"""
    db = get_database()
    limit = request.args.get('limit', 20, type=int)
    sessions = db.get_recent_sessions(limit)
    return jsonify(sessions)

@app.route('/api/db/sessions/<session_id>')
def get_session_details(session_id):
    """Get details for a specific session"""
    db = get_database()
    session = db.get_session(session_id)
    if not session:
        return jsonify({'error': 'Session not found'}), 404
    
    stats = db.get_session_stats(session_id)
    session.update(stats)
    return jsonify(session)

@app.route('/api/db/sessions/<session_id>/records')
def get_session_records(session_id):
    """Get records for a session"""
    db = get_database()
    limit = request.args.get('limit', 100, type=int)
    matches_only = request.args.get('matches_only', 'false').lower() == 'true'
    
    records = db.get_session_records(session_id, limit=limit, matches_only=matches_only)
    return jsonify({
        'session_id': session_id,
        'count': len(records),
        'records': records
    })

@app.route('/api/db/sessions/<session_id>/export')
def export_session_to_csv(session_id):
    """Export session records to CSV"""
    from flask import send_file
    
    db = get_database()
    matches_only = request.args.get('matches_only', 'false').lower() == 'true'
    
    # Create export filename
    session = db.get_session(session_id)
    if not session:
        return jsonify({'error': 'Session not found'}), 404
    
    suffix = '_matches' if matches_only else '_all'
    filename = f"bhoomi_export_{session_id}{suffix}.csv"
    filepath = os.path.join(db.db_folder, filename)
    
    db.export_to_csv(session_id, filepath, matches_only=matches_only)
    
    if not os.path.exists(filepath):
        return jsonify({'error': 'No records to export'}), 404
    
    return send_file(
        filepath,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

@app.route('/api/db/search')
def search_database():
    """Search all records by owner name"""
    db = get_database()
    owner_name = request.args.get('q', '')
    limit = request.args.get('limit', 100, type=int)
    
    if not owner_name:
        return jsonify({'error': 'Query parameter "q" is required'}), 400
    
    records = db.search_records(owner_name, limit=limit)
    return jsonify({
        'query': owner_name,
        'count': len(records),
        'records': records
    })

@app.route('/api/db/resumable')
def get_resumable_sessions():
    """Get sessions that can be resumed"""
    db = get_database()
    sessions = db.get_resumable_sessions()
    return jsonify(sessions)

# ═══════════════════════════════════════════════════════════════════════════════════════
# SKIPPED SURVEYS API - For retry capability and reporting
# ═══════════════════════════════════════════════════════════════════════════════════════

@app.route('/api/db/sessions/<session_id>/skipped')
def get_session_skipped_surveys(session_id):
    """Get all skipped surveys for a session"""
    db = get_database()
    skipped = db.get_skipped_items(session_id)
    return jsonify({
        'session_id': session_id,
        'count': len(skipped),
        'skipped_surveys': skipped
    })

@app.route('/api/db/sessions/<session_id>/skipped/export')
def export_skipped_surveys_csv(session_id):
    """Export skipped surveys to CSV for later retry"""
    from flask import send_file
    import csv
    
    db = get_database()
    skipped = db.get_skipped_items(session_id)
    
    if not skipped:
        return jsonify({'error': 'No skipped surveys found for this session'}), 404
    
    # Create CSV file
    filename = f"skipped_surveys_{session_id}.csv"
    filepath = os.path.join(db.db_folder, filename)
    
    fieldnames = ['village_name', 'survey_no', 'surnoc', 'hissa', 'period', 'error_message', 'created_at', 'status']
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(skipped)
    
    logger.info(f"📁 Exported {len(skipped)} skipped surveys to {filepath}")
    
    return send_file(
        filepath,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

@app.route('/api/skipped/current')
def get_current_skipped_surveys():
    """Get skipped surveys from current running/completed search.

    v12: count comes from the DB (full total), not the in-memory list which the UI
    snapshot truncates to the last 20 rows.
    """
    state = coordinator.get_state()
    session_id = state.get('database', {}).get('session_id')
    count = state.get('accuracy_metrics', {}).get('skipped_surveys_count', 0)
    preview = state.get('accuracy_metrics', {}).get('skipped_surveys', [])
    try:
        if session_id and coordinator.db:
            count = coordinator.db.get_skipped_count(session_id)
    except Exception:
        pass
    return jsonify({
        'count': count,
        'skipped_surveys': preview,  # last-N preview only; use /export for the full list
        'session_id': session_id
    })

@app.route('/api/skipped/current/export')
def export_current_skipped_csv():
    """Export current search's skipped surveys to CSV.

    v12: exports the FULL skipped_items list from the DB (v11 exported only the last 20
    rows that survived the in-memory snapshot truncation).
    """
    from flask import send_file
    import csv

    session_id = coordinator.current_session_id
    skipped = []
    try:
        if session_id and coordinator.db:
            skipped = coordinator.db.get_skipped_items(session_id)
    except Exception as e:
        logger.error(f"skipped export: DB read failed: {e}")

    if not skipped:
        return jsonify({'error': 'No skipped surveys in current search'}), 404

    # Create CSV in Downloads folder
    downloads = os.path.join(os.path.expanduser('~'), 'Downloads')
    filename = f"skipped_surveys_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(downloads, filename)

    fieldnames = ['village', 'village_name', 'village_code', 'survey_no', 'surnoc',
                  'hissa', 'period', 'reason', 'error_message', 'status', 'created_at', 'timestamp']

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(skipped)
    
    logger.info(f"📁 Exported {len(skipped)} skipped surveys to {filepath}")
    
    return send_file(
        filepath,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

# ═══════════════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════════════


def repair_import_csv(csv_path: str, session_id: str) -> dict:
    """v12.2 (audit C1 repair): re-import a live all_records CSV into a session.

    The pre-v12.2 UNIQUE key silently dropped same-owner multi-extent rows from
    the DB; the live writer CSV kept every extracted row. This re-imports the
    CSV with the new identity (extent + owner_seq): rows already present dedup
    via INSERT OR IGNORE, previously-dropped rows are restored. owner_seq is
    assigned per occurrence within each (parcel, owner, extent) group, and
    is_match is recomputed from the session's stored owner_variants.
    """
    import collections
    db = get_database()
    sess = db.get_session(session_id)
    if not sess:
        return {'error': f'session not found: {session_id}'}
    try:
        variants = json.loads(sess.get('owner_variants') or '[]')
        if isinstance(variants, str):
            variants = json.loads(variants)
    except Exception:
        variants = [sess.get('owner_name', '')]

    # Build all rows first (seq assignment per identical-key occurrence), then
    # import in ONE transaction — per-row round-trips would take minutes on an
    # 80k-row CSV and hold the write lock far longer than necessary.
    seen_seq = collections.Counter()
    rows_out = []
    with open(csv_path, encoding='utf-8') as fh:
        for row in csv.DictReader(fh):
            key = (row.get('village',''), row.get('survey_no',''), row.get('surnoc',''),
                   row.get('hissa',''), row.get('period',''), row.get('owner_name',''),
                   row.get('extent',''))
            seq = row.get('owner_seq')
            if seq in (None, ''):
                seq = seen_seq[key]
            seen_seq[key] += 1
            owner = row.get('owner_name','')
            is_match = any(_norm_for_match(v) in _norm_for_match(owner) for v in variants if v)
            rows_out.append((
                session_id, row.get('district',''), row.get('taluk',''), row.get('hobli',''),
                row.get('village',''), row.get('survey_no',0), row.get('surnoc',''),
                row.get('hissa',''), row.get('period',''), owner, row.get('extent',''),
                int(seq), 1 if is_match else 0, row.get('worker_id',0) or 0,
            ))
    with db.lock:
        with db.get_connection() as conn:
            before = conn.execute("SELECT COUNT(*) FROM land_records WHERE session_id=?",
                                  (session_id,)).fetchone()[0]
            conn.executemany('''
                INSERT OR IGNORE INTO land_records (
                    session_id, district, taluk, hobli, village, survey_no, surnoc,
                    hissa, period, owner_name, extent, owner_seq, is_match, worker_id
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', rows_out)
            after = conn.execute("SELECT COUNT(*) FROM land_records WHERE session_id=?",
                                 (session_id,)).fetchone()[0]
    result = {'csv_rows': len(rows_out), 'newly_imported': after - before, 'session': session_id}
    logger.info(f"repair_import_csv: {result}")
    return result


if __name__ == '__main__':
    # v12.2: repair mode — restore rows the old UNIQUE key dropped, from a live CSV.
    #   ./venv/bin/python bhoomi_playwright_v12.py --repair-import <all_records.csv> <session_id>
    if '--repair-import' in sys.argv:
        _i = sys.argv.index('--repair-import')
        _csv, _sid = sys.argv[_i+1], sys.argv[_i+2]
        print(json.dumps(repair_import_csv(_csv, _sid), indent=2))
        sys.exit(0)

    print("""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║       POWER-BHOOMI v12.0 - COMPLETION-HARDENED EDITION                               ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  🏗️  ARCHITECTURE:                                                                    ║
║   • Service154 skeleton API + Service2 owner extraction (hybrid)                     ║
║   • 24 Parallel Browser Workers (no captcha, no auth)                                ║
║   • v12: STALL WATCHDOG — wedged worker no longer hangs the search                   ║
║   • v12: GRACEFUL STOP — drains workers, exports all CSVs, resets coordinator        ║
║   • v12: Coverage pass + DB-backed Phase 2 + skeleton-verified smart-stop            ║
║   • v12: Hobli-qualified checkpoints (no cross-hobli survey skipping)                ║
║   • Stale 'running' sessions auto-marked 'interrupted' on server restart             ║
║                                                                                       ║
║  🌐 OPEN YOUR BROWSER:                                                               ║
║       http://localhost:5001                                                          ║
║                                                                                      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
    """)
    # v11.0: Auto-open browser. Spawned in a daemon thread so Flask starts first;
    # the 2s sleep gives the server time to bind to the port before we navigate.
    # Disabled when running inside Flask's reloader child process (avoids double-opening).
    if not os.environ.get('WERKZEUG_RUN_MAIN'):
        def _open_browser():
            import webbrowser, time as _t
            _t.sleep(2.0)
            try:
                webbrowser.open(f'http://localhost:{Config.PORT}', new=2)
                logger.info(f"🌐 Opening http://localhost:{Config.PORT} in default browser")
            except Exception as e:
                logger.debug(f"Browser auto-open failed (run manually): {e}")
        threading.Thread(target=_open_browser, daemon=True).start()
    
    # IMPORTANT: use_reloader=False prevents server restart when code changes mid-search
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG, threaded=True, use_reloader=False)

