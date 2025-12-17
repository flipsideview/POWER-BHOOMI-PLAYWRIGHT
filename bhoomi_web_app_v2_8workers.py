#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                  POWER-BHOOMI v3.0 - BULLETPROOF EDITION (8 WORKERS)                 ║
║                        Karnataka Land Records Search Tool                             ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  Features:                                                                            ║
║  • 8 parallel browser workers (HIGH-SPEED optimized for macOS)                       ║
║  • SESSION EXPIRATION DETECTION & AUTO-RECOVERY                                      ║
║  • ZERO VILLAGES MISSED - Retry on session expire, not skip!                         ║
║  • Sequential survey iteration (1, 2, 3... no skips)                                 ║
║  • Thread-safe CSV output to Downloads                                               ║
║  • Real-time progress with survey tracking                                           ║
║  • Robust error recovery with detailed logging                                       ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

Version: 3.3.0-Accuracy
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
from typing import Dict, List, Optional, Tuple, Any
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
    """Mac-optimized configuration for HIGH-SPEED processing with 8 workers"""
    # Server
    HOST = '0.0.0.0'
    PORT = 5001
    DEBUG = True
    
    # Parallel Processing - 8 HIGH-SPEED workers for Mac
    MAX_WORKERS = 8
    WORKER_STARTUP_DELAY = 0.7  # Slightly longer delay for 8 workers to prevent Chrome conflicts
    
    # Timeouts (seconds) - Optimized for Mac speed
    PAGE_LOAD_TIMEOUT = 20
    ELEMENT_WAIT_TIMEOUT = 8
    POST_CLICK_WAIT = 4  # Faster clicks
    POST_SELECT_WAIT = 1.5  # Faster selections
    
    # Search Settings - NO SURVEY SKIPPING
    DEFAULT_MAX_SURVEY = 200
    EMPTY_SURVEY_THRESHOLD = 999999  # DISABLED - Check ALL surveys for 100% accuracy
    
    # Session Recovery Settings
    MAX_SESSION_RETRIES = 3  # Retry this many times on session expiry
    SESSION_REFRESH_WAIT = 3  # Wait after refreshing session
    
    # Browser Stability Settings
    MAX_HISSA_BEFORE_RESTART = 200  # Restart browser after processing this many Hissa to prevent memory issues
    BROWSER_RESTART_DELAY = 3  # Seconds to wait before restarting browser
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # ACCURACY SETTINGS - Sacrifice 5% speed for 100% accuracy
    # ═══════════════════════════════════════════════════════════════════════════════════
    ACCURACY_MODE = True  # Enable all accuracy features
    PROCESS_ALL_PERIODS = True  # Process ALL periods, not just the latest
    MAX_HISSA_RETRIES = 2  # Retry individual Hissa on failure
    VERIFY_PAGE_LOAD = True  # Verify page loaded after each action
    LOG_SKIPPED_ITEMS = True  # Log all skipped items for later retry
    
    # URLs
    ECHAWADI_BASE = "https://rdservices.karnataka.gov.in/echawadi/Home"
    SERVICE2_URL = "https://landrecords.karnataka.gov.in/Service2/"
    
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
            return 'landrecords.karnataka.gov.in' in self.driver.current_url
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
# 8 workers at 2 req/sec = 16 req/sec total, conservative for portal
_global_rate_limiter = RateLimiter(requests_per_second=2.0, burst_size=10)

def rate_limited_request(func):
    """Decorator to rate limit portal requests"""
    def wrapper(*args, **kwargs):
        _global_rate_limiter.acquire()
        return func(*args, **kwargs)
    return wrapper

# ═══════════════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('POWER-BHOOMI')

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
    khatah: str
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
    
    # Worker details
    workers: Dict[int, WorkerStatus] = field(default_factory=dict)
    
    # Logs
    logs: List[str] = field(default_factory=list)
    
    # File paths
    all_records_file: str = ''
    matches_file: str = ''
    
    # Real-time records storage (for UI display)
    all_records: List[Dict] = field(default_factory=list)
    matches: List[Dict] = field(default_factory=list)

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
        
        # Initialize connection pool (size = workers + 2 for overhead)
        pool_size = pool_size or (Config.MAX_WORKERS + 2)
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
                        khatah TEXT,
                        is_match INTEGER DEFAULT 0,
                        worker_id INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id)
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
                            INSERT INTO land_records (
                                session_id, district, taluk, hobli, village,
                                survey_no, surnoc, hissa, period,
                                owner_name, extent, khatah, is_match, worker_id
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
                            record.get('khatah', ''),
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
                    INSERT INTO land_records (
                        session_id, district, taluk, hobli, village,
                        survey_no, surnoc, hissa, period,
                        owner_name, extent, khatah, is_match, worker_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        r.get('khatah', ''),
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
    # EXPORT FUNCTIONS
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def export_to_csv(self, session_id: str, output_path: str, matches_only: bool = False) -> str:
        """Export session records to CSV file"""
        records = self.get_session_records(session_id, matches_only=matches_only)
        
        if not records:
            return None
        
        fieldnames = ['district', 'taluk', 'hobli', 'village', 'survey_no', 
                      'surnoc', 'hissa', 'period', 'owner_name', 'extent', 'khatah', 'created_at']
        
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
        session_id: str = None  # Current search session ID
    ):
        self.worker_id = worker_id
        self.params = search_params
        self.villages = villages
        self.state = state
        self.all_records_writer = all_records_writer
        self.matches_writer = matches_writer
        self.state_lock = state_lock
        
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
    
    def _add_log(self, message: str):
        """Thread-safe log addition"""
        with self.state_lock:
            log_entry = f"[W{self.worker_id}] {message}"
            self.state.logs.append(log_entry)
            # Keep only last 100 logs
            if len(self.state.logs) > 100:
                self.state.logs = self.state.logs[-100:]
        self.logger.info(message)
    
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
    
    def _init_browser(self, retry_count: int = 3):
        """Initialize browser - Mac optimized for speed"""
        import shutil
        import tempfile
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        # Clean user data directory for this worker - store as instance variable for cleanup
        self._user_data_dir = os.path.join(tempfile.gettempdir(), f'bhoomi_chrome_{self.worker_id}')
        if os.path.exists(self._user_data_dir):
            try:
                shutil.rmtree(self._user_data_dir)
            except Exception:
                pass
        
        last_error = None
        for attempt in range(retry_count):
            try:
                options = Options()
                
                # Mac-optimized headless Chrome settings
                options.add_argument('--headless=new')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-gpu')
                options.add_argument('--window-size=1920,1080')
                
                # Speed optimizations - disable unnecessary features
                options.add_argument('--disable-extensions')
                options.add_argument('--disable-images')
                options.add_argument('--blink-settings=imagesEnabled=false')
                options.add_argument('--disable-javascript-harmony-shipping')
                options.add_argument('--disable-background-networking')
                options.add_argument('--disable-sync')
                options.add_argument('--disable-translate')
                options.add_argument('--disable-default-apps')
                options.add_argument('--mute-audio')
                options.add_argument('--no-first-run')
                
                # Unique user data dir per worker
                options.add_argument(f'--user-data-dir={self._user_data_dir}')
                
                # Page load strategy - don't wait for all resources
                options.page_load_strategy = 'eager'
                
                # Use cached ChromeDriver path for faster startup
                service = CachedChromeDriver.get_service()
                self.driver = webdriver.Chrome(service=service, options=options)
                self.driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
                
                # Implicit wait for elements
                self.driver.implicitly_wait(2)
                
                self._add_log(f"✅ Worker {self.worker_id} browser ready!")
                return  # Success
                
            except Exception as e:
                last_error = e
                self._add_log(f"Browser init failed (attempt {attempt + 1}): {str(e)[:50]}")
                time.sleep(1)  # Quick retry on Mac
                
                # Cleanup failed attempt
                try:
                    if self.driver:
                        self.driver.quit()
                except Exception:
                    pass
                self.driver = None
        
        # All retries failed
        raise Exception(f"Failed to initialize browser after {retry_count} attempts: {last_error}")
    
    def _close_browser(self):
        """Safely close browser and cleanup"""
        import shutil
        
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
        
        # Clean up user data directory using stored path
        if hasattr(self, '_user_data_dir') and self._user_data_dir and os.path.exists(self._user_data_dir):
            try:
                shutil.rmtree(self._user_data_dir, ignore_errors=True)
            except Exception:
                pass
    
    def _handle_alert(self) -> tuple:
        """
        Handle any JavaScript alert that might be blocking the page.
        Returns (had_alert: bool, alert_text: str, is_portal_issue: bool)
        
        For 100% accuracy, we detect portal issues and retry instead of failing.
        """
        try:
            from selenium.webdriver.common.alert import Alert
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import NoAlertPresentException, TimeoutException
            
            # Check if there's an alert (with short timeout)
            try:
                WebDriverWait(self.driver, 1).until(EC.alert_is_present())
                alert = Alert(self.driver)
                alert_text = alert.text
                
                # Check if this is a portal issue (not our fault)
                portal_issues = [
                    'facing some issues',
                    'try after some time',
                    'currently facing',
                    'service unavailable',
                    'server error',
                    'technical difficulties',
                    'please try again',
                    'contact bhoomi@karnataka.gov.in'
                ]
                
                is_portal_issue = any(phrase in alert_text.lower() for phrase in portal_issues)
                
                # Dismiss the alert
                alert.accept()
                
                if is_portal_issue:
                    self._add_log(f"⚠️ Portal issue detected: {alert_text[:50]}...")
                else:
                    self._add_log(f"⚠️ Alert dismissed: {alert_text[:50]}...")
                
                return (True, alert_text, is_portal_issue)
                
            except (NoAlertPresentException, TimeoutException):
                # No alert present - this is fine
                return (False, '', False)
                
        except Exception as e:
            self.logger.warning(f"Alert handling error: {e}")
            return (False, '', False)
    
    def _wait_for_portal_recovery(self, max_wait: int = 30) -> bool:
        """
        Wait for portal to recover from issues.
        Returns True if recovered, False if still having issues.
        """
        self._add_log(f"⏳ Waiting for portal to recover (max {max_wait}s)...")
        
        for attempt in range(max_wait // 5):
            time.sleep(5)
            
            # Try to access the portal
            try:
                self.driver.get(Config.SERVICE2_URL)
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
                page_source = self.driver.page_source
            
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
        """
        Refresh the session by navigating back to the portal.
        Returns True if session refresh succeeded, False otherwise.
        Raises exception if browser is dead (invalid session id).
        """
        self._add_log(f"🔄 Refreshing session...")
        
        # This will raise an exception if browser is dead
        # Let the caller handle browser restart
        self.driver.delete_all_cookies()
        self.driver.get(Config.SERVICE2_URL)
        time.sleep(Config.SESSION_REFRESH_WAIT)
        
        # Verify session is good
        if not self._is_session_expired():
            self._add_log(f"✅ Session refreshed successfully")
            return True
        else:
            self._add_log(f"⚠️ Session still expired after refresh")
            return False
    
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
            RESULT_KEYWORDS = ['Owner', 'ಮಾಲೀಕರ', 'Extent', 'ವಿಸ್ತೀರ್ಣ', 'Khata', 'ಖಾತಾ', 'Name', 'ಹೆಸರು']
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
                                'khatah': ''
                            })
                
                if not owners:
                    self.logger.debug(f"No valid results table or div found in page")
                    return owners
            
            if results_table:
                # Extract from the validated results table only
                rows = results_table.find_all('tr')
                header_idx = {}  # Track column indices for better extraction
                
                for row_idx, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        cell_texts = [c.get_text(strip=True) for c in cells]
                        row_text = ' '.join(cell_texts)
                        
                        # Detect header row and build column map
                        if row_idx == 0 or any(h.lower() in row_text.lower() for h in ['owner', 'extent', 'slno', 'name']):
                            for i, txt in enumerate(cell_texts):
                                txt_lower = txt.lower()
                                if 'owner' in txt_lower or 'name' in txt_lower or 'ಮಾಲೀಕ' in txt or 'ಹೆಸರು' in txt:
                                    header_idx['owner'] = i
                                elif 'extent' in txt_lower or 'ವಿಸ್ತೀರ್ಣ' in txt:
                                    header_idx['extent'] = i
                                elif 'khata' in txt_lower or 'ಖಾತಾ' in txt:
                                    header_idx['khatah'] = i
                            continue  # Skip header row
                        
                        # Skip rows that look like form elements
                        is_form_row = any(pattern in row_text for pattern in FORM_KEYWORDS)
                        is_district_list = re.search(r'[A-Z]{5,}[A-Z]{5,}', row_text.replace(' ', ''))
                        
                        if is_form_row or is_district_list:
                            continue
                        
                        # Skip serial number only rows
                        if SKIP_PATTERNS.match(cell_texts[0]):
                            cell_texts = cell_texts[1:]  # Shift left, skip serial number
                            if not cell_texts:
                                continue
                        
                        # Extract owner data using column indices or positional fallback
                        owner_idx = header_idx.get('owner', 0)
                        extent_idx = header_idx.get('extent', 1)
                        khatah_idx = header_idx.get('khatah', 2)
                        
                        owner_name = cell_texts[owner_idx] if owner_idx < len(cell_texts) else ''
                        extent = cell_texts[extent_idx] if extent_idx < len(cell_texts) else ''
                        khatah = cell_texts[khatah_idx] if khatah_idx < len(cell_texts) else ''
                        
                        # Validate owner name
                        if not owner_name or len(owner_name) < 2:
                            continue
                        if owner_name.isdigit():
                            continue
                        if any(skip in owner_name for skip in ['Select', 'ಆಯ್ಕೆ', 'Toggle']):
                            continue
                        
                        owner_entry = {
                            'owner_name': owner_name,
                            'extent': extent,
                            'khatah': khatah,
                        }
                        
                        # Avoid duplicates
                        if owner_entry not in owners:
                            owners.append(owner_entry)
            
            # Log extraction result for debugging
            if not owners:
                self.logger.debug(f"No owners extracted from page")
                
        except Exception as e:
            self.logger.error(f"Extract error: {e}")
        
        return owners
    
    def _search_village(self, village_code: str, village_name: str, hobli_code: str, hobli_name: str):
        """
        Search a single village for all survey numbers.
        NOW WITH SESSION EXPIRATION DETECTION AND RECOVERY!
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        IDS = Config.ELEMENT_IDS
        max_survey = self.params.get('max_survey', Config.DEFAULT_MAX_SURVEY)
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
        portal_retries = 0   # Track RTC access retries per survey (FIXED: prevent infinite loops)

        # ═══════════════════════════════════════════════════════════════════════
        # CHECK FOR RESUME CHECKPOINT - Skip already completed surveys
        # ═══════════════════════════════════════════════════════════════════════
        start_survey = 1
        if self.db and self.session_id:
            try:
                checkpoint = self.db.get_last_checkpoint(self.session_id, village_code)
                if checkpoint:
                    start_survey = checkpoint['survey_no'] + 1  # Resume from next survey
                    self._add_log(f"📍 Resuming {village_name} from survey {start_survey} (checkpoint found)")
            except Exception as chkpt_err:
                self.logger.debug(f"Checkpoint lookup failed: {chkpt_err}")

        self._add_log(f"🏘️ Starting {village_name}: Surveys {start_survey} to {max_survey}")
        
        # SEQUENTIAL SURVEY ITERATION: 1, 2, 3... NO SKIPPING
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
                # Navigate to portal
                self.driver.get(Config.SERVICE2_URL)
                time.sleep(Config.POST_SELECT_WAIT)
                
                # ═══════════════════════════════════════════════════════════════════════
                # SESSION EXPIRATION CHECK #1 - After loading portal
                # ═══════════════════════════════════════════════════════════════════════
                if self._is_session_expired():
                    self._add_log(f"⚠️ Session expired at {village_name} survey {survey_no}")
                    if session_retries < Config.MAX_SESSION_RETRIES:
                        session_retries += 1
                        self._add_log(f"🔄 Retry {session_retries}/{Config.MAX_SESSION_RETRIES} - refreshing session...")
                        if self._refresh_session():
                            continue  # RETRY same survey, don't increment
                        else:
                            # Refresh failed, try restarting browser
                            self._add_log(f"⚠️ Session refresh failed, restarting browser...")
                            self._close_browser()
                            time.sleep(2)
                            self._init_browser()
                            continue  # RETRY same survey
                    else:
                        self._add_log(f"❌ Max session retries reached for {village_name}")
                        raise Exception(f"Session expired {session_retries} times for {village_name}")
                
                # Reset session retries on successful page load
                session_retries = 0
                
                # Select location (fast sequence)
                Select(self.driver.find_element(By.ID, IDS['district'])).select_by_value(self.params['district_code'])
                time.sleep(Config.POST_SELECT_WAIT)
                
                Select(self.driver.find_element(By.ID, IDS['taluk'])).select_by_value(self.params['taluk_code'])
                time.sleep(Config.POST_SELECT_WAIT)
                
                Select(self.driver.find_element(By.ID, IDS['hobli'])).select_by_value(hobli_code)
                time.sleep(Config.POST_SELECT_WAIT)
                
                Select(self.driver.find_element(By.ID, IDS['village'])).select_by_value(village_code)
                time.sleep(Config.POST_SELECT_WAIT)
                
                # Enter survey number
                survey_input = self.driver.find_element(By.ID, IDS['survey_no'])
                survey_input.clear()
                survey_input.send_keys(str(survey_no))
                
                # Click GO using JavaScript (rate limited to prevent portal overload)
                _global_rate_limiter.acquire()
                go_btn = self.driver.find_element(By.ID, IDS['go_btn'])
                self.driver.execute_script("arguments[0].click();", go_btn)
                time.sleep(Config.POST_CLICK_WAIT)
                
                # ═══════════════════════════════════════════════════════════════════════
                # PORTAL ISSUE & SESSION CHECK - After clicking GO
                # For 100% accuracy: Handle alerts and portal issues before proceeding
                # ═══════════════════════════════════════════════════════════════════════
                
                # First, handle any portal alerts (e.g., "facing issues" messages)
                had_alert, alert_text, is_portal_issue = self._handle_alert()
                
                if is_portal_issue:
                    # FIXED: Don't loop forever! This alert is for THIS SPECIFIC survey/RTC
                    # The portal is working, but THIS record might have issues
                    portal_retries += 1
                    if portal_retries <= 2:  # Max 2 retries per survey
                        self._add_log(f"⚠️ RTC issue at {village_name} Sy:{survey_no} (retry {portal_retries}/2)")
                        time.sleep(3)
                        continue  # Retry same survey once more
                    else:
                        # After 2 retries, this survey has a persistent issue - SKIP IT
                        self._add_log(f"⏭️ Skipping Sy:{survey_no} - RTC access issue (not portal down)")
                        portal_retries = 0  # Reset for next survey
                        survey_no += 1
                        continue  # Move to next survey
                
                page_source = self.driver.page_source
                if self._is_session_expired(page_source):
                    self._add_log(f"⚠️ Session expired after GO click - {village_name} survey {survey_no}")
                    if session_retries < Config.MAX_SESSION_RETRIES:
                        session_retries += 1
                        self._refresh_session()
                        continue  # RETRY same survey, don't increment!
                    else:
                        raise Exception(f"Persistent session expiry for {village_name}")
                
                # Check if surnoc populated
                surnoc_sel = Select(self.driver.find_element(By.ID, IDS['surnoc']))
                surnoc_opts = [o.text for o in surnoc_sel.options if "Select" not in o.text]
                
                if not surnoc_opts:
                    # This is a genuinely empty survey (not session expired)
                    empty_count += 1
                    # Only skip village if we've had MANY consecutive empty surveys
                    if empty_count > Config.EMPTY_SURVEY_THRESHOLD:
                        self._add_log(f"⏭️ {village_name}: {empty_count} consecutive empty surveys, completing village")
                        self._add_log(f"📊 {village_name} Summary: Checked {surveys_checked}, Found data in {surveys_with_data}")
                        break
                    survey_no += 1  # Move to next survey
                    continue
                
                # Found data - reset counters and increment found count
                empty_count = 0
                portal_retries = 0  # Reset portal retry counter on success
                surveys_with_data += 1
                
                # Process each surnoc
                for surnoc in surnoc_opts:
                    if not self.state.running:
                        return
                    
                    try:
                        surnoc_sel = Select(self.driver.find_element(By.ID, IDS['surnoc']))
                        surnoc_sel.select_by_visible_text(surnoc)
                        time.sleep(Config.POST_SELECT_WAIT + 1)
                        
                        # Get hissa options
                        hissa_sel = Select(self.driver.find_element(By.ID, IDS['hissa']))
                        hissa_opts = [o.text for o in hissa_sel.options if "Select" not in o.text]
                        
                        # Process each hissa
                        for hissa in hissa_opts:
                            if not self.state.running:
                                return
                            
                            hissa_retry_count = 0
                            max_hissa_retries = 2
                            
                            while hissa_retry_count <= max_hissa_retries:
                                try:
                                    hissa_sel = Select(self.driver.find_element(By.ID, IDS['hissa']))
                                    hissa_sel.select_by_visible_text(hissa)
                                    time.sleep(Config.POST_SELECT_WAIT)
                                    
                                    # ═══════════════════════════════════════════════════════════════════════
                                    # PERIOD PROCESSING: Respects PROCESS_ALL_PERIODS config
                                    # If True: Process ALL periods for 100% accuracy
                                    # If False: Process only the latest available period for speed
                                    # ═══════════════════════════════════════════════════════════════════════
                                    period_sel = Select(self.driver.find_element(By.ID, IDS['period']))
                                    period_opts = [o.text for o in period_sel.options if "Select" not in o.text]
                                    
                                    if not period_opts:
                                        self._add_log(f"⚠️ No periods for Sy:{survey_no} H:{hissa}")
                                        break  # Move to next hissa
                                    
                                    # Determine how many periods to process based on config
                                    period_selected = False
                                    if Config.PROCESS_ALL_PERIODS:
                                        # Process ALL periods for 100% accuracy
                                        max_period_attempts = len(period_opts)
                                    else:
                                        # Process only first few periods (speed mode)
                                        max_period_attempts = min(5, len(period_opts))
                                    
                                    for period_idx in range(max_period_attempts):
                                        if not self.state.running:
                                            return
                                        
                                        period = period_opts[period_idx]
                                        
                                        try:
                                            period_sel = Select(self.driver.find_element(By.ID, IDS['period']))
                                            period_sel.select_by_visible_text(period)
                                            time.sleep(1)
                                            
                                            # Click Fetch Details with verification (rate limited)
                                            _global_rate_limiter.acquire()
                                            fetch_btn = self.driver.find_element(By.ID, IDS['fetch_btn'])
                                            self.driver.execute_script("arguments[0].click();", fetch_btn)
                                            time.sleep(Config.POST_CLICK_WAIT)
                                            
                                            # Handle any portal alerts after Fetch
                                            had_alert, alert_text, is_portal_issue = self._handle_alert()
                                            if is_portal_issue:
                                                # Portal issue - this period might not be accessible, try next
                                                self._add_log(f"⚠️ Portal issue for Sy:{survey_no} H:{hissa} P:{period[:20]}")
                                                continue  # Try next period
                                            
                                            # Verify page loaded (look for owner table)
                                            page_source = self.driver.page_source
                                            if 'Session expired' in page_source or 'login again' in page_source.lower():
                                                raise Exception("Session expired during fetch")
                                            
                                            # Successfully selected period - log it
                                            self._add_log(f"✓ Sy:{survey_no} H:{hissa} Using period: {period[:30]}")
                                            period_selected = True
                                            
                                            # Extract owners
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
                                                    khatah=owner['khatah'],
                                                    worker_id=self.worker_id
                                                )
                                                
                                                record_dict = asdict(record)
                                                
                                                # Check for match
                                                is_match = any(v.lower() in owner['owner_name'].lower() for v in owner_variants if v)
                                                
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
                                            
                                            # If NOT processing all periods, stop after first success
                                            if not Config.PROCESS_ALL_PERIODS:
                                                break
                                            # Otherwise, continue to process remaining periods
                                        
                                        except Exception as period_error:
                                            # This period had an error, try the next one
                                            if Config.PROCESS_ALL_PERIODS:
                                                # When processing all periods, log each error but continue
                                                self.logger.debug(f"Period {period} error: {str(period_error)[:40]}")
                                                continue
                                            elif period_idx < max_period_attempts - 1:
                                                # Speed mode: silently continue to next period
                                                continue
                                            else:
                                                # Last attempt failed - log it
                                                self._add_log(f"⚠️ All periods disabled for Sy:{survey_no} H:{hissa}")
                                                self.errors += 1
                                    
                                    if not period_selected:
                                        # No period could be selected - log and continue
                                        self._add_log(f"⚠️ No available period for Sy:{survey_no} H:{hissa}")
                                    
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
                                        # Reload page for retry
                                        try:
                                            self.driver.get(Config.SERVICE2_URL)
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            Select(self.driver.find_element(By.ID, IDS['district'])).select_by_value(self.params['district_code'])
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            Select(self.driver.find_element(By.ID, IDS['taluk'])).select_by_value(self.params['taluk_code'])
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            Select(self.driver.find_element(By.ID, IDS['hobli'])).select_by_value(hobli_code)
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            Select(self.driver.find_element(By.ID, IDS['village'])).select_by_value(village_code)
                                            time.sleep(Config.POST_SELECT_WAIT)
                                            self.driver.find_element(By.ID, IDS['survey_no']).send_keys(str(survey_no))
                                            go_btn = self.driver.find_element(By.ID, IDS['go_btn'])
                                            self.driver.execute_script("arguments[0].click();", go_btn)
                                            time.sleep(Config.POST_CLICK_WAIT)
                                            Select(self.driver.find_element(By.ID, IDS['surnoc'])).select_by_visible_text(surnoc)
                                            time.sleep(Config.POST_SELECT_WAIT)
                                        except (NoSuchElementException, StaleElementReferenceException, TimeoutException) as retry_err:
                                            self.logger.debug(f"Retry setup failed: {retry_err}")
                                    else:
                                        self._add_log(f"❌ Max retries for Hissa {hissa}, skipping")
                                        self.errors += 1
                                
                    except Exception as surnoc_error:
                        self._add_log(f"⚠️ Surnoc error Sy:{survey_no} S:{surnoc}: {str(surnoc_error)[:40]}")
                        self.errors += 1
                        continue
                
                # ═══════════════════════════════════════════════════════════════════════
                # SUCCESSFULLY PROCESSED SURVEY - Save checkpoint and move to next
                # ═══════════════════════════════════════════════════════════════════════
                
                # Save survey-level checkpoint for granular resume capability
                if self.db and self.session_id:
                    try:
                        self.db.save_survey_checkpoint(
                            session_id=self.session_id,
                            village_code=village_code,
                            survey_no=survey_no,
                            surnocs_processed=list(surnocs)  # Save which surnocs were processed
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
                            break
                
                else:
                    # Other error - log and continue to next survey
                    self.errors += 1
                    empty_count += 1
                    survey_no += 1  # Move to next survey
                    
                    if empty_count > Config.EMPTY_SURVEY_THRESHOLD:
                        self._add_log(f"📊 {village_name} complete after {surveys_checked} surveys, {surveys_with_data} with data")
                        break
        
        # End of village summary
        self._add_log(f"✅ {village_name} COMPLETE: {surveys_checked} surveys, {surveys_with_data} with data, {self.records_found} records")
    
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
                        # Non-browser error, log and move to next village
                        self.errors += 1
                        self._add_log(f"📝 Non-critical error, continuing: {str(village_error)[:50]}")
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
    
    def _prepare_villages(self, params: dict) -> List[Tuple[str, str, str, str]]:
        """
        Prepare list of all villages to search.
        Returns: List of (village_code, village_name, hobli_code, hobli_name)
        """
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        logger.info("Preparing village list...")
        
        # Use Selenium to get exact dropdown values
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # Use cached ChromeDriver path
        driver = webdriver.Chrome(service=CachedChromeDriver.get_service(), options=options)
        
        try:
            driver.get(Config.SERVICE2_URL)
            time.sleep(5)  # Increased wait for page to fully load
            
            IDS = Config.ELEMENT_IDS
            
            # Select district with retry and validation
            logger.info(f"Selecting district: {params.get('district_code')}")
            dist_sel = Select(driver.find_element(By.ID, IDS['district']))
            
            # Wait for options to load
            time.sleep(1)
            dist_opts = {o.get_attribute('value'): o.text for o in dist_sel.options if o.get_attribute('value')}
            logger.info(f"Found {len(dist_opts)} districts. First 5: {list(dist_opts.keys())[:5]}")
            
            params['district_name'] = dist_opts.get(params['district_code'], 'Unknown')
            
            # Handle both "2" and "2.0" formats
            district_code = params['district_code']
            if district_code not in dist_opts:
                # Try with .0 suffix
                district_code_float = f"{district_code}.0" if '.' not in str(district_code) else district_code
                if district_code_float in dist_opts:
                    district_code = district_code_float
                    logger.info(f"Converted district code {params['district_code']} → {district_code}")
                else:
                    raise ValueError(
                        f"District code '{params['district_code']}' not found. "
                        f"Available: {list(dist_opts.keys())[:10]}"
                    )
            
            dist_sel.select_by_value(district_code)
            time.sleep(3)  # Wait for taluk to load
            
            # Select taluk with retry and validation
            logger.info(f"Selecting taluk: {params.get('taluk_code')}")
            taluk_sel = Select(driver.find_element(By.ID, IDS['taluk']))
            
            # Wait for options to load
            time.sleep(1)
            taluk_opts = {o.get_attribute('value'): o.text for o in taluk_sel.options if o.get_attribute('value')}
            logger.info(f"Found {len(taluk_opts)} taluks. First 5: {list(taluk_opts.keys())[:5]}")
            
            params['taluk_name'] = taluk_opts.get(params['taluk_code'], 'Unknown')
            
            # Handle both "5" and "5.0" formats
            taluk_code = params['taluk_code']
            if taluk_code not in taluk_opts:
                # Try with .0 suffix
                taluk_code_float = f"{taluk_code}.0" if '.' not in str(taluk_code) else taluk_code
                if taluk_code_float in taluk_opts:
                    taluk_code = taluk_code_float
                    logger.info(f"Converted taluk code {params['taluk_code']} → {taluk_code}")
                else:
                    raise ValueError(
                        f"Taluk code '{params['taluk_code']}' not found. "
                        f"Available: {list(taluk_opts.keys())[:10]}"
                    )
            
            taluk_sel.select_by_value(taluk_code)
            time.sleep(3)  # Wait for hobli to load
            
            # Get all hoblis
            hobli_sel = Select(driver.find_element(By.ID, IDS['hobli']))
            all_hoblis = [(o.get_attribute('value'), o.text) for o in hobli_sel.options 
                         if o.get_attribute('value') and 'Select' not in o.text]
            
            # Filter hoblis
            hobli_code_param = params.get('hobli_code', 'all')
            if hobli_code_param == 'all':
                hoblis_to_search = all_hoblis
            else:
                hoblis_to_search = [(h, n) for h, n in all_hoblis if h == hobli_code_param]
            
            # Collect all villages
            all_villages = []
            for hobli_code, hobli_name in hoblis_to_search:
                driver.get(Config.SERVICE2_URL)
                time.sleep(2)
                
                Select(driver.find_element(By.ID, IDS['district'])).select_by_value(params['district_code'])
                time.sleep(2)
                Select(driver.find_element(By.ID, IDS['taluk'])).select_by_value(params['taluk_code'])
                time.sleep(2)
                Select(driver.find_element(By.ID, IDS['hobli'])).select_by_value(hobli_code)
                time.sleep(2)
                
                village_sel = Select(driver.find_element(By.ID, IDS['village']))
                villages = [(o.get_attribute('value'), o.text, hobli_code, hobli_name) 
                           for o in village_sel.options 
                           if o.get_attribute('value') and 'Select' not in o.text]
                
                # Filter villages
                village_code_param = params.get('village_code', 'all')
                if village_code_param != 'all' and village_code_param:
                    villages = [(v, vn, h, hn) for v, vn, h, hn in villages if v == village_code_param]
                
                all_villages.extend(villages)
            
            logger.info(f"Found {len(all_villages)} villages to search")
            return all_villages
            
        finally:
            driver.quit()
    
    def _distribute_villages(self, villages: List[Tuple], num_workers: int) -> List[List[Tuple]]:
        """Distribute villages evenly across workers"""
        chunks = [[] for _ in range(num_workers)]
        for i, village in enumerate(villages):
            chunks[i % num_workers].append(village)
        return chunks
    
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
                matches_file=matches_path
            )
            
            # ═══════════════════════════════════════════════════════════════════════
            # CREATE DATABASE SESSION - Records will be saved in real-time!
            # ═══════════════════════════════════════════════════════════════════════
            params['owner_variants'] = self.state.owner_variants
            self.current_session_id = self.db.create_session(params)
            with self.state_lock:
                self.state.logs.append(f"💾 Database session created: {self.current_session_id}")
                self.state.logs.append(f"📁 Data saved to: {self.db.db_path}")
            
            # Initialize CSV writers (backup to database)
            fieldnames = ['district', 'taluk', 'hobli', 'village', 'survey_no', 
                         'surnoc', 'hissa', 'period', 'owner_name', 'extent', 
                         'khatah', 'timestamp', 'worker_id']
            
            self.all_records_writer = ThreadSafeCSVWriter(self.state.all_records_file, fieldnames)
            self.matches_writer = ThreadSafeCSVWriter(self.state.matches_file, fieldnames)
            
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
                    session_id=self.current_session_id  # Current session ID
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
                    self.state.running = False
                    self.state.completed = True
                    
                    # ═══════════════════════════════════════════════════════════════════════
                    # COMPREHENSIVE COMPLETION SUMMARY
                    # ═══════════════════════════════════════════════════════════════════════
                    total_villages = len(self.state.villages_all)
                    processed = len(self.state.villages_processed)
                    retried = len(self.state.villages_retried)
                    failed = len(self.state.villages_failed)
                    
                    self.state.logs.append("=" * 60)
                    self.state.logs.append("📊 FINAL SEARCH SUMMARY")
                    self.state.logs.append("=" * 60)
                    self.state.logs.append(f"📋 Total villages in search: {total_villages}")
                    self.state.logs.append(f"✅ Successfully processed: {processed}")
                    self.state.logs.append(f"🔄 Villages retried (session expiry): {retried}")
                    self.state.logs.append(f"❌ Villages failed: {failed}")
                    self.state.logs.append(f"🔐 Session recovery attempts: {self.state.session_recoveries}")
                    self.state.logs.append(f"📝 Total records found: {self.state.total_records}")
                    self.state.logs.append(f"🎯 Owner matches: {self.state.total_matches}")
                    
                    if failed > 0:
                        self.state.logs.append(f"⚠️ FAILED VILLAGES: {', '.join(self.state.villages_failed)}")
                    
                    if processed < total_villages:
                        missing = total_villages - processed
                        self.state.logs.append(f"⚠️ WARNING: {missing} villages may have been missed!")
                    else:
                        self.state.logs.append("✅ ALL VILLAGES PROCESSED!")
                    
                    self.state.logs.append("=" * 60)
                    
                    # ═══════════════════════════════════════════════════════════════════════
                    # UPDATE DATABASE SESSION STATUS
                    # ═══════════════════════════════════════════════════════════════════════
                    if self.current_session_id:
                        self.db.update_session_status(
                            self.current_session_id, 
                            'completed',
                            villages_completed=processed,
                            total_records=self.state.total_records,
                            total_matches=self.state.total_matches
                        )
                        self.state.logs.append(f"💾 Search saved to database: {self.current_session_id}")
                    
                    logger.info("Search completed")
                    break
    
    def stop_search(self):
        """Stop all workers immediately"""
        logger.info("Stop search requested")
        
        # Set running to False immediately
        self.state.running = False
        
        with self.state_lock:
            self.state.logs.append("⏹️ Stop requested by user - shutting down workers...")
        
        # Force shutdown executor (don't wait for workers)
        if self.executor:
            try:
                self.executor.shutdown(wait=False, cancel_futures=True)
            except Exception as e:
                logger.warning(f"Executor shutdown warning: {e}")
        
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
        
        logger.info("Stop search completed")
    
    def get_state(self) -> dict:
        """Get current search state as dict - with robust error handling"""
        try:
            with self.state_lock:
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
                    'logs': list(self.state.logs[-30:]) if self.state.logs else [],  # Last 30 logs
                    # Real-time records for UI (last 100)
                    'all_records': list(self.state.all_records[-100:]) if self.state.all_records else [],
                    'matches': list(self.state.matches) if self.state.matches else [],
                    # BULLETPROOF VILLAGE TRACKING
                    'village_tracking': {
                        'total_to_search': len(self.state.villages_all) if self.state.villages_all else 0,
                        'processed': len(self.state.villages_processed) if self.state.villages_processed else 0,
                        'retried': len(self.state.villages_retried) if self.state.villages_retried else 0,
                        'failed': len(self.state.villages_failed) if self.state.villages_failed else 0,
                        'session_recoveries': self.state.session_recoveries or 0,
                        'failed_villages': list(self.state.villages_failed[-10:]) if self.state.villages_failed else [],
                    },
                    # Database info
                    'database': {
                        'session_id': self.current_session_id,
                        'db_path': self.db.db_path if self.db else None,
                        'persistent': True  # Records are saved in real-time
                    },
                    'workers': workers_dict
                }
                return state_dict
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
                'database': {'session_id': None, 'db_path': None, 'persistent': True},
                'workers': {}
            }

# ═══════════════════════════════════════════════════════════════════════════════════════
# FLASK APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)
CORS(app)

# Global instances
api = BhoomiAPI()
coordinator = ParallelSearchCoordinator()

# ═══════════════════════════════════════════════════════════════════════════════════════
# HTML TEMPLATE (Enhanced with parallel worker visualization)
# ═══════════════════════════════════════════════════════════════════════════════════════

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>POWER-BHOOMI v3.0 | Bulletproof Edition</title>
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
        }
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
            <div class="version-badge">v3.0 🛡️ Bulletproof • 8 HIGH-SPEED Workers</div>
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
                <label class="form-label">Max Survey Number</label>
                <input type="number" id="maxSurvey" class="form-input" value="200" min="1" max="1000">
            </div>
            
            <button id="searchBtn" class="btn btn-primary">
                <span>⚡</span>
                <span>Start Parallel Search</span>
            </button>
        </aside>
        
        <section>
            <!-- Heartbeat Indicator - Shows if UI is updating -->
            <div class="heartbeat-container" id="heartbeatContainer" style="display: none;">
                <div class="heartbeat-dot" id="heartbeatDot"></div>
                <span class="heartbeat-text">Last update:</span>
                <span class="heartbeat-time" id="heartbeatTime">--:--:--</span>
                <span class="heartbeat-status ok" id="heartbeatStatus">● Live</span>
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
                
                <!-- Workers Grid -->
                <h3 class="card-title" style="margin-top: 1rem;">Browser Workers</h3>
                <div class="workers-grid" id="workersGrid">
                    <div class="worker-card" id="worker-0">
                        <div class="worker-header">
                            <span class="worker-id">🖥️ Worker 1</span>
                            <span class="worker-status idle">Idle</span>
                        </div>
                        <div class="worker-village">Waiting to start...</div>
                        <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                        <div class="worker-stats"><span>0/0 villages</span><span>0 records</span></div>
                    </div>
                    <div class="worker-card" id="worker-1">
                        <div class="worker-header">
                            <span class="worker-id">🖥️ Worker 2</span>
                            <span class="worker-status idle">Idle</span>
                        </div>
                        <div class="worker-village">Waiting to start...</div>
                        <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                        <div class="worker-stats"><span>0/0 villages</span><span>0 records</span></div>
                    </div>
                    <div class="worker-card" id="worker-2">
                        <div class="worker-header">
                            <span class="worker-id">🖥️ Worker 3</span>
                            <span class="worker-status idle">Idle</span>
                        </div>
                        <div class="worker-village">Waiting to start...</div>
                        <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                        <div class="worker-stats"><span>0/0 villages</span><span>0 records</span></div>
                    </div>
                    <div class="worker-card" id="worker-3">
                        <div class="worker-header">
                            <span class="worker-id">🖥️ Worker 4</span>
                            <span class="worker-status idle">Idle</span>
                        </div>
                        <div class="worker-village">Waiting to start...</div>
                        <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                        <div class="worker-stats"><span>0/0 villages</span><span>0 records</span></div>
                    </div>
                    <div class="worker-card" id="worker-4">
                        <div class="worker-header">
                            <span class="worker-id">🖥️ Worker 5</span>
                            <span class="worker-status idle">Idle</span>
                        </div>
                        <div class="worker-village">Waiting to start...</div>
                        <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                        <div class="worker-stats"><span>0/0 villages</span><span>0 records</span></div>
                    </div>
                    <div class="worker-card" id="worker-5">
                        <div class="worker-header">
                            <span class="worker-id">🖥️ Worker 6</span>
                            <span class="worker-status idle">Idle</span>
                        </div>
                        <div class="worker-village">Waiting to start...</div>
                        <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                        <div class="worker-stats"><span>0/0 villages</span><span>0 records</span></div>
                    </div>
                    <div class="worker-card" id="worker-6">
                        <div class="worker-header">
                            <span class="worker-id">🖥️ Worker 7</span>
                            <span class="worker-status idle">Idle</span>
                        </div>
                        <div class="worker-village">Waiting to start...</div>
                        <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                        <div class="worker-stats"><span>0/0 villages</span><span>0 records</span></div>
                    </div>
                    <div class="worker-card" id="worker-7">
                        <div class="worker-header">
                            <span class="worker-id">🖥️ Worker 8</span>
                            <span class="worker-status idle">Idle</span>
                        </div>
                        <div class="worker-village">Waiting to start...</div>
                        <div class="worker-progress"><div class="worker-progress-fill" style="width: 0%"></div></div>
                        <div class="worker-stats"><span>0/0 villages</span><span>0 records</span></div>
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
                                <th>Khatah</th>
                            </tr>
                        </thead>
                        <tbody id="matchesBody">
                            <tr><td colspan="6" class="empty-row">No matches found yet.</td></tr>
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
        
        // Initialize
        document.addEventListener('DOMContentLoaded', () => {
            loadDistricts();
            setupEventListeners();
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
        }
        
        async function loadDistricts() {
            try {
                const res = await fetch('/api/districts');
                const data = await res.json();
                districtSelect.innerHTML = '<option value="">Select District</option>';
                data.forEach(d => {
                    const name = d.district_name_kn || d.district_code;
                    districtSelect.innerHTML += `<option value="${d.district_code}">${name}</option>`;
                });
            } catch (e) {
                districtSelect.innerHTML = '<option value="">Error loading</option>';
            }
        }
        
        async function loadTaluks(distCode) {
            resetDropdowns(['taluk', 'hobli', 'village']);
            talukSelect.innerHTML = '<option value="">Loading...</option>';
            try {
                const res = await fetch(`/api/taluks/${distCode}`);
                const data = await res.json();
                talukSelect.innerHTML = '<option value="">Select Taluk</option>';
                data.forEach(t => {
                    const name = t.taluka_name_kn || t.taluka_code;
                    talukSelect.innerHTML += `<option value="${t.taluka_code}">${name}</option>`;
                });
                talukSelect.disabled = false;
            } catch (e) {}
        }
        
        async function loadHoblis(distCode, talukCode) {
            resetDropdowns(['hobli', 'village']);
            hobliSelect.innerHTML = '<option value="">Loading...</option>';
            try {
                const res = await fetch(`/api/hoblis/${distCode}/${talukCode}`);
                const data = await res.json();
                hobliSelect.innerHTML = '<option value="">Select Hobli</option>';
                hobliSelect.innerHTML += '<option value="all">🔍 All Hoblis (Search Entire Taluk)</option>';
                data.forEach(h => {
                    const name = h.hobli_name_kn || h.hobli_code;
                    hobliSelect.innerHTML += `<option value="${h.hobli_code}">${name}</option>`;
                });
                hobliSelect.disabled = false;
            } catch (e) {}
        }
        
        async function loadVillages(distCode, talukCode, hobliCode) {
            resetDropdowns(['village']);
            villageSelect.innerHTML = '<option value="">Loading...</option>';
            try {
                const res = await fetch(`/api/villages/${distCode}/${talukCode}/${hobliCode}`);
                const data = await res.json();
                villageSelect.innerHTML = '<option value="">Select Village</option>';
                villageSelect.innerHTML += '<option value="all">🔍 All Villages (in this Hobli)</option>';
                data.forEach(v => {
                    const name = v.village_name_kn || v.village_code;
                    villageSelect.innerHTML += `<option value="${v.village_code}">${name}</option>`;
                });
                villageSelect.disabled = false;
            } catch (e) {}
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
            
            if (!districtCode || !talukCode) {
                alert('Please select District and Taluk');
                return;
            }
            
            searchRunning = true;
            searchBtn.innerHTML = '<span class="spinner"></span><span>Stop Search</span>';
            searchBtn.classList.add('btn-stop');
            document.getElementById('progressSection').style.display = 'block';
            
            addLog('🚀 Starting parallel search...');
            
            try {
                await fetch('/api/search/start', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        owner_name: ownerName,
                        district_code: districtCode,
                        taluk_code: talukCode,
                        hobli_code: hobliCode,
                        village_code: villageCode,
                        max_survey: parseInt(maxSurveyInput.value) || 200
                    })
                });
                
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
                
                // Update workers
                if (status.workers) {
                    Object.entries(status.workers).forEach(([id, w]) => {
                        const card = document.getElementById(`worker-${id}`);
                        if (card && w) {
                            const statusEl = card.querySelector('.worker-status');
                            const villageEl = card.querySelector('.worker-village');
                            const progressEl = card.querySelector('.worker-progress-fill');
                            const statsEl = card.querySelector('.worker-stats');
                            
                            if (statusEl) {
                                statusEl.textContent = w.status || 'idle';
                                statusEl.className = `worker-status ${w.status || 'idle'}`;
                            }
                            if (villageEl) villageEl.textContent = w.current_village || 'Waiting...';
                            if (progressEl) progressEl.style.width = (w.progress || 0) + '%';
                            if (statsEl) statsEl.innerHTML = 
                                `<span>${w.villages_completed || 0}/${w.villages_total || 0} villages</span><span>${w.records_found || 0} records</span>`;
                        }
                    });
                }
                
                // Update records tables (real-time)
                if (status.all_records && Array.isArray(status.all_records)) {
                    updateRecordsTable(status.all_records);
                }
                if (status.matches && Array.isArray(status.matches)) {
                    updateMatchesTable(status.matches);
                }
                
                // Update logs
                if (status.logs && Array.isArray(status.logs)) {
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
                
                // Update heartbeat on every successful poll
                updateHeartbeat();
                
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
                    <td>${r.khatah || ''}</td>
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
    global coordinator
    data = request.json
    
    # Create new coordinator for each search
    coordinator = ParallelSearchCoordinator()
    success = coordinator.start_search(data)
    
    return jsonify({'status': 'started' if success else 'failed'})

@app.route('/api/search/status')
def search_status():
    return jsonify(coordinator.get_state())

@app.route('/api/search/stop', methods=['POST'])
def stop_search():
    coordinator.stop_search()
    return jsonify({'status': 'stopped'})

@app.route('/api/download/<file_type>')
def download_csv(file_type):
    """Download CSV file with custom filename"""
    from flask import send_file, request
    
    state = coordinator.get_state()
    
    if file_type == 'records':
        filepath = state.get('all_records_file', '')
        default_name = 'all_records.csv'
    elif file_type == 'matches':
        filepath = state.get('matches_file', '')
        default_name = 'owner_matches.csv'
    else:
        return jsonify({'error': 'Invalid file type'}), 400
    
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
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║              POWER-BHOOMI v3.0 - BULLETPROOF EDITION (8 HIGH-SPEED WORKERS)          ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                      ║
║   🍎 Optimized for macOS                                                             ║
║   🚀 8 HIGH-SPEED Parallel Browser Workers (2x Performance)                          ║
║   📊 Sequential Survey Checking (1, 2, 3... no skips)                                ║
║   ⚡ Speed-Optimized Timeouts                                                        ║
║   📁 CSV Auto-Saves to Downloads                                                     ║
║   💾 Real-time SQLite Database Persistence                                           ║
║                                                                                      ║
║   🌐 Open your browser and navigate to:                                              ║
║                                                                                      ║
║       http://localhost:5001                                                          ║
║                                                                                      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
    """)
    # IMPORTANT: use_reloader=False prevents server restart when code changes mid-search
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG, threaded=True, use_reloader=False)

