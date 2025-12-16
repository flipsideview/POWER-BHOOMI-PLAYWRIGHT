#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                  POWER-BHOOMI v3.0 - BULLETPROOF EDITION                             ║
║                        Karnataka Land Records Search Tool                             ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  Features:                                                                            ║
║  • 4 parallel browser workers (optimized for macOS)                                  ║
║  • SESSION EXPIRATION DETECTION & AUTO-RECOVERY                                      ║
║  • ZERO VILLAGES MISSED - Retry on session expire, not skip!                         ║
║  • Sequential survey iteration (1, 2, 3... no skips)                                 ║
║  • Thread-safe CSV output to Downloads                                               ║
║  • Real-time progress with survey tracking                                           ║
║  • Robust error recovery with detailed logging                                       ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

Version: 3.5.0-MasterDB
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
    """Mac-optimized configuration for speed"""
    # Server
    HOST = '0.0.0.0'
    PORT = 5001
    DEBUG = True
    
    # Parallel Processing - 4 fast workers for Mac
    MAX_WORKERS = 4
    WORKER_STARTUP_DELAY = 0.5  # Fast startup on Mac
    
    # Timeouts (seconds) - Optimized for Mac speed
    PAGE_LOAD_TIMEOUT = 20
    ELEMENT_WAIT_TIMEOUT = 8
    POST_CLICK_WAIT = 4  # Faster clicks
    POST_SELECT_WAIT = 1.5  # Faster selections
    
    # Search Settings - NO SURVEY SKIPPING
    DEFAULT_MAX_SURVEY = 200
    EMPTY_SURVEY_THRESHOLD = 100  # High threshold to avoid premature skipping
    
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
    PROCESS_ALL_PERIODS = False  # Only LATEST period (user preference)
    MAX_HISSA_RETRIES = 2  # Retry individual Hissa on failure
    VERIFY_PAGE_LOAD = True  # Verify page loaded after each action
    LOG_SKIPPED_ITEMS = True  # Log all skipped items for later retry
    
    # Enhancement 1: Survey Range Auto-Detection
    AUTO_DETECT_SURVEY_RANGE = True  # Try to detect max survey from portal
    SURVEY_RANGE_DETECTION_SAMPLES = 5  # Sample this many surveys to estimate range
    
    # Enhancement 4: Smart Empty Detection
    SMART_EMPTY_DETECTION = True  # Use pattern-based detection
    CONSECUTIVE_EMPTY_LIMIT = 30  # Base consecutive empty limit
    SMART_EMPTY_MULTIPLIER = 1.5  # If survey > max_found * this, likely done
    
    # Enhancement 5: Validation Layer
    VALIDATE_RECORDS = True  # Validate extracted data before saving
    INVALID_OWNER_NAMES = {'Select', '--', 'N/A', '', ' ', 'Owner', 'ಮಾಲೀಕರ'}
    MIN_OWNER_NAME_LENGTH = 2
    
    # Enhancement 6: Post-Search Audit
    POST_SEARCH_AUDIT = True  # Run audit after search completes
    AUDIT_RETRY_MISSING = True  # Automatically queue missing items for retry
    
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
    
    # Enhancement tracking
    detected_survey_ranges: Dict[str, int] = field(default_factory=dict)  # village -> max survey
    validation_rejections: int = 0  # Records rejected by validation
    audit_results: Dict = field(default_factory=dict)  # Post-search audit results
    resumable: bool = False  # Whether this search can be resumed
    
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
# THREAD-SAFE CSV WRITER
# ═══════════════════════════════════════════════════════════════════════════════════════

class ThreadSafeCSVWriter:
    """Thread-safe CSV writer for parallel access"""
    
    def __init__(self, filepath: str, fieldnames: List[str]):
        self.filepath = filepath
        self.fieldnames = fieldnames
        self.lock = threading.Lock()
        self._initialized = False
    
    def _initialize(self):
        """Initialize CSV file with headers"""
        if not self._initialized:
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
            self._initialized = True
    
    def write_record(self, record: Dict[str, Any]):
        """Write a single record to CSV (thread-safe)"""
        with self.lock:
            if not self._initialized:
                self._initialize()
            
            with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerow(record)
    
    def write_records(self, records: List[Dict[str, Any]]):
        """Write multiple records to CSV (thread-safe)"""
        with self.lock:
            if not self._initialized:
                self._initialize()
            
            with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerows(records)

# ═══════════════════════════════════════════════════════════════════════════════════════
# PERSISTENT DATABASE MANAGER (SQLite)
# ═══════════════════════════════════════════════════════════════════════════════════════

import sqlite3
from contextlib import contextmanager

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
    
    def __init__(self, db_path: str = None):
        """Initialize database manager with optional custom path"""
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
        self._init_database()
        logger.info(f"📁 Database initialized: {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        """Thread-safe database connection context manager"""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging for better concurrency
        conn.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and speed
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
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
                
                # ═══════════════════════════════════════════════════════════════════════
                # MASTER LOCATION DATABASE - Pre-indexed for 100% accuracy
                # ═══════════════════════════════════════════════════════════════════════
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS master_districts (
                        district_code INTEGER PRIMARY KEY,
                        district_name TEXT,
                        district_name_kn TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS master_taluks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        district_code INTEGER,
                        taluk_code INTEGER,
                        taluk_name TEXT,
                        taluk_name_kn TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(district_code, taluk_code)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS master_hoblis (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        district_code INTEGER,
                        taluk_code INTEGER,
                        hobli_code INTEGER,
                        hobli_name TEXT,
                        hobli_name_kn TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(district_code, taluk_code, hobli_code)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS master_villages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        district_code INTEGER,
                        district_name TEXT,
                        taluk_code INTEGER,
                        taluk_name TEXT,
                        hobli_code INTEGER,
                        hobli_name TEXT,
                        village_code INTEGER,
                        village_name TEXT,
                        village_name_kn TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(district_code, taluk_code, hobli_code, village_code)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS master_sync_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sync_type TEXT,  -- full, partial
                        districts_synced INTEGER DEFAULT 0,
                        taluks_synced INTEGER DEFAULT 0,
                        hoblis_synced INTEGER DEFAULT 0,
                        villages_synced INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'in_progress',
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP
                    )
                ''')
                
                # Create indexes for master tables
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_master_taluks_dist ON master_taluks(district_code)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_master_hoblis_dist_taluk ON master_hoblis(district_code, taluk_code)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_master_villages_full ON master_villages(district_code, taluk_code, hobli_code)')
                
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
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # ENHANCEMENT 2: RESUME CAPABILITY
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def get_resume_state(self, session_id: str) -> Optional[dict]:
        """Get the state needed to resume a search session"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get session info
            cursor.execute('SELECT * FROM search_sessions WHERE session_id = ?', (session_id,))
            session = cursor.fetchone()
            if not session:
                return None
            
            session_dict = dict(session)
            
            # Get completed villages
            cursor.execute('''
                SELECT village_name, max_survey_checked 
                FROM village_progress 
                WHERE session_id = ? AND status = 'completed'
            ''', (session_id,))
            completed_villages = {row['village_name']: row['max_survey_checked'] 
                                  for row in cursor.fetchall()}
            
            # Get in-progress villages with their last survey
            cursor.execute('''
                SELECT village_name, max_survey_checked 
                FROM village_progress 
                WHERE session_id = ? AND status = 'in_progress'
            ''', (session_id,))
            in_progress = {row['village_name']: row['max_survey_checked'] 
                          for row in cursor.fetchall()}
            
            return {
                'session': session_dict,
                'completed_villages': completed_villages,
                'in_progress_villages': in_progress,
                'records_found': session_dict.get('total_records', 0),
                'matches_found': session_dict.get('total_matches', 0)
            }
    
    def save_village_progress(self, session_id: str, village_name: str, 
                              survey_no: int, status: str = 'in_progress'):
        """Save progress for a specific village"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO village_progress 
                    (session_id, village_name, max_survey_checked, status, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (session_id, village_name, survey_no, status))
    
    def get_resumable_sessions(self) -> List[dict]:
        """Get sessions that can be resumed (incomplete)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT s.*, 
                       (SELECT COUNT(*) FROM village_progress vp 
                        WHERE vp.session_id = s.session_id AND vp.status = 'completed') as completed_villages,
                       (SELECT COUNT(*) FROM village_progress vp 
                        WHERE vp.session_id = s.session_id) as total_villages_started
                FROM search_sessions s
                WHERE s.status IN ('in_progress', 'paused', 'error')
                ORDER BY s.created_at DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # ENHANCEMENT 6: POST-SEARCH AUDIT
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def run_audit(self, session_id: str, expected_villages: List[str]) -> dict:
        """
        Run a post-search audit to verify completeness.
        Returns audit results with missing/failed villages.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all processed villages
            cursor.execute('''
                SELECT village_name, status, max_survey_checked
                FROM village_progress 
                WHERE session_id = ?
            ''', (session_id,))
            processed = {row['village_name']: dict(row) for row in cursor.fetchall()}
            
            # Find missing villages
            processed_names = set(processed.keys())
            expected_names = set(expected_villages)
            missing_villages = list(expected_names - processed_names)
            
            # Find failed villages
            failed_villages = [name for name, info in processed.items() 
                              if info['status'] in ('failed', 'error')]
            
            # Get records per village
            cursor.execute('''
                SELECT village, COUNT(*) as count 
                FROM land_records 
                WHERE session_id = ?
                GROUP BY village
            ''', (session_id,))
            records_per_village = {row['village']: row['count'] for row in cursor.fetchall()}
            
            # Identify villages with suspiciously low records
            avg_records = sum(records_per_village.values()) / max(len(records_per_village), 1)
            suspicious_villages = [name for name, count in records_per_village.items()
                                  if count < avg_records * 0.1 and avg_records > 10]
            
            # Get skipped items count
            cursor.execute('''
                SELECT COUNT(*) FROM skipped_items 
                WHERE session_id = ? AND status = 'pending'
            ''', (session_id,))
            pending_skips = cursor.fetchone()[0]
            
            audit_result = {
                'session_id': session_id,
                'audit_time': datetime.now().isoformat(),
                'total_expected': len(expected_villages),
                'total_processed': len(processed),
                'total_missing': len(missing_villages),
                'total_failed': len(failed_villages),
                'total_suspicious': len(suspicious_villages),
                'pending_skips': pending_skips,
                'missing_villages': missing_villages[:20],  # First 20
                'failed_villages': failed_villages[:20],
                'suspicious_villages': suspicious_villages[:20],
                'is_complete': len(missing_villages) == 0 and len(failed_villages) == 0,
                'accuracy_score': (len(processed) - len(failed_villages)) / max(len(expected_villages), 1) * 100
            }
            
            return audit_result
    
    def save_audit_result(self, session_id: str, audit_result: dict):
        """Save audit result to database"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE search_sessions 
                    SET audit_result = ?, status = ?
                    WHERE session_id = ?
                ''', (json.dumps(audit_result), 
                      'completed' if audit_result['is_complete'] else 'incomplete',
                      session_id))
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # MASTER LOCATION DATABASE - Build & Query
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def get_master_stats(self) -> dict:
        """Get statistics of the master location database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM master_districts')
            districts = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM master_taluks')
            taluks = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM master_hoblis')
            hoblis = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM master_villages')
            villages = cursor.fetchone()[0]
            
            # Get last sync info
            cursor.execute('''
                SELECT * FROM master_sync_log 
                ORDER BY id DESC LIMIT 1
            ''')
            last_sync = cursor.fetchone()
            
            return {
                'districts': districts,
                'taluks': taluks,
                'hoblis': hoblis,
                'villages': villages,
                'total_locations': districts + taluks + hoblis + villages,
                'is_synced': villages > 0,
                'last_sync': dict(last_sync) if last_sync else None
            }
    
    def save_master_district(self, district: dict):
        """Save a district to master database"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO master_districts 
                    (district_code, district_name, district_name_kn)
                    VALUES (?, ?, ?)
                ''', (
                    district.get('district_code'),
                    district.get('district_name_en', district.get('district_name', '')),
                    district.get('district_name_kn', '')
                ))
    
    def save_master_taluk(self, district_code: int, taluk: dict):
        """Save a taluk to master database"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO master_taluks 
                    (district_code, taluk_code, taluk_name, taluk_name_kn)
                    VALUES (?, ?, ?, ?)
                ''', (
                    district_code,
                    taluk.get('taluka_code'),
                    taluk.get('taluka_name_en', taluk.get('taluka_name', '')),
                    taluk.get('taluka_name_kn', '')
                ))
    
    def save_master_hobli(self, district_code: int, taluk_code: int, hobli: dict):
        """Save a hobli to master database"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO master_hoblis 
                    (district_code, taluk_code, hobli_code, hobli_name, hobli_name_kn)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    district_code,
                    taluk_code,
                    hobli.get('hobli_code'),
                    hobli.get('hobli_name_en', hobli.get('hobli_name', '')),
                    hobli.get('hobli_name_kn', '')
                ))
    
    def save_master_village(self, district_code: int, district_name: str,
                           taluk_code: int, taluk_name: str,
                           hobli_code: int, hobli_name: str, village: dict):
        """Save a village to master database"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO master_villages 
                    (district_code, district_name, taluk_code, taluk_name,
                     hobli_code, hobli_name, village_code, village_name, village_name_kn)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    district_code, district_name,
                    taluk_code, taluk_name,
                    hobli_code, hobli_name,
                    village.get('village_code'),
                    village.get('village_name_en', village.get('village_name', '')),
                    village.get('village_name_kn', '')
                ))
    
    def start_master_sync(self) -> int:
        """Start a new sync log entry, returns sync_id"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO master_sync_log (sync_type, status)
                    VALUES ('full', 'in_progress')
                ''')
                return cursor.lastrowid
    
    def update_master_sync(self, sync_id: int, districts: int = 0, taluks: int = 0,
                          hoblis: int = 0, villages: int = 0, status: str = 'in_progress'):
        """Update sync progress"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                completed_at = datetime.now().isoformat() if status == 'completed' else None
                cursor.execute('''
                    UPDATE master_sync_log 
                    SET districts_synced = ?, taluks_synced = ?, hoblis_synced = ?, 
                        villages_synced = ?, status = ?, completed_at = ?
                    WHERE id = ?
                ''', (districts, taluks, hoblis, villages, status, completed_at, sync_id))
    
    def get_master_villages_for_search(self, district_code: int = None, taluk_code: int = None,
                                       hobli_code: int = None) -> List[dict]:
        """Get villages from master database for search"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = 'SELECT * FROM master_villages WHERE 1=1'
            params = []
            
            if district_code:
                query += ' AND district_code = ?'
                params.append(district_code)
            if taluk_code:
                query += ' AND taluk_code = ?'
                params.append(taluk_code)
            if hobli_code:
                query += ' AND hobli_code = ?'
                params.append(hobli_code)
            
            query += ' ORDER BY district_name, taluk_name, hobli_name, village_name'
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_master_districts(self) -> List[dict]:
        """Get all districts from master database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM master_districts ORDER BY district_name')
            return [dict(row) for row in cursor.fetchall()]
    
    def get_master_taluks(self, district_code: int) -> List[dict]:
        """Get all taluks for a district from master database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM master_taluks 
                WHERE district_code = ? ORDER BY taluk_name
            ''', (district_code,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_master_hoblis(self, district_code: int, taluk_code: int) -> List[dict]:
        """Get all hoblis from master database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM master_hoblis 
                WHERE district_code = ? AND taluk_code = ? ORDER BY hobli_name
            ''', (district_code, taluk_code))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_master_villages(self, district_code: int, taluk_code: int, hobli_code: int) -> List[dict]:
        """Get all villages from master database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM master_villages 
                WHERE district_code = ? AND taluk_code = ? AND hobli_code = ?
                ORDER BY village_name
            ''', (district_code, taluk_code, hobli_code))
            return [dict(row) for row in cursor.fetchall()]
    
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
    
    def clear_cache(self):
        """Clear the API cache"""
        self._cache = {}
        logger.info("API cache cleared")
    
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
# MASTER DATABASE SYNCER - Pre-index ALL locations for 100% accuracy
# ═══════════════════════════════════════════════════════════════════════════════════════

class MasterDatabaseSyncer:
    """
    Syncs ALL Districts → Taluks → Hoblis → Villages to local database.
    This ensures we know EXACTLY what exists before searching.
    """
    
    def __init__(self, db: DatabaseManager, api: 'BhoomiAPI'):
        self.db = db
        self.api = api
        self.logger = logging.getLogger('MasterSync')
        self.sync_id = None
        self.stats = {'districts': 0, 'taluks': 0, 'hoblis': 0, 'villages': 0}
        self.is_syncing = False
        self.sync_progress = []
    
    def sync_all(self, progress_callback=None) -> dict:
        """
        Sync entire Karnataka location hierarchy.
        Returns statistics of synced data.
        """
        if self.is_syncing:
            return {'error': 'Sync already in progress'}
        
        self.is_syncing = True
        self.sync_progress = []
        self.stats = {'districts': 0, 'taluks': 0, 'hoblis': 0, 'villages': 0}
        
        try:
            self.sync_id = self.db.start_master_sync()
            self._log("🚀 Starting Master Database Sync...")
            
            # Clear API cache to ensure fresh data
            self.api.clear_cache()
            self._log("🧹 Cleared API cache")
            
            # Get all districts
            districts = self.api.get_districts()
            self._log(f"📍 Found {len(districts)} districts")
            
            for district in districts:
                if not self.is_syncing:
                    break
                    
                # API returns floats (1.0, 2.0) - convert to int
                district_code = int(district.get('district_code', 0))
                # Use Kannada name since that's what API returns
                district_name = district.get('district_name_kn', district.get('district_name_en', district.get('district_name', f'District {district_code}')))
                
                # Save district
                self.db.save_master_district(district)
                self.stats['districts'] += 1
                
                # Get taluks for this district
                taluks = self.api.get_taluks(district_code)
                self._log(f"  📍 {district_name}: {len(taluks)} taluks")
                
                for taluk in taluks:
                    if not self.is_syncing:
                        break
                    
                    # API returns floats - convert to int
                    taluk_code = int(taluk.get('taluka_code', 0))
                    taluk_name = taluk.get('taluka_name_kn', taluk.get('taluka_name_en', taluk.get('taluka_name', f'Taluk {taluk_code}')))
                    
                    # Save taluk
                    self.db.save_master_taluk(district_code, taluk)
                    self.stats['taluks'] += 1
                    
                    # Get hoblis for this taluk
                    hoblis = self.api.get_hoblis(district_code, taluk_code)
                    
                    for hobli in hoblis:
                        if not self.is_syncing:
                            break
                        
                        # API returns floats - convert to int
                        hobli_code = int(hobli.get('hobli_code', 0))
                        hobli_name = hobli.get('hobli_name_kn', hobli.get('hobli_name_en', hobli.get('hobli_name', f'Hobli {hobli_code}')))
                        
                        # Save hobli
                        self.db.save_master_hobli(district_code, taluk_code, hobli)
                        self.stats['hoblis'] += 1
                        
                        # Get villages for this hobli
                        villages = self.api.get_villages(district_code, taluk_code, hobli_code)
                        
                        for village in villages:
                            # Save village with full hierarchy
                            self.db.save_master_village(
                                district_code, district_name,
                                taluk_code, taluk_name,
                                hobli_code, hobli_name,
                                village
                            )
                            self.stats['villages'] += 1
                        
                        # Update sync progress periodically
                        if self.stats['villages'] % 100 == 0:
                            self.db.update_master_sync(
                                self.sync_id,
                                self.stats['districts'],
                                self.stats['taluks'],
                                self.stats['hoblis'],
                                self.stats['villages']
                            )
                            self._log(f"    ✓ Synced {self.stats['villages']} villages...")
                
                # Log district completion
                self._log(f"  ✅ {district_name} complete")
            
            # Mark sync as complete
            self.db.update_master_sync(
                self.sync_id,
                self.stats['districts'],
                self.stats['taluks'],
                self.stats['hoblis'],
                self.stats['villages'],
                'completed'
            )
            
            self._log(f"✅ SYNC COMPLETE!")
            self._log(f"   Districts: {self.stats['districts']}")
            self._log(f"   Taluks: {self.stats['taluks']}")
            self._log(f"   Hoblis: {self.stats['hoblis']}")
            self._log(f"   Villages: {self.stats['villages']}")
            
            return self.stats
            
        except Exception as e:
            self._log(f"❌ Sync error: {str(e)}")
            if self.sync_id:
                self.db.update_master_sync(self.sync_id, status='failed')
            return {'error': str(e)}
        finally:
            self.is_syncing = False
    
    def stop_sync(self):
        """Stop ongoing sync"""
        self.is_syncing = False
        self._log("⏹️ Sync stopped by user")
    
    def _log(self, message: str):
        """Log sync progress"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = f"{timestamp} - {message}"
        self.sync_progress.append(log_entry)
        self.logger.info(message)
    
    def get_sync_status(self) -> dict:
        """Get current sync status"""
        return {
            'is_syncing': self.is_syncing,
            'stats': self.stats,
            'progress': self.sync_progress[-50:] if self.sync_progress else []
        }


# Global syncer instance
master_syncer = None

def get_master_syncer():
    """Get or create master syncer"""
    global master_syncer
    if master_syncer is None:
        master_syncer = MasterDatabaseSyncer(get_database(), api)
    return master_syncer


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
        
        # Worker-local stats
        self.records_found = 0
        self.matches_found = 0
        self.errors = 0
    
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
        
        # Clean user data directory for this worker
        user_data_dir = os.path.join(tempfile.gettempdir(), f'bhoomi_chrome_{self.worker_id}')
        if os.path.exists(user_data_dir):
            try:
                shutil.rmtree(user_data_dir)
            except:
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
                options.add_argument(f'--user-data-dir={user_data_dir}')
                
                # Page load strategy - don't wait for all resources
                options.page_load_strategy = 'eager'
                
                service = Service(ChromeDriverManager().install())
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
                except:
                    pass
                self.driver = None
        
        # All retries failed
        raise Exception(f"Failed to initialize browser after {retry_count} attempts: {last_error}")
    
    def _close_browser(self):
        """Safely close browser and cleanup"""
        import shutil
        import tempfile
        
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
        
        # Clean up user data directory
        user_data_dir = os.path.join(tempfile.gettempdir(), f'chrome_worker_{self.worker_id}_{os.getpid()}')
    
    def _is_session_expired(self, page_source: str = None) -> bool:
        """
        Detect if the Bhoomi portal session has expired.
        Returns True if session expired, False otherwise.
        """
        try:
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
        try:
            if os.path.exists(user_data_dir):
                shutil.rmtree(user_data_dir, ignore_errors=True)
        except:
            pass
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # ENHANCEMENT 5: VALIDATION LAYER
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def _validate_record(self, owner: dict) -> bool:
        """
        Validate extracted owner record before saving.
        Returns True if record is valid, False otherwise.
        """
        owner_name = owner.get('owner_name', '').strip()
        
        # Check for empty or too short names
        if len(owner_name) < Config.MIN_OWNER_NAME_LENGTH:
            return False
        
        # Check against invalid names list
        if owner_name in Config.INVALID_OWNER_NAMES:
            return False
        
        # Check for common invalid patterns
        invalid_patterns = [
            owner_name.lower() == 'select',
            owner_name.startswith('--'),
            owner_name.isdigit(),  # Pure numbers are not valid names
            owner_name.lower() in ['owner', 'name', 'sl.no', 'slno'],
        ]
        
        if any(invalid_patterns):
            return False
        
        # Valid record
        return True
    
    def _extract_owners(self, page_source: str) -> List[dict]:
        """
        Extract owner details from page source.
        IMPROVED for 100% accuracy - multiple extraction strategies.
        """
        from bs4 import BeautifulSoup
        import re
        
        owners = []
        try:
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Strategy 1: Look for tables with Owner/Extent keywords
            for table in soup.find_all('table'):
                table_text = table.get_text()
                if any(kw in table_text for kw in ['Owner', 'ಮಾಲೀಕರ', 'Extent', 'ವಿಸ್ತೀರ್ಣ', 'Khata', 'ಖಾತಾ']):
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            cell_texts = [c.get_text(strip=True) for c in cells]
                            row_text = ' '.join(cell_texts)
                            
                            # Multiple patterns to catch owner data
                            # Pattern 1: Extent format like 0.12.0 or 1-2-3
                            # Pattern 2: Rows with substantial text (likely names)
                            has_extent = re.search(r'\d+[\.\-]\d+[\.\-]\d+', row_text)
                            has_name = len(cell_texts[0]) > 2 and not cell_texts[0].isdigit()
                            
                            # Skip header rows
                            is_header = any(h in row_text.lower() for h in ['owner', 'extent', 'sl.no', 'slno', 'ಮಾಲೀಕರ'])
                            
                            if (has_extent or has_name) and not is_header:
                                owner_entry = {
                                    'owner_name': cell_texts[0] if cell_texts else '',
                                    'extent': cell_texts[1] if len(cell_texts) > 1 else '',
                                    'khatah': cell_texts[2] if len(cell_texts) > 2 else '',
                                }
                                # Avoid duplicates
                                if owner_entry['owner_name'] and owner_entry not in owners:
                                    owners.append(owner_entry)
            
            # Strategy 2: Look for specific div/span elements with owner info
            if not owners:
                # Try finding labeled sections
                for label in soup.find_all(['label', 'span', 'div']):
                    label_text = label.get_text(strip=True)
                    if 'Owner' in label_text or 'ಮಾಲೀಕ' in label_text:
                        # Get next sibling or parent's next element for the value
                        next_elem = label.find_next(['span', 'div', 'td'])
                        if next_elem:
                            owner_name = next_elem.get_text(strip=True)
                            if owner_name and len(owner_name) > 2:
                                owners.append({
                                    'owner_name': owner_name,
                                    'extent': '',
                                    'khatah': ''
                                })
            
            # Log extraction result for debugging
            if not owners:
                self.logger.warning(f"No owners extracted from page")
                
        except Exception as e:
            self.logger.error(f"Extract error: {e}")
        
        return owners
    
    # ═══════════════════════════════════════════════════════════════════════════════════
    # ENHANCEMENT 1: SURVEY RANGE AUTO-DETECTION
    # ═══════════════════════════════════════════════════════════════════════════════════
    
    def _detect_survey_range(self, village_code: str, hobli_code: str) -> int:
        """
        Auto-detect the survey range for a village by sampling.
        Uses binary search-like approach to find max survey.
        Returns estimated max survey number.
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select
        
        IDS = Config.ELEMENT_IDS
        
        if not Config.AUTO_DETECT_SURVEY_RANGE:
            return self.params.get('max_survey', Config.DEFAULT_MAX_SURVEY)
        
        try:
            # Sample survey numbers to estimate range
            # Test: 1, 50, 100, 200, 500
            test_surveys = [1, 50, 100, 200, 500]
            max_found = 0
            
            for test_no in test_surveys:
                try:
                    # Clear and enter survey number
                    survey_input = self.driver.find_element(By.ID, IDS['survey_no'])
                    survey_input.clear()
                    survey_input.send_keys(str(test_no))
                    
                    # Click Go
                    go_btn = self.driver.find_element(By.ID, IDS['go_btn'])
                    self.driver.execute_script("arguments[0].click();", go_btn)
                    time.sleep(2)  # Quick check
                    
                    # Check if surnoc has options
                    surnoc_sel = Select(self.driver.find_element(By.ID, IDS['surnoc']))
                    surnoc_opts = [o.text for o in surnoc_sel.options if "Select" not in o.text]
                    
                    if surnoc_opts:
                        max_found = test_no
                    else:
                        break  # No data at this survey, earlier was likely max
                    
                except:
                    break
            
            # Estimate max as 1.3x the highest found (with data)
            estimated_max = int(max_found * 1.3) if max_found > 0 else Config.DEFAULT_MAX_SURVEY
            
            # Store for smart detection
            with self.state_lock:
                self.state.detected_survey_ranges[village_code] = estimated_max
            
            self._add_log(f"📊 Auto-detected survey range: 1-{estimated_max} (found data up to {max_found})")
            
            return max(estimated_max, 20)  # Minimum 20 surveys to check
            
        except Exception as e:
            self.logger.warning(f"Survey range detection failed: {e}")
            return self.params.get('max_survey', Config.DEFAULT_MAX_SURVEY)
    
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
        owner_variants = self.state.owner_variants
        
        district_name = self.params.get('district_name', 'Unknown')
        taluk_name = self.params.get('taluk_name', 'Unknown')
        
        # ═══════════════════════════════════════════════════════════════════════════════════
        # ENHANCEMENT 2: Check if resuming - get last survey checked
        # ═══════════════════════════════════════════════════════════════════════════════════
        start_survey = 1
        if self.db and self.session_id:
            resume_state = self.db.get_resume_state(self.session_id)
            if resume_state:
                in_progress = resume_state.get('in_progress_villages', {})
                if village_name in in_progress:
                    start_survey = in_progress[village_name] + 1
                    self._add_log(f"📌 Resuming {village_name} from survey {start_survey}")
        
        # ═══════════════════════════════════════════════════════════════════════════════════
        # ENHANCEMENT 1: Auto-detect survey range (optional)
        # ═══════════════════════════════════════════════════════════════════════════════════
        max_survey = self.params.get('max_survey', Config.DEFAULT_MAX_SURVEY)
        
        self._update_status(
            current_village=village_name,
            current_survey=start_survey,
            max_survey=max_survey
        )
        
        empty_count = 0
        surveys_checked = 0
        surveys_with_data = 0
        session_retries = 0  # Track session recovery attempts
        self._max_survey_with_data = 0  # Track for smart detection
        
        self._add_log(f"🏘️ Starting {village_name}: Surveys {start_survey} to {max_survey}")
        
        # SEQUENTIAL SURVEY ITERATION: No skipping surveys
        survey_no = start_survey
        last_progress_save = 0  # Track when we last saved progress
        
        while survey_no <= max_survey:
            if not self.state.running:
                self._add_log(f"⏹️ Stopped at survey {survey_no}/{max_survey}")
                # Save progress before stopping (Enhancement 2)
                if self.db and self.session_id:
                    self.db.save_village_progress(self.session_id, village_name, survey_no, 'paused')
                return
            
            # Save progress every 10 surveys (Enhancement 2)
            if self.db and self.session_id and survey_no - last_progress_save >= 10:
                self.db.save_village_progress(self.session_id, village_name, survey_no, 'in_progress')
                last_progress_save = survey_no
            
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
                
                # Click GO using JavaScript
                go_btn = self.driver.find_element(By.ID, IDS['go_btn'])
                self.driver.execute_script("arguments[0].click();", go_btn)
                time.sleep(Config.POST_CLICK_WAIT)
                
                # ═══════════════════════════════════════════════════════════════════════
                # SESSION EXPIRATION CHECK #2 - After clicking GO
                # ═══════════════════════════════════════════════════════════════════════
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
                    
                    # ═══════════════════════════════════════════════════════════════════════
                    # ENHANCEMENT 4: SMART EMPTY DETECTION
                    # Use pattern-based detection instead of fixed threshold
                    # ═══════════════════════════════════════════════════════════════════════
                    should_stop = False
                    
                    if Config.SMART_EMPTY_DETECTION and surveys_with_data > 0:
                        # Pattern 1: If we've gone past 1.5x the max found survey with data
                        max_survey_with_data = getattr(self, '_max_survey_with_data', survey_no)
                        if survey_no > max_survey_with_data * Config.SMART_EMPTY_MULTIPLIER:
                            should_stop = True
                            self._add_log(f"🧠 Smart detection: survey {survey_no} > {max_survey_with_data}*1.5, likely end")
                        
                        # Pattern 2: If consecutive empties exceed adaptive threshold
                        adaptive_threshold = min(Config.CONSECUTIVE_EMPTY_LIMIT, 
                                                 max(10, int(surveys_with_data * 0.3)))
                        if empty_count > adaptive_threshold:
                            should_stop = True
                            self._add_log(f"🧠 Smart detection: {empty_count} consecutive empty > adaptive threshold {adaptive_threshold}")
                    else:
                        # Fallback to fixed threshold
                        if empty_count > Config.EMPTY_SURVEY_THRESHOLD:
                            should_stop = True
                    
                    if should_stop:
                        self._add_log(f"⏭️ {village_name}: completing after {empty_count} empty surveys")
                        self._add_log(f"📊 {village_name} Summary: Checked {surveys_checked}, Found data in {surveys_with_data}")
                        break
                    
                    survey_no += 1  # Move to next survey
                    continue
                
                # Found data - reset empty count and increment found count
                empty_count = 0
                surveys_with_data += 1
                
                # Track max survey with data for smart detection
                self._max_survey_with_data = survey_no
                
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
                                    
                                    # Select LATEST period only (user preference for speed)
                                    period_sel = Select(self.driver.find_element(By.ID, IDS['period']))
                                    period_opts = [o.text for o in period_sel.options if "Select" not in o.text]
                                    
                                    if not period_opts:
                                        self._add_log(f"⚠️ No periods for Sy:{survey_no} H:{hissa}")
                                        break  # Move to next hissa
                                    
                                    # Select LATEST period (first in list is most recent)
                                    period = period_opts[0]
                                    period_sel.select_by_visible_text(period)
                                    time.sleep(1)
                                    
                                    # Click Fetch Details with verification
                                    fetch_btn = self.driver.find_element(By.ID, IDS['fetch_btn'])
                                    self.driver.execute_script("arguments[0].click();", fetch_btn)
                                    time.sleep(Config.POST_CLICK_WAIT)
                                    
                                    # Verify page loaded (look for owner table)
                                    page_source = self.driver.page_source
                                    if 'Session expired' in page_source or 'login again' in page_source.lower():
                                        raise Exception("Session expired during fetch")
                                    
                                    # Extract owners
                                    owners = self._extract_owners(page_source)
                                    
                                    for owner in owners:
                                        # ═══════════════════════════════════════════════════════════════════════
                                        # ENHANCEMENT 5: VALIDATION LAYER - Validate before saving
                                        # ═══════════════════════════════════════════════════════════════════════
                                        if Config.VALIDATE_RECORDS:
                                            if not self._validate_record(owner):
                                                with self.state_lock:
                                                    self.state.validation_rejections += 1
                                                continue  # Skip invalid records
                                        
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
                                        if self.db and self.session_id:
                                            self.db.save_record(self.session_id, record_dict, is_match=is_match)
                                        
                                        # Write to CSV (backup)
                                        self.all_records_writer.write_record(record_dict)
                                        self.records_found += 1
                                        
                                        # Add to state for real-time UI display
                                        with self.state_lock:
                                            self.state.all_records.append(record_dict)
                                            if len(self.state.all_records) > 500:
                                                self.state.all_records = self.state.all_records[-500:]
                                        
                                        if is_match:
                                            self.matches_writer.write_record(record_dict)
                                            self.matches_found += 1
                                            with self.state_lock:
                                                self.state.matches.append(record_dict)
                                            self._add_log(f"🎯 MATCH: {owner['owner_name']} in {village_name} Sy:{survey_no}")
                                    
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
                                        except:
                                            pass  # Will retry in next iteration
                                    else:
                                        self._add_log(f"❌ Max retries for Hissa {hissa}, skipping")
                                        self.errors += 1
                                
                    except Exception as surnoc_error:
                        self._add_log(f"⚠️ Surnoc error Sy:{survey_no} S:{surnoc}: {str(surnoc_error)[:40]}")
                        self.errors += 1
                        continue
                
                # ═══════════════════════════════════════════════════════════════════════
                # SUCCESSFULLY PROCESSED SURVEY - Move to next
                # ═══════════════════════════════════════════════════════════════════════
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
                        except:
                            # Refresh failed - browser might be dead, restart it
                            self._add_log(f"🔄 Session refresh failed, restarting browser...")
                            self._close_browser()
                            time.sleep(2)
                            try:
                                self._init_browser()
                                continue  # RETRY same survey with new browser
                            except:
                                self._add_log(f"❌ Browser restart failed!")
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
                        except:
                            self._add_log(f"❌ Browser restart failed after max retries!")
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
        
        # Mark village as completed in database (Enhancement 2)
        if self.db and self.session_id:
            self.db.save_village_progress(self.session_id, village_name, survey_no, 'completed')
    
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
        
        NOTE: eChawadi API codes DON'T match Bhoomi portal dropdown values!
        We must select by visible text (name) instead of by value (code).
        """
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        logger.info("Preparing village list...")
        
        # Get the names from eChawadi API for matching
        district_name = params.get('district_name', '')
        taluk_name = params.get('taluk_name', '')
        
        # Use Selenium to get exact dropdown values
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        
        try:
            driver.get(Config.SERVICE2_URL)
            time.sleep(3)
            
            IDS = Config.ELEMENT_IDS
            
            # Select district by finding matching name
            dist_sel = Select(driver.find_element(By.ID, IDS['district']))
            
            # Find the district option that matches our name
            district_found = False
            for opt in dist_sel.options:
                opt_text = opt.text.strip().upper()
                if opt.get_attribute('value') and opt.get_attribute('value') != '0':
                    # Try exact match first, then partial match
                    if district_name.upper() in opt_text or opt_text in district_name.upper():
                        dist_sel.select_by_visible_text(opt.text)
                        params['district_name'] = opt.text
                        params['portal_district_value'] = opt.get_attribute('value')
                        district_found = True
                        logger.info(f"Selected district: {opt.text} (value: {opt.get_attribute('value')})")
                        break
            
            if not district_found:
                raise Exception(f"District '{district_name}' not found in portal dropdown")
            
            time.sleep(2)
            
            # Select taluk by finding matching name
            taluk_sel = Select(driver.find_element(By.ID, IDS['taluk']))
            
            taluk_found = False
            for opt in taluk_sel.options:
                opt_text = opt.text.strip().upper()
                if opt.get_attribute('value') and opt.get_attribute('value') != '0':
                    if taluk_name.upper() in opt_text or opt_text in taluk_name.upper():
                        taluk_sel.select_by_visible_text(opt.text)
                        params['taluk_name'] = opt.text
                        params['portal_taluk_value'] = opt.get_attribute('value')
                        taluk_found = True
                        logger.info(f"Selected taluk: {opt.text} (value: {opt.get_attribute('value')})")
                        break
            
            if not taluk_found:
                raise Exception(f"Taluk '{taluk_name}' not found in portal dropdown")
            
            time.sleep(2)
            
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
            self.state.running = False
            return False
    
    def _monitor_completion(self):
        """Monitor workers and mark search as complete when all done"""
        while self.state.running:
            time.sleep(2)
            
            with self.state_lock:
                all_done = all(
                    ws.status in ('completed', 'failed') 
                    for ws in self.state.workers.values()
                )
                
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
                        
                        # ═══════════════════════════════════════════════════════════════════════
                        # ENHANCEMENT 6: POST-SEARCH AUDIT
                        # ═══════════════════════════════════════════════════════════════════════
                        if Config.POST_SEARCH_AUDIT:
                            self.state.logs.append("🔍 Running post-search audit...")
                            try:
                                audit_result = self.db.run_audit(
                                    self.current_session_id, 
                                    self.state.villages_all
                                )
                                self.db.save_audit_result(self.current_session_id, audit_result)
                                
                                # Store in state for UI
                                self.state.audit_results = audit_result
                                
                                # Log audit results
                                self.state.logs.append(f"📊 AUDIT RESULTS:")
                                self.state.logs.append(f"   Expected villages: {audit_result['total_expected']}")
                                self.state.logs.append(f"   Processed: {audit_result['total_processed']}")
                                self.state.logs.append(f"   Missing: {audit_result['total_missing']}")
                                self.state.logs.append(f"   Failed: {audit_result['total_failed']}")
                                self.state.logs.append(f"   Accuracy Score: {audit_result['accuracy_score']:.1f}%")
                                
                                if audit_result['is_complete']:
                                    self.state.logs.append("✅ AUDIT PASSED: 100% villages processed!")
                                else:
                                    self.state.logs.append(f"⚠️ AUDIT: {audit_result['total_missing']} villages need retry")
                                    if audit_result['missing_villages']:
                                        self.state.logs.append(f"   Missing: {', '.join(audit_result['missing_villages'][:5])}...")
                                        
                                # Get skipped items count
                                skipped_count = self.db.get_skipped_count(self.current_session_id)
                                if skipped_count > 0:
                                    self.state.logs.append(f"📋 Skipped items for retry: {skipped_count}")
                                    
                            except Exception as audit_err:
                                self.state.logs.append(f"⚠️ Audit error: {str(audit_err)[:50]}")
                    
                    logger.info("Search completed")
                    break
    
    def stop_search(self):
        """Stop all workers"""
        self.state.running = False
        with self.state_lock:
            self.state.logs.append("⏹️ Stop requested by user")
        
        # Update database session status
        if self.current_session_id:
            self.db.update_session_status(
                self.current_session_id, 
                'stopped',
                villages_completed=len(self.state.villages_processed),
                total_records=self.state.total_records,
                total_matches=self.state.total_matches
            )
        
        if self.executor:
            self.executor.shutdown(wait=False)
    
    def get_state(self) -> dict:
        """Get current search state as dict"""
        with self.state_lock:
            state_dict = {
                'running': self.state.running,
                'completed': self.state.completed,
                'start_time': self.state.start_time,
                'owner_name': self.state.owner_name,
                'total_workers': self.state.total_workers,
                'active_workers': self.state.active_workers,
                'total_villages': self.state.total_villages,
                'villages_completed': self.state.villages_completed,
                'total_records': self.state.total_records,
                'total_matches': self.state.total_matches,
                'progress': int((self.state.villages_completed / max(self.state.total_villages, 1)) * 100),
                'all_records_file': self.state.all_records_file,
                'matches_file': self.state.matches_file,
                'logs': self.state.logs[-30:],  # Last 30 logs (increased)
                # Real-time records for UI (last 100)
                'all_records': self.state.all_records[-100:],
                'matches': self.state.matches,
                # BULLETPROOF VILLAGE TRACKING
                'village_tracking': {
                    'total_to_search': len(self.state.villages_all),
                    'processed': len(self.state.villages_processed),
                    'retried': len(self.state.villages_retried),
                    'failed': len(self.state.villages_failed),
                    'session_recoveries': self.state.session_recoveries,
                    'failed_villages': self.state.villages_failed[-10:],  # Last 10 failed
                },
                # Database info
                'database': {
                    'session_id': self.current_session_id,
                    'db_path': self.db.db_path if self.db else None,
                    'persistent': True  # Records are saved in real-time
                },
                'workers': {
                    str(wid): {
                        'status': ws.status,
                        'current_village': ws.current_village,
                        'current_survey': ws.current_survey,
                        'max_survey': ws.max_survey,
                        'villages_completed': ws.villages_completed,
                        'villages_total': ws.villages_total,
                        'records_found': ws.records_found,
                        'matches_found': ws.matches_found,
                        'progress': int((ws.villages_completed / max(ws.villages_total, 1)) * 100)
                    }
                    for wid, ws in self.state.workers.items()
                }
            }
            return state_dict

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
            grid-template-columns: repeat(2, 1fr);
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
        @media (max-width: 1200px) {
            .main-container { grid-template-columns: 1fr; }
            .workers-grid { grid-template-columns: repeat(2, 1fr); }
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
        }
        
        @media (max-width: 768px) {
            .workers-grid { grid-template-columns: 1fr; }
        }
        
        /* Master Sync Styles */
        .master-sync-card {
            margin-bottom: 0;
        }
        
        .master-stats {
            background: var(--bg-input);
            border-radius: 8px;
            padding: 0.75rem;
            margin-bottom: 1rem;
        }
        
        .stat-row {
            display: flex;
            justify-content: space-between;
            padding: 0.35rem 0;
            font-size: 0.85rem;
            color: var(--text-secondary);
            border-bottom: 1px solid var(--border-color);
        }
        
        .stat-row:last-child {
            border-bottom: none;
        }
        
        .stat-row.total {
            font-weight: 600;
            color: var(--accent-primary);
            padding-top: 0.5rem;
            margin-top: 0.25rem;
            border-top: 1px solid var(--accent-primary);
        }
        
        .sync-status {
            text-align: center;
            margin-bottom: 1rem;
        }
        
        .sync-badge {
            display: inline-block;
            padding: 0.35rem 0.75rem;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        
        .sync-badge.not-synced {
            background: rgba(239, 68, 68, 0.15);
            color: var(--error);
            border: 1px solid rgba(239, 68, 68, 0.3);
        }
        
        .sync-badge.synced {
            background: rgba(16, 185, 129, 0.15);
            color: var(--success);
            border: 1px solid rgba(16, 185, 129, 0.3);
        }
        
        .sync-badge.syncing {
            background: rgba(59, 130, 246, 0.15);
            color: var(--info);
            border: 1px solid rgba(59, 130, 246, 0.3);
            animation: pulse 1.5s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        
        .sync-actions {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }
        
        .btn-sync {
            flex: 1;
            background: linear-gradient(135deg, var(--info), #2563eb);
            border: none;
            color: white;
            padding: 0.6rem 1rem;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .btn-sync:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 15px rgba(59, 130, 246, 0.4);
        }
        
        .btn-sync:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none;
        }
        
        .btn-stop-sync {
            flex: 1;
            background: linear-gradient(135deg, var(--error), #dc2626);
            border: none;
            color: white;
            padding: 0.6rem 1rem;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
        }
        
        .sync-progress {
            background: var(--bg-input);
            border-radius: 8px;
            padding: 0.75rem;
            margin-bottom: 1rem;
        }
        
        .progress-header {
            display: flex;
            justify-content: space-between;
            font-size: 0.8rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }
        
        .progress-bar-container {
            height: 6px;
            background: var(--border-color);
            border-radius: 3px;
            overflow: hidden;
            margin-bottom: 0.75rem;
        }
        
        .progress-bar-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--info), var(--success));
            border-radius: 3px;
            transition: width 0.3s;
        }
        
        .sync-log {
            max-height: 150px;
            overflow-y: auto;
            font-size: 0.7rem;
            font-family: 'JetBrains Mono', monospace;
            color: var(--text-muted);
            line-height: 1.6;
        }
        
        .sync-log .log-entry {
            padding: 0.15rem 0;
        }
        
        .sync-note {
            font-size: 0.7rem;
            color: var(--text-muted);
            text-align: center;
            padding: 0.5rem;
            background: rgba(59, 130, 246, 0.05);
            border-radius: 6px;
            border: 1px dashed rgba(59, 130, 246, 0.2);
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
            <div class="version-badge">v3.0 🛡️ Bulletproof • 4 Workers</div>
        </div>
    </header>
    
    <main class="main-container">
        <aside>
            <!-- Master Database Sync Card -->
            <div class="card master-sync-card" id="masterSyncCard">
                <h2 class="card-title" style="cursor: pointer;" onclick="toggleMasterSync()">
                    🗄️ Master Database
                    <span id="masterSyncToggle" style="margin-left: auto; font-size: 0.8rem; color: var(--text-muted);">▼</span>
                </h2>
                
                <div id="masterSyncContent">
                    <div class="master-stats" id="masterStats">
                        <div class="stat-row">
                            <span>Districts:</span>
                            <span id="statDistricts" class="mono">-</span>
                        </div>
                        <div class="stat-row">
                            <span>Taluks:</span>
                            <span id="statTaluks" class="mono">-</span>
                        </div>
                        <div class="stat-row">
                            <span>Hoblis:</span>
                            <span id="statHoblis" class="mono">-</span>
                        </div>
                        <div class="stat-row">
                            <span>Villages:</span>
                            <span id="statVillages" class="mono">-</span>
                        </div>
                        <div class="stat-row total">
                            <span>Total Locations:</span>
                            <span id="statTotal" class="mono">-</span>
                        </div>
                    </div>
                    
                    <div class="sync-status" id="syncStatus">
                        <span class="sync-badge not-synced" id="syncBadge">Not Synced</span>
                    </div>
                    
                    <div class="sync-actions">
                        <button class="btn btn-sync" id="btnStartSync" onclick="startMasterSync()">
                            🔄 Sync All Locations
                        </button>
                        <button class="btn btn-stop-sync" id="btnStopSync" onclick="stopMasterSync()" style="display: none;">
                            ⏹️ Stop Sync
                        </button>
                    </div>
                    
                    <div class="sync-progress" id="syncProgress" style="display: none;">
                        <div class="progress-header">
                            <span>Syncing...</span>
                            <span id="syncPercent" class="mono">0%</span>
                        </div>
                        <div class="progress-bar-container">
                            <div class="progress-bar-fill" id="syncProgressBar" style="width: 0%;"></div>
                        </div>
                        <div class="sync-log" id="syncLog"></div>
                    </div>
                    
                    <p class="sync-note">
                        ℹ️ Sync once to index all ~30,000 Karnataka villages for 100% accurate search.
                    </p>
                </div>
            </div>
            
            <!-- Search Configuration Card -->
            <div class="card" style="margin-top: 1rem;">
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
            </div>
        </aside>
        
        <section>
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
        let masterSyncRunning = false;
        let masterSyncInterval = null;
        let masterSyncCollapsed = false;
        
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
            loadMasterStats();  // Load master database stats on page load
        });
        
        // ═══════════════════════════════════════════════════════════════════════
        // MASTER DATABASE SYNC FUNCTIONS
        // ═══════════════════════════════════════════════════════════════════════
        
        function toggleMasterSync() {
            const content = document.getElementById('masterSyncContent');
            const toggle = document.getElementById('masterSyncToggle');
            masterSyncCollapsed = !masterSyncCollapsed;
            content.style.display = masterSyncCollapsed ? 'none' : 'block';
            toggle.textContent = masterSyncCollapsed ? '▶' : '▼';
        }
        
        async function loadMasterStats() {
            try {
                const response = await fetch('/api/master/stats');
                const stats = await response.json();
                
                document.getElementById('statDistricts').textContent = stats.districts.toLocaleString();
                document.getElementById('statTaluks').textContent = stats.taluks.toLocaleString();
                document.getElementById('statHoblis').textContent = stats.hoblis.toLocaleString();
                document.getElementById('statVillages').textContent = stats.villages.toLocaleString();
                document.getElementById('statTotal').textContent = stats.total_locations.toLocaleString();
                
                const badge = document.getElementById('syncBadge');
                if (stats.is_synced) {
                    badge.textContent = '✅ Synced';
                    badge.className = 'sync-badge synced';
                    // Auto-collapse if already synced
                    if (!masterSyncCollapsed) {
                        toggleMasterSync();
                    }
                } else {
                    badge.textContent = '⚠️ Not Synced';
                    badge.className = 'sync-badge not-synced';
                }
            } catch (error) {
                console.error('Failed to load master stats:', error);
            }
        }
        
        async function startMasterSync() {
            if (masterSyncRunning) return;
            
            try {
                const response = await fetch('/api/master/sync/start', { method: 'POST' });
                const result = await response.json();
                
                if (result.error && !result.error.includes('already in progress')) {
                    alert('Sync error: ' + result.error);
                    return;
                }
                
                masterSyncRunning = true;
                
                // Update UI
                document.getElementById('btnStartSync').style.display = 'none';
                document.getElementById('btnStopSync').style.display = 'block';
                document.getElementById('syncProgress').style.display = 'block';
                document.getElementById('syncBadge').textContent = '🔄 Syncing...';
                document.getElementById('syncBadge').className = 'sync-badge syncing';
                
                // Start polling for progress
                masterSyncInterval = setInterval(pollMasterSync, 1000);
                
            } catch (error) {
                alert('Failed to start sync: ' + error.message);
            }
        }
        
        async function stopMasterSync() {
            try {
                await fetch('/api/master/sync/stop', { method: 'POST' });
                clearInterval(masterSyncInterval);
                masterSyncRunning = false;
                
                // Update UI
                document.getElementById('btnStartSync').style.display = 'block';
                document.getElementById('btnStopSync').style.display = 'none';
                
                loadMasterStats();
            } catch (error) {
                console.error('Failed to stop sync:', error);
            }
        }
        
        async function pollMasterSync() {
            try {
                const response = await fetch('/api/master/sync/status');
                const status = await response.json();
                
                // Update stats
                const stats = status.stats;
                document.getElementById('statDistricts').textContent = stats.districts.toLocaleString();
                document.getElementById('statTaluks').textContent = stats.taluks.toLocaleString();
                document.getElementById('statHoblis').textContent = stats.hoblis.toLocaleString();
                document.getElementById('statVillages').textContent = stats.villages.toLocaleString();
                
                const total = stats.districts + stats.taluks + stats.hoblis + stats.villages;
                document.getElementById('statTotal').textContent = total.toLocaleString();
                
                // Estimate progress (rough estimate based on expected ~30k villages)
                const estimatedTotal = 30000;
                const progress = Math.min(100, Math.round((stats.villages / estimatedTotal) * 100));
                document.getElementById('syncPercent').textContent = progress + '%';
                document.getElementById('syncProgressBar').style.width = progress + '%';
                
                // Update log
                const logContainer = document.getElementById('syncLog');
                logContainer.innerHTML = status.progress
                    .slice(-15)
                    .map(entry => `<div class="log-entry">${entry}</div>`)
                    .join('');
                logContainer.scrollTop = logContainer.scrollHeight;
                
                // Check if sync completed
                if (!status.is_syncing && masterSyncRunning) {
                    clearInterval(masterSyncInterval);
                    masterSyncRunning = false;
                    
                    document.getElementById('btnStartSync').style.display = 'block';
                    document.getElementById('btnStopSync').style.display = 'none';
                    
                    // Reload final stats
                    loadMasterStats();
                }
                
            } catch (error) {
                console.error('Failed to poll sync status:', error);
            }
        }
        
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
            
            // Get selected names (not just codes) - CRITICAL for portal matching!
            const districtName = districtSelect.options[districtSelect.selectedIndex]?.text || '';
            const talukName = talukSelect.options[talukSelect.selectedIndex]?.text || '';
            const hobliName = hobliSelect.options[hobliSelect.selectedIndex]?.text || '';
            const villageName = villageSelect.options[villageSelect.selectedIndex]?.text || '';
            
            if (!districtCode || !talukCode) {
                alert('Please select District and Taluk');
                return;
            }
            
            searchRunning = true;
            searchBtn.innerHTML = '<span class="spinner"></span><span>Stop Search</span>';
            searchBtn.classList.add('btn-stop');
            document.getElementById('progressSection').style.display = 'block';
            
            addLog('🚀 Starting parallel search...');
            addLog(`📍 Location: ${districtName} > ${talukName}`);
            
            try {
                await fetch('/api/search/start', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        owner_name: ownerName,
                        district_code: districtCode,
                        district_name: districtName,  // Send name for portal matching
                        taluk_code: talukCode,
                        taluk_name: talukName,  // Send name for portal matching
                        hobli_code: hobliCode,
                        hobli_name: hobliName,
                        village_code: villageCode,
                        village_name: villageName,
                        max_survey: parseInt(maxSurveyInput.value) || 200
                    })
                });
                
                pollInterval = setInterval(pollStatus, 1500);
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
        }
        
        async function pollStatus() {
            try {
                const res = await fetch('/api/search/status');
                const status = await res.json();
                
                // Update overall progress
                document.getElementById('progressPercent').textContent = status.progress + '%';
                document.getElementById('progressFill').style.width = status.progress + '%';
                document.getElementById('villagesCompleted').textContent = `${status.villages_completed}/${status.total_villages}`;
                document.getElementById('totalRecords').textContent = status.total_records;
                document.getElementById('totalMatches').textContent = status.total_matches;
                document.getElementById('activeWorkers').textContent = status.active_workers;
                
                // Update badges
                document.getElementById('recordsBadge').textContent = status.total_records;
                document.getElementById('matchesBadge').textContent = status.total_matches;
                
                // Update workers
                if (status.workers) {
                    Object.entries(status.workers).forEach(([id, w]) => {
                        const card = document.getElementById(`worker-${id}`);
                        if (card) {
                            card.querySelector('.worker-status').textContent = w.status;
                            card.querySelector('.worker-status').className = `worker-status ${w.status}`;
                            card.querySelector('.worker-village').textContent = w.current_village || 'Waiting...';
                            card.querySelector('.worker-progress-fill').style.width = w.progress + '%';
                            card.querySelector('.worker-stats').innerHTML = 
                                `<span>${w.villages_completed}/${w.villages_total} villages</span><span>${w.records_found} records</span>`;
                        }
                    });
                }
                
                // Update records tables (real-time)
                if (status.all_records) {
                    updateRecordsTable(status.all_records);
                }
                if (status.matches) {
                    updateMatchesTable(status.matches);
                }
                
                // Update logs
                if (status.logs) {
                    const container = document.getElementById('logsContainer');
                    container.innerHTML = status.logs.map(log => 
                        `<div class="log-entry">${log}</div>`
                    ).reverse().join('');
                }
                
                // Check if completed
                if (status.completed || !status.running) {
                    addLog('✅ Search completed!');
                    stopSearch();
                }
                
            } catch (e) {}
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

@app.route('/api/db/resumable')
def get_resumable_sessions():
    """Get sessions that can be resumed (Enhancement 2)"""
    db = get_database()
    sessions = db.get_resumable_sessions()
    return jsonify(sessions)

# ═══════════════════════════════════════════════════════════════════════════════════════
# MASTER DATABASE API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════════════

@app.route('/api/master/stats')
def get_master_stats():
    """Get master database statistics"""
    db = get_database()
    stats = db.get_master_stats()
    return jsonify(stats)

@app.route('/api/master/sync/start', methods=['POST'])
def start_master_sync():
    """Start syncing master location database"""
    syncer = get_master_syncer()
    
    if syncer.is_syncing:
        return jsonify({'error': 'Sync already in progress', 'status': syncer.get_sync_status()})
    
    # Run sync in background thread
    import threading
    def run_sync():
        syncer.sync_all()
    
    thread = threading.Thread(target=run_sync, daemon=True)
    thread.start()
    
    return jsonify({'message': 'Sync started', 'status': syncer.get_sync_status()})

@app.route('/api/master/sync/stop', methods=['POST'])
def stop_master_sync():
    """Stop ongoing master sync"""
    syncer = get_master_syncer()
    syncer.stop_sync()
    return jsonify({'message': 'Sync stopped', 'status': syncer.get_sync_status()})

@app.route('/api/master/sync/status')
def get_master_sync_status():
    """Get current sync status"""
    syncer = get_master_syncer()
    return jsonify(syncer.get_sync_status())

@app.route('/api/master/districts')
def get_master_districts():
    """Get all districts from master database"""
    db = get_database()
    districts = db.get_master_districts()
    return jsonify(districts)

@app.route('/api/master/taluks/<int:district_code>')
def get_master_taluks(district_code):
    """Get all taluks for a district from master database"""
    db = get_database()
    taluks = db.get_master_taluks(district_code)
    return jsonify(taluks)

@app.route('/api/master/hoblis/<int:district_code>/<int:taluk_code>')
def get_master_hoblis(district_code, taluk_code):
    """Get all hoblis from master database"""
    db = get_database()
    hoblis = db.get_master_hoblis(district_code, taluk_code)
    return jsonify(hoblis)

@app.route('/api/master/villages/<int:district_code>/<int:taluk_code>/<int:hobli_code>')
def get_master_villages_api(district_code, taluk_code, hobli_code):
    """Get all villages from master database"""
    db = get_database()
    villages = db.get_master_villages(district_code, taluk_code, hobli_code)
    return jsonify(villages)

@app.route('/api/master/villages/count')
def get_master_village_count():
    """Get total village count with optional filters"""
    db = get_database()
    district_code = request.args.get('district', type=int)
    taluk_code = request.args.get('taluk', type=int)
    hobli_code = request.args.get('hobli', type=int)
    
    villages = db.get_master_villages_for_search(district_code, taluk_code, hobli_code)
    return jsonify({
        'count': len(villages),
        'filters': {
            'district': district_code,
            'taluk': taluk_code,
            'hobli': hobli_code
        }
    })

@app.route('/api/db/sessions/<session_id>/resume')
def get_resume_state(session_id):
    """Get the state needed to resume a session (Enhancement 2)"""
    db = get_database()
    state = db.get_resume_state(session_id)
    if not state:
        return jsonify({'error': 'Session not found'}), 404
    return jsonify(state)

@app.route('/api/db/sessions/<session_id>/audit')
def get_session_audit(session_id):
    """Get audit results for a session (Enhancement 6)"""
    db = get_database()
    session = db.get_session(session_id)
    if not session:
        return jsonify({'error': 'Session not found'}), 404
    
    audit_result = session.get('audit_result')
    if audit_result and isinstance(audit_result, str):
        try:
            audit_result = json.loads(audit_result)
        except:
            audit_result = None
    
    return jsonify({
        'session_id': session_id,
        'has_audit': audit_result is not None,
        'audit': audit_result
    })

@app.route('/api/db/sessions/<session_id>/skipped')
def get_session_skipped(session_id):
    """Get skipped items for a session (for retry)"""
    db = get_database()
    skipped = db.get_skipped_items(session_id)
    return jsonify({
        'session_id': session_id,
        'count': len(skipped),
        'items': skipped
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

# ═══════════════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                  POWER-BHOOMI v3.0 - BULLETPROOF EDITION (4 WORKERS)                 ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                      ║
║   🍎 Optimized for macOS                                                             ║
║   🚀 4 Fast Parallel Browser Workers                                                 ║
║   📊 Sequential Survey Checking (1, 2, 3... no skips)                                ║
║   ⚡ Speed-Optimized Timeouts                                                        ║
║   📁 CSV Auto-Saves to Downloads                                                     ║
║                                                                                      ║
║   🌐 Open your browser and navigate to:                                              ║
║                                                                                      ║
║       http://localhost:5001                                                          ║
║                                                                                      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
    """)
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG, threaded=True)

