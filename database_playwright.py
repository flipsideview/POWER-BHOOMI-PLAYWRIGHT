#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                  POWER-BHOOMI - Database Manager                             ║
║                  Thread-safe SQLite with WAL mode                            ║
╚══════════════════════════════════════════════════════════════════════════════╝

Purpose:
  - Process-safe SQLite operations (WAL mode)
  - Session management and progress tracking
  - Real-time record persistence
  - Export to CSV
  - Resume capability

Author: POWER-BHOOMI Team
Version: 4.0.0
"""

import sqlite3
import json
import logging
import threading
import csv
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger('DatabaseManager')


class DatabaseManager:
    """
    Thread-safe and process-safe SQLite database manager.
    
    Features:
    - WAL mode for concurrent reads/writes
    - Real-time record persistence
    - Session tracking
    - Village progress tracking
    - CSV export
    - Resume after crash
    
    Database Schema:
    - search_sessions: Track each search operation
    - land_records: All records found (with match flag)
    - village_progress: Track which villages are done
    - task_checkpoints: Granular task completion tracking
    """
    
    DB_VERSION = 4
    
    def __init__(self, db_path: str = None):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        if db_path is None:
            db_path = Path.home() / 'Documents' / 'POWER-BHOOMI' / 'bhoomi_data.db'
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._lock = threading.RLock()
        
        # Initialize database schema
        self._init_database()
        
        logger.info(f"Database initialized: {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        """Get database connection (thread-safe)"""
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row  # Return rows as dicts
        conn.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_database(self):
        """Initialize database schema"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Search Sessions Table
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
                        status TEXT DEFAULT 'running',
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        total_villages INTEGER DEFAULT 0,
                        villages_completed INTEGER DEFAULT 0,
                        total_records INTEGER DEFAULT 0,
                        total_matches INTEGER DEFAULT 0,
                        notes TEXT
                    )
                ''')
                
                # Land Records Table
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
                        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id),
                        UNIQUE(session_id, village, survey_no, surnoc, hissa, period, owner_name)
                    )
                ''')
                
                # Village Progress Table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS village_progress (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        village_code TEXT NOT NULL,
                        village_name TEXT NOT NULL,
                        hobli_code TEXT,
                        hobli_name TEXT,
                        status TEXT DEFAULT 'pending',
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
                
                # Task Checkpoints Table (NEW - for granular resume)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS task_checkpoints (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        task_id TEXT NOT NULL,
                        village_code TEXT NOT NULL,
                        survey_no INTEGER NOT NULL,
                        status TEXT DEFAULT 'pending',
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        worker_id INTEGER,
                        retry_count INTEGER DEFAULT 0,
                        error_message TEXT,
                        FOREIGN KEY (session_id) REFERENCES search_sessions(session_id),
                        UNIQUE(session_id, task_id)
                    )
                ''')
                
                # Indexes for performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_session ON land_records(session_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_village ON land_records(village)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_records_match ON land_records(is_match)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_progress_session ON village_progress(session_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_checkpoints_session ON task_checkpoints(session_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_checkpoints_status ON task_checkpoints(status)')
                
                # Database metadata
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS db_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                ''')
                cursor.execute('INSERT OR REPLACE INTO db_meta (key, value) VALUES (?, ?)', 
                              ('version', str(self.DB_VERSION)))
                
                conn.commit()
    
    # ═══════════════════════════════════════════════════════════════════════
    # SESSION MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════
    
    def create_session(self, params: dict) -> str:
        """Create new search session"""
        import uuid
        session_id = f"search_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        with self._lock:
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
                conn.commit()
        
        logger.info(f"📝 Created session: {session_id}")
        return session_id
    
    def update_session_status(self, session_id: str, status: str, **kwargs):
        """Update session status"""
        with self._lock:
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
                conn.commit()
    
    def get_session(self, session_id: str) -> Optional[Dict]:
        """Get session details"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM search_sessions WHERE session_id = ?', (session_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    # ═══════════════════════════════════════════════════════════════════════
    # VILLAGE MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════
    
    def register_villages(self, session_id: str, villages: List[Tuple]):
        """Register villages for tracking"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for village_code, village_name, hobli_code, hobli_name in villages:
                    cursor.execute('''
                        INSERT OR IGNORE INTO village_progress 
                        (session_id, village_code, village_name, hobli_code, hobli_name)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (session_id, village_code, village_name, hobli_code, hobli_name))
                conn.commit()
    
    def get_pending_villages(self, session_id: str) -> List[Dict]:
        """Get villages that need to be processed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM village_progress 
                WHERE session_id = ? AND status IN ('pending', 'in_progress')
                ORDER BY village_name
            ''', (session_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    # ═══════════════════════════════════════════════════════════════════════
    # TASK CHECKPOINTS
    # ═══════════════════════════════════════════════════════════════════════
    
    def mark_task_started(self, session_id: str, task_id: str, village_code: str, 
                          survey_no: int, worker_id: int):
        """Mark task as started"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO task_checkpoints 
                    (session_id, task_id, village_code, survey_no, status, started_at, worker_id)
                    VALUES (?, ?, ?, ?, 'processing', CURRENT_TIMESTAMP, ?)
                ''', (session_id, task_id, village_code, survey_no, worker_id))
                conn.commit()
    
    def mark_task_completed(self, session_id: str, task_id: str):
        """Mark task as completed"""
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE task_checkpoints 
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                    WHERE session_id = ? AND task_id = ?
                ''', (session_id, task_id))
                conn.commit()
    
    def get_completed_tasks(self, session_id: str) -> List[str]:
        """Get list of completed task IDs"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT task_id FROM task_checkpoints
                WHERE session_id = ? AND status = 'completed'
            ''', (session_id,))
            return [row[0] for row in cursor.fetchall()]
    
    # ═══════════════════════════════════════════════════════════════════════
    # STATISTICS
    # ═══════════════════════════════════════════════════════════════════════
    
    def get_session_stats(self, session_id: str) -> Dict:
        """Get session statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Total records
            cursor.execute('SELECT COUNT(*) FROM land_records WHERE session_id = ?', (session_id,))
            total_records = cursor.fetchone()[0]
            
            # Total matches
            cursor.execute('SELECT COUNT(*) FROM land_records WHERE session_id = ? AND is_match = 1', (session_id,))
            total_matches = cursor.fetchone()[0]
            
            # Villages completed
            cursor.execute('SELECT COUNT(*) FROM village_progress WHERE session_id = ? AND status = "completed"', (session_id,))
            villages_completed = cursor.fetchone()[0]
            
            # Total villages
            cursor.execute('SELECT COUNT(*) FROM village_progress WHERE session_id = ?', (session_id,))
            total_villages = cursor.fetchone()[0]
            
            return {
                'total_records': total_records,
                'total_matches': total_matches,
                'villages_completed': villages_completed,
                'total_villages': total_villages,
                'completion_percentage': (villages_completed / max(total_villages, 1)) * 100
            }
    
    # ═══════════════════════════════════════════════════════════════════════
    # CSV EXPORT
    # ═══════════════════════════════════════════════════════════════════════
    
    def export_to_csv(self, session_id: str, output_path: str, matches_only: bool = False):
        """Export records to CSV"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = 'SELECT * FROM land_records WHERE session_id = ?'
            if matches_only:
                query += ' AND is_match = 1'
            query += ' ORDER BY village, survey_no, surnoc, hissa'
            
            cursor.execute(query, (session_id,))
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning(f"No records to export for session {session_id}")
                return
            
            # Write to CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                for row in rows:
                    writer.writerow(dict(row))
            
            logger.info(f"Exported {len(rows)} records to {output_path}")


if __name__ == '__main__':
    # Test database
    logging.basicConfig(level=logging.INFO)
    
    db = DatabaseManager('/tmp/test_bhoomi.db')
    
    # Create test session
    session_id = db.create_session({
        'owner_name': 'Test Owner',
        'district_name': 'Bangalore Urban',
        'taluk_name': 'Bangalore North',
        'hobli_name': 'Yelahanka',
        'village_name': 'Test Village',
        'max_survey': 10
    })
    
    print(f"Created session: {session_id}")
    
    # Get session
    session = db.get_session(session_id)
    print(f"Session: {session}")
    
    # Get stats
    stats = db.get_session_stats(session_id)
    print(f"Stats: {stats}")
    
    print("\n✅ Database test complete!")





