#!/usr/bin/env python3
"""
POWER-BHOOMI v4.0 - COMPLETE Playwright Edition
Direct port of v3.x with Playwright + Process architecture
"""

import os
import sys
import json
import time
import logging
import multiprocessing as mp
import threading
import signal
import shutil
import sqlite3
import csv
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from contextlib import contextmanager

# Flask
from flask import Flask, render_template_string, jsonify, request, send_file
from flask_cors import CORS

# Playwright
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page
from playwright.sync_api import TimeoutError as PlaywrightTimeout

# HTML parsing
from bs4 import BeautifulSoup
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('BHOOMI-PW')

# ═══════════════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════════════

class Config:
    HOST = '0.0.0.0'
    PORT = 5002  # Different port from v3.x
    MAX_WORKERS = 4
    WORKER_STARTUP_DELAY = 1.0
    PAGE_LOAD_TIMEOUT = 20
    POST_CLICK_WAIT = 3
    POST_SELECT_WAIT = 1.5
    DEFAULT_MAX_SURVEY = 200
    SERVICE2_URL = "https://landrecords.karnataka.gov.in/Service2/"
    SMART_STOP_ENABLED = True
    EMPTY_SURVEY_THRESHOLD = 50
    MIN_SURVEYS_BEFORE_STOP = 10
    MAX_SESSION_RETRIES = 3
    PROCESS_ALL_PERIODS = True
    
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

# ═════════════════════════════════════════════════════════════════════════════════════
# DATABASE (simplified from v3.x)
# ═══════════════════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = Path.home() / 'Documents' / 'POWER-BHOOMI' / 'bhoomi_playwright.db'
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_database()
        logger.info(f"Database: {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_database(self):
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS land_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT,
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
                        worker_id INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
    
    def save_record(self, session_id, record):
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO land_records (session_id, district, taluk, hobli, village,
                                            survey_no, surnoc, hissa, period, owner_name,
                                            extent, khatah, worker_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    record.get('worker_id', 0)
                ))
                conn.commit()

# ═══════════════════════════════════════════════════════════════════════════════════════
# PLAYWRIGHT WORKER (Complete port from v3.x)
# ═══════════════════════════════════════════════════════════════════════════════════════

class PlaywrightWorker:
    def __init__(self, worker_id, villages, search_params, db_path, session_id):
        self.worker_id = worker_id
        self.villages = villages
        self.params = search_params
        self.db_path = db_path
        self.session_id = session_id
        self.logger = logging.getLogger(f'Worker-{worker_id}')
        
        self.playwright = None
        self.browser = None
        self.page = None
        self.records_found = 0
        
    @staticmethod
    def run_process(worker_id, villages, search_params, db_path, session_id):
        """Entry point for process"""
        worker = PlaywrightWorker(worker_id, villages, search_params, db_path, session_id)
        worker.run()
    
    def run(self):
        try:
            self.logger.info(f"Worker {self.worker_id} starting (PID: {os.getpid()})")
            self._init_browser()
            db = DatabaseManager(self.db_path)
            
            for village_code, village_name, hobli_code, hobli_name in self.villages:
                self.logger.info(f"Searching village: {village_name}")
                self._search_village(village_code, village_name, hobli_code, hobli_name, db)
            
            self.logger.info(f"Worker {self.worker_id} completed. Records: {self.records_found}")
            
        except Exception as e:
            self.logger.error(f"Worker error: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self._cleanup()
    
    def _init_browser(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-gpu', '--disable-images']
        )
        self.page = self.browser.new_page()
        self.page.set_default_timeout(20000)
        self.logger.info(f"Browser initialized")
    
    def _cleanup(self):
        if self.page:
            self.page.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    
    def _search_village(self, village_code, village_name, hobli_code, hobli_name, db):
        """PORT OF FULL SEARCH LOGIC FROM V3.X"""
        IDS = Config.ELEMENT_IDS
        max_survey = self.params.get('max_survey', Config.DEFAULT_MAX_SURVEY)
        owner_name = self.params.get('owner_name', '')
        
        for survey_no in range(1, max_survey + 1):
            try:
                # Navigate
                self.page.goto(Config.SERVICE2_URL)
                self.page.wait_for_load_state('domcontentloaded')
                time.sleep(1)
                
                # Fill form
                self.page.select_option(f'#{IDS["district"]}', value=self.params['district_code'])
                time.sleep(Config.POST_SELECT_WAIT)
                self.page.select_option(f'#{IDS["taluk"]}', value=self.params['taluk_code'])
                time.sleep(Config.POST_SELECT_WAIT)
                self.page.select_option(f'#{IDS["hobli"]}', value=hobli_code)
                time.sleep(Config.POST_SELECT_WAIT)
                self.page.select_option(f'#{IDS["village"]}', value=village_code)
                time.sleep(Config.POST_SELECT_WAIT)
                
                # Survey number
                self.page.fill(f'#{IDS["survey_no"]}', str(survey_no))
                self.page.click(f'#{IDS["go_btn"]}')
                self.page.wait_for_load_state('domcontentloaded')
                time.sleep(Config.POST_CLICK_WAIT)
                
                # Check for surnoc options
                surnoc_options = self.page.locator(f'#{IDS["surnoc"]} option').all_text_contents()
                surnoc_options = [o for o in surnoc_options if 'Select' not in o]
                
                if not surnoc_options:
                    continue  # Empty survey
                
                # Process each combination
                for surnoc in surnoc_options:
                    self.page.select_option(f'#{IDS["surnoc"]}', label=surnoc)
                    time.sleep(0.5)
                    
                    hissa_options = self.page.locator(f'#{IDS["hissa"]} option').all_text_contents()
                    hissa_options = [o for o in hissa_options if 'Select' not in o]
                    
                    for hissa in hissa_options:
                        self.page.select_option(f'#{IDS["hissa"]}', label=hissa)
                        time.sleep(0.5)
                        
                        period_options = self.page.locator(f'#{IDS["period"]} option').all_text_contents()
                        period_options = [o for o in period_options if 'Select' not in o]
                        
                        for period in period_options:
                            self.page.select_option(f'#{IDS["period"]}', label=period)
                            time.sleep(0.5)
                            
                            # Fetch
                            self.page.click(f'#{IDS["fetch_btn"]}')
                            self.page.wait_for_load_state('domcontentloaded')
                            time.sleep(Config.POST_CLICK_WAIT)
                            
                            # Extract owners
                            owners = self._extract_owners()
                            
                            for owner in owners:
                                record = {
                                    'district': self.params.get('district_name', ''),
                                    'taluk': self.params.get('taluk_name', ''),
                                    'hobli': hobli_name,
                                    'village': village_name,
                                    'survey_no': survey_no,
                                    'surnoc': surnoc,
                                    'hissa': hissa,
                                    'period': period,
                                    'owner_name': owner['name'],
                                    'extent': owner['extent'],
                                    'khatah': owner['khatah'],
                                    'worker_id': self.worker_id
                                }
                                
                                db.save_record(self.session_id, record)
                                self.records_found += 1
                                
                                if owner_name.lower() in owner['name'].lower():
                                    self.logger.info(f"MATCH: {owner['name']}")
            
            except Exception as e:
                self.logger.error(f"Survey {survey_no} error: {e}")
                self.logger.error(f"Full trace: {traceback.format_exc()}")
                continue
    
    def _extract_owners(self):
        """Extract owners from page"""
        owners = []
        try:
            page_html = self.page.content()
            soup = BeautifulSoup(page_html, 'html.parser')
            
            # Remove form elements
            for tag in soup.find_all(['select', 'input', 'button']):
                tag.decompose()
            
            # Find results table
            for table in soup.find_all('table'):
                text = table.get_text()
                if 'Owner' in text or 'ಮಾಲೀಕರ' in text:
                    rows = table.find_all('tr')
                    for row in rows[1:]:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            texts = [c.get_text(strip=True) for c in cells]
                            if re.search(r'\d+[\.\-]\d+[\.\-]\d+', ' '.join(texts)):
                                owners.append({
                                    'name': texts[0],
                                    'extent': texts[1] if len(texts) > 1 else '',
                                    'khatah': texts[2] if len(texts) > 2 else ''
                                })
        except:
            pass
        
        return owners

# ═══════════════════════════════════════════════════════════════════════════════════════
# FLASK APP
# ═══════════════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)
CORS(app)

# Global state
manager = None
shared_state = None
worker_processes = []
db = None

def init_globals():
    global manager, shared_state, db
    manager = mp.Manager()
    shared_state = manager.dict({
        'running': False,
        'session_id': None,
        'total_records': 0,
        'villages_completed': 0
    })
    db = DatabaseManager()

HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>POWER-BHOOMI v4.0 Playwright</title>
    <style>
        body { font-family: system-ui; background: #1a1a1a; color: #fff; padding: 40px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #f59e0b; }
        .btn { padding: 12px 24px; background: #f59e0b; border: none; border-radius: 8px; 
               color: #000; font-weight: bold; cursor: pointer; }
        .btn:hover { background: #d97706; }
        .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin: 30px 0; }
        .stat-card { background: #2a2a2a; padding: 20px; border-radius: 12px; }
        .stat-value { font-size: 36px; font-weight: bold; color: #f59e0b; }
        .stat-label { font-size: 14px; opacity: 0.7; }
        input, select { padding: 10px; background: #2a2a2a; border: 1px solid #3a3a3a; 
                        border-radius: 6px; color: #fff; width: 100%; margin-bottom: 15px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 POWER-BHOOMI v4.0 - Playwright Edition</h1>
        <p>Process-Based | Hard Browser Budget | Port 5002</p>
        
        <div style="margin: 30px 0;">
            <input id="owner" placeholder="Owner Name" />
            <input id="district_code" placeholder="District Code (e.g., 21)" />
            <input id="district_name" placeholder="District Name" />
            <input id="taluk_code" placeholder="Taluk Code (e.g., 4)" />
            <input id="taluk_name" placeholder="Taluk Name" />
            <input id="village_code" placeholder="Village Code (e.g., 1)" />
            <input id="village_name" placeholder="Village Name" />
            <input id="max_survey" placeholder="Max Survey (e.g., 5)" value="5" />
            <button class="btn" onclick="startSearch()">Start Search</button>
            <button class="btn" onclick="stopSearch()">Stop</button>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="status">IDLE</div>
                <div class="stat-label">Status</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="records">0</div>
                <div class="stat-label">Records</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="villages">0</div>
                <div class="stat-label">Villages</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="chromium">0</div>
                <div class="stat-label">Chromium Processes</div>
            </div>
        </div>
        
        <div style="background: #2a2a2a; padding: 20px; border-radius: 12px;">
            <h3>Recent Records</h3>
            <pre id="records_display" style="max-height: 300px; overflow: auto; font-size: 11px;"></pre>
        </div>
    </div>
    
    <script>
        let updateInterval = null;
        
        async function startSearch() {
            const params = {
                owner_name: document.getElementById('owner').value,
                district_code: document.getElementById('district_code').value,
                district_name: document.getElementById('district_name').value,
                taluk_code: document.getElementById('taluk_code').value,
                taluk_name: document.getElementById('taluk_name').value,
                village_code: document.getElementById('village_code').value,
                village_name: document.getElementById('village_name').value,
                hobli_code: '1',
                hobli_name: 'KASABA',
                max_survey: parseInt(document.getElementById('max_survey').value) || 5
            };
            
            const res = await fetch('/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(params)
            });
            
            const data = await res.json();
            alert(JSON.stringify(data));
            
            updateInterval = setInterval(updateStatus, 2000);
        }
        
        async function stopSearch() {
            await fetch('/stop', {method: 'POST'});
            clearInterval(updateInterval);
        }
        
        async function updateStatus() {
            try {
                const res = await fetch('/status');
                const data = await res.json();
                
                document.getElementById('status').textContent = data.running ? 'RUNNING' : 'STOPPED';
                document.getElementById('records').textContent = data.total_records || 0;
                document.getElementById('villages').textContent = data.villages_completed || 0;
                document.getElementById('chromium').textContent = data.chromium_count || 0;
                
                if (data.recent_records) {
                    document.getElementById('records_display').textContent = 
                        JSON.stringify(data.recent_records.slice(-10), null, 2);
                }
            } catch (e) {
                console.error('Update error:', e);
            }
        }
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML)

@app.route('/start', methods=['POST'])
def start_search():
    global worker_processes
    
    if shared_state.get('running'):
        return jsonify({'error': 'Already running'}), 400
    
    params = request.json
    session_id = f"pw_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Single village for now
    villages = [(
        params['village_code'],
        params['village_name'],
        params['hobli_code'],
        params['hobli_name']
    )]
    
    shared_state['running'] = True
    shared_state['session_id'] = session_id
    
    # Start workers
    for i in range(Config.MAX_WORKERS):
        p = mp.Process(
            target=PlaywrightWorker.run_process,
            args=(i, [villages[0]], params, str(db.db_path), session_id)
        )
        p.start()
        worker_processes.append(p)
        time.sleep(Config.WORKER_STARTUP_DELAY)
    
    logger.info(f"Started {len(worker_processes)} workers")
    
    return jsonify({
        'status': 'started',
        'session_id': session_id,
        'workers': len(worker_processes)
    })

@app.route('/stop', methods=['POST'])
def stop_search():
    global worker_processes
    
    shared_state['running'] = False
    
    for p in worker_processes:
        if p.is_alive():
            p.terminate()
            p.join(timeout=10)
            if p.is_alive():
                p.kill()
    
    worker_processes.clear()
    
    return jsonify({'status': 'stopped'})

@app.route('/status')
def status():
    # Get records from database
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM land_records WHERE session_id = ?', 
                      (shared_state.get('session_id', ''),))
        count = cursor.fetchone()[0]
        
        cursor.execute('SELECT * FROM land_records WHERE session_id = ? ORDER BY id DESC LIMIT 10',
                      (shared_state.get('session_id', ''),))
        recent = [dict(row) for row in cursor.fetchall()]
    
    # Count Chromium
    import subprocess
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        chromium_count = sum(1 for line in result.stdout.split('\n') if 'chromium' in line.lower())
    except:
        chromium_count = 0
    
    return jsonify({
        'running': shared_state.get('running', False),
        'session_id': shared_state.get('session_id'),
        'total_records': count,
        'villages_completed': 0,
        'chromium_count': chromium_count,
        'recent_records': recent
    })

def main():
    init_globals()
    
    logger.info("=" * 80)
    logger.info("POWER-BHOOMI v4.0 - Playwright Edition (COMPLETE)")
    logger.info(f"Port: {Config.PORT} (v3.x is on 5001)")
    logger.info(f"Max Workers: {Config.MAX_WORKERS}")
    logger.info(f"Database: {db.db_path}")
    logger.info("=" * 80)
    
    app.run(host=Config.HOST, port=Config.PORT, debug=False, use_reloader=False)

if __name__ == '__main__':
    try:
        mp.set_start_method('spawn')
    except RuntimeError:
        pass
    main()

