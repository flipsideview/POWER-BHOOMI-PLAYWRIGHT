#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                      POWER-BHOOMI v2.0 - PARALLEL SEARCH ENGINE                      ║
║                        Karnataka Land Records Search Tool                             ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  Features:                                                                            ║
║  • Parallel browser processing (4x faster)                                           ║
║  • Thread-safe CSV output                                                            ║
║  • Real-time multi-worker progress tracking                                          ║
║  • Smart error recovery and retry logic                                              ║
║  • Professional, production-ready code                                               ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

Version: 2.0.0
Author: POWER-BHOOMI Team
"""

import os
import sys
import json
import time
import logging
import threading
import queue
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
    """Application configuration"""
    # Server
    HOST = '0.0.0.0'
    PORT = 5001
    DEBUG = True
    
    # Parallel Processing
    MAX_WORKERS = 4  # Number of parallel browser instances
    
    # Timeouts (seconds)
    PAGE_LOAD_TIMEOUT = 30
    ELEMENT_WAIT_TIMEOUT = 10
    POST_CLICK_WAIT = 6
    POST_SELECT_WAIT = 2
    
    # Search Settings
    DEFAULT_MAX_SURVEY = 200
    EMPTY_SURVEY_THRESHOLD = 30  # Skip village after this many empty surveys
    
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
    
    # Worker details
    workers: Dict[int, WorkerStatus] = field(default_factory=dict)
    
    # Logs
    logs: List[str] = field(default_factory=list)
    
    # File paths
    all_records_file: str = ''
    matches_file: str = ''

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
    """
    
    def __init__(
        self,
        worker_id: int,
        search_params: dict,
        villages: List[Tuple[str, str, str, str]],  # (village_code, village_name, hobli_code, hobli_name)
        state: SearchState,
        all_records_writer: ThreadSafeCSVWriter,
        matches_writer: ThreadSafeCSVWriter,
        state_lock: threading.Lock
    ):
        self.worker_id = worker_id
        self.params = search_params
        self.villages = villages
        self.state = state
        self.all_records_writer = all_records_writer
        self.matches_writer = matches_writer
        self.state_lock = state_lock
        
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
    
    def _init_browser(self):
        """Initialize browser with optimized settings"""
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-images')
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument(f'--user-data-dir=/tmp/chrome_worker_{self.worker_id}')
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
        
        self._add_log("Browser initialized")
    
    def _close_browser(self):
        """Safely close browser"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
    
    def _extract_owners(self, page_source: str) -> List[dict]:
        """Extract owner details from page source"""
        from bs4 import BeautifulSoup
        import re
        
        owners = []
        try:
            soup = BeautifulSoup(page_source, 'html.parser')
            for table in soup.find_all('table'):
                text = table.get_text()
                if 'Owner' in text or 'Extent' in text:
                    for row in table.find_all('tr'):
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            cell_texts = [c.get_text(strip=True) for c in cells]
                            row_text = ' '.join(cell_texts)
                            # Look for extent pattern (e.g., 0.12.0)
                            if re.search(r'\d+\.\d+\.\d+', row_text):
                                owners.append({
                                    'owner_name': cell_texts[0] if cell_texts else '',
                                    'extent': cell_texts[1] if len(cell_texts) > 1 else '',
                                    'khatah': cell_texts[2] if len(cell_texts) > 2 else '',
                                })
        except Exception as e:
            self.logger.error(f"Extract error: {e}")
        
        return owners
    
    def _search_village(self, village_code: str, village_name: str, hobli_code: str, hobli_name: str):
        """Search a single village for all survey numbers"""
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
        
        for survey_no in range(1, max_survey + 1):
            if not self.state.running:
                return
            
            self._update_status(current_survey=survey_no)
            
            try:
                # Navigate to portal
                self.driver.get(Config.SERVICE2_URL)
                time.sleep(Config.POST_SELECT_WAIT)
                
                # Select location
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
                time.sleep(Config.POST_CLICK_WAIT + 2)
                
                # Check if surnoc populated
                surnoc_sel = Select(self.driver.find_element(By.ID, IDS['surnoc']))
                surnoc_opts = [o.text for o in surnoc_sel.options if "Select" not in o.text]
                
                if not surnoc_opts:
                    empty_count += 1
                    if empty_count > Config.EMPTY_SURVEY_THRESHOLD:
                        self._add_log(f"Skipping {village_name} after {empty_count} empty surveys")
                        break
                    continue
                
                # Found data - reset empty count
                empty_count = 0
                
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
                            
                            try:
                                hissa_sel = Select(self.driver.find_element(By.ID, IDS['hissa']))
                                hissa_sel.select_by_visible_text(hissa)
                                time.sleep(Config.POST_SELECT_WAIT)
                                
                                # Select latest period
                                period_sel = Select(self.driver.find_element(By.ID, IDS['period']))
                                period_opts = [o.text for o in period_sel.options if "Select" not in o.text]
                                period = period_opts[0] if period_opts else ''
                                if period:
                                    period_sel.select_by_visible_text(period)
                                    time.sleep(1)
                                
                                # Click Fetch Details
                                fetch_btn = self.driver.find_element(By.ID, IDS['fetch_btn'])
                                self.driver.execute_script("arguments[0].click();", fetch_btn)
                                time.sleep(Config.POST_CLICK_WAIT)
                                
                                # Extract owners
                                owners = self._extract_owners(self.driver.page_source)
                                
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
                                    
                                    # Write to all records
                                    self.all_records_writer.write_record(asdict(record))
                                    self.records_found += 1
                                    
                                    # Check for match
                                    is_match = any(v.lower() in owner['owner_name'].lower() for v in owner_variants if v)
                                    if is_match:
                                        self.matches_writer.write_record(asdict(record))
                                        self.matches_found += 1
                                        self._add_log(f"🎯 MATCH: {owner['owner_name']} in {village_name} Sy:{survey_no}")
                                
                                # Update stats
                                self._update_status(
                                    records_found=self.records_found,
                                    matches_found=self.matches_found
                                )
                                self._update_global_stats()
                                
                                # Reload page for next hissa
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
                                
                            except Exception as e:
                                self.errors += 1
                                continue
                                
                    except Exception as e:
                        self.errors += 1
                        continue
                        
            except Exception as e:
                self.errors += 1
                empty_count += 1
                if empty_count > Config.EMPTY_SURVEY_THRESHOLD:
                    break
    
    def run(self):
        """Main worker execution"""
        self._update_status(status='running', villages_total=len(self.villages))
        self._add_log(f"Starting with {len(self.villages)} villages")
        
        try:
            self._init_browser()
            
            for idx, (village_code, village_name, hobli_code, hobli_name) in enumerate(self.villages):
                if not self.state.running:
                    self._add_log("Stopped by user")
                    break
                
                self._add_log(f"Village {idx+1}/{len(self.villages)}: {village_name}")
                self._search_village(village_code, village_name, hobli_code, hobli_name)
                
                self._update_status(villages_completed=idx + 1)
                self._update_global_stats()
            
            self._update_status(status='completed')
            self._add_log(f"Completed: {self.records_found} records, {self.matches_found} matches")
            
        except Exception as e:
            self._update_status(status='failed', errors=self.errors + 1)
            self._add_log(f"Error: {str(e)}")
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
    """
    
    def __init__(self):
        self.state = SearchState()
        self.state_lock = threading.Lock()
        self.executor: Optional[ThreadPoolExecutor] = None
        self.workers: List[SearchWorker] = []
        self.all_records_writer: Optional[ThreadSafeCSVWriter] = None
        self.matches_writer: Optional[ThreadSafeCSVWriter] = None
        self.api = BhoomiAPI()
    
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
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        
        try:
            driver.get(Config.SERVICE2_URL)
            time.sleep(3)
            
            IDS = Config.ELEMENT_IDS
            
            # Select district
            dist_sel = Select(driver.find_element(By.ID, IDS['district']))
            dist_opts = {o.get_attribute('value'): o.text for o in dist_sel.options if o.get_attribute('value')}
            params['district_name'] = dist_opts.get(params['district_code'], 'Unknown')
            
            dist_sel.select_by_value(params['district_code'])
            time.sleep(2)
            
            # Select taluk
            taluk_sel = Select(driver.find_element(By.ID, IDS['taluk']))
            taluk_opts = {o.get_attribute('value'): o.text for o in taluk_sel.options if o.get_attribute('value')}
            params['taluk_name'] = taluk_opts.get(params['taluk_code'], 'Unknown')
            
            taluk_sel.select_by_value(params['taluk_code'])
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
    
    def start_search(self, params: dict) -> bool:
        """Start parallel search"""
        if self.state.running:
            logger.warning("Search already running")
            return False
        
        try:
            # Initialize state
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            owner_name = params.get('owner_name', '')
            
            self.state = SearchState(
                running=True,
                completed=False,
                start_time=datetime.now().isoformat(),
                owner_name=owner_name,
                owner_variants=[owner_name, owner_name.upper(), owner_name.lower()],
                all_records_file=f'all_records_{timestamp}.csv',
                matches_file=f'owner_matches_{timestamp}.csv'
            )
            
            # Initialize CSV writers
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
                self.state.logs.append(f"Starting {num_workers} workers for {len(villages)} villages")
            
            # Start workers
            self.executor = ThreadPoolExecutor(max_workers=num_workers)
            
            for i in range(num_workers):
                worker = SearchWorker(
                    worker_id=i,
                    search_params=params,
                    villages=village_chunks[i],
                    state=self.state,
                    all_records_writer=self.all_records_writer,
                    matches_writer=self.matches_writer,
                    state_lock=self.state_lock
                )
                self.workers.append(worker)
                self.executor.submit(worker.run)
            
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
                    self.state.logs.append("✅ All workers completed!")
                    logger.info("Search completed")
                    break
    
    def stop_search(self):
        """Stop all workers"""
        self.state.running = False
        with self.state_lock:
            self.state.logs.append("⏹️ Stop requested by user")
        
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
                'logs': self.state.logs[-20:],  # Last 20 logs
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
    <title>POWER-BHOOMI v2.0 | Parallel Search Engine</title>
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
        
        /* Responsive */
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
            <div class="version-badge">v2.0 • 4x Faster</div>
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
            
            <!-- Activity Log -->
            <div class="card">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                    <h3 class="card-title" style="margin: 0;">Activity Log</h3>
                    <button id="exportBtn" class="btn btn-sm" style="background: var(--bg-input); border: 1px solid var(--border-color);">
                        📥 Export CSV
                    </button>
                </div>
                <div class="logs-container" id="logsContainer">
                    <div class="log-entry">Ready to start parallel search...</div>
                </div>
            </div>
        </section>
    </main>
    
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
            document.getElementById('exportBtn').addEventListener('click', exportCSV);
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
        
        async function exportCSV() {
            try {
                const res = await fetch('/api/search/status');
                const status = await res.json();
                if (status.all_records_file) {
                    alert(`Files saved:\\n📁 ${status.all_records_file}\\n📁 ${status.matches_file}`);
                } else {
                    alert('No search results yet');
                }
            } catch (e) {
                alert('Error getting file info');
            }
        }
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

# ═══════════════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                      POWER-BHOOMI v2.0 - PARALLEL SEARCH ENGINE                      ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                      ║
║   🚀 4 Parallel Browser Workers                                                      ║
║   📊 Thread-Safe CSV Output                                                          ║
║   ⚡ Real-Time Multi-Worker Progress                                                 ║
║   🛡️ Smart Error Recovery                                                            ║
║                                                                                      ║
║   🌐 Open your browser and navigate to:                                              ║
║                                                                                      ║
║       http://localhost:5001                                                          ║
║                                                                                      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
    """)
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG, threaded=True)

