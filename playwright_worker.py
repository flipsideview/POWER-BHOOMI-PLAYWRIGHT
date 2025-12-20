#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                  POWER-BHOOMI - Playwright Worker Process                    ║
║                  Process-based worker with bounded browser                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

Purpose:
  - Each worker is an OS process (not thread)
  - Owns exactly ONE browser instance
  - Pulls tasks from multiprocessing.Queue
  - Saves results to SQLite in real-time
  - Gracefully handles shutdown signals
  - Planned browser recycling (not emergency kills)

Architecture:
  Process → Browser → Context → Page
  (1:1)     (1:1)     (1:1)     (1:1)
  
  No browser churn - context/page reused for all tasks

Author: POWER-BHOOMI Team
Version: 4.0.0
"""

import os
import sys
import time
import signal
import logging
import traceback
import multiprocessing as mp
from typing import Optional, Dict, List
from datetime import datetime
import sqlite3
from pathlib import Path

# Playwright imports
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeout

# Local imports
from task_models import SearchTask
from portal_health import get_portal_monitor

# Configure logging for worker process
logger = logging.getLogger(f'PlaywrightWorker')


class PlaywrightWorker:
    """
    Worker process that owns exactly one browser.
    
    Lifecycle:
    1. __init__ → store config
    2. run() → entry point (static method for multiprocessing)
    3. _init_browser() → create browser ONCE
    4. _task_loop() → pull and process tasks
    5. _recycle_browser() → planned restart (every 200 tasks or 2 hours)
    6. _cleanup() → graceful shutdown
    
    GUARANTEED: Only 1 browser per process, no create/destroy in loops
    """
    
    def __init__(
        self,
        worker_id: int,
        task_queue: mp.Queue,
        shutdown_event: mp.Event,
        shared_state: Dict,
        db_path: str = None
    ):
        """
        Initialize worker (called in child process).
        
        Args:
            worker_id: Unique worker ID (0, 1, 2, ...)
            task_queue: multiprocessing.Queue for tasks
            shutdown_event: multiprocessing.Event for graceful shutdown
            shared_state: Shared dict for metrics
            db_path: SQLite database path
        """
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.shutdown_event = shutdown_event
        self.shared_state = shared_state
        
        # Database
        self.db_path = db_path or str(Path.home() / 'Documents' / 'POWER-BHOOMI' / 'bhoomi_data.db')
        self.db_conn: Optional[sqlite3.Connection] = None
        
        # Playwright instances (created once, reused)
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        # Metrics
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.records_saved = 0
        self.last_recycle_time = time.time()
        self.start_time = time.time()
        
        # Portal health
        self.portal_monitor = get_portal_monitor()
        
        # Current state (for smart navigation)
        self._current_village = None
        self._current_survey = None
        
        # Configure logging with worker ID
        self.logger = logging.getLogger(f'Worker-{worker_id}')
        
    @staticmethod
    def run(worker_id: int, task_queue: mp.Queue, shutdown_event: mp.Event, 
            shared_state: Dict, db_path: str = None):
        """
        Static entry point for multiprocessing.Process.
        
        This is called in the child process after fork.
        """
        # Reconfigure logging in child process
        logging.basicConfig(
            level=logging.INFO,
            format=f'%(asctime)s | Worker-{worker_id} | %(levelname)-7s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        worker = PlaywrightWorker(worker_id, task_queue, shutdown_event, shared_state, db_path)
        worker._setup_signal_handlers()
        worker._run_loop()
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def handler(signum, frame):
            self.logger.info(f"Received signal {signum}, shutting down...")
            self._cleanup()
            sys.exit(0)
        
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)
    
    def _run_loop(self):
        """Main worker loop - initialize, process tasks, cleanup"""
        try:
            self.logger.info(f"🚀 Worker {self.worker_id} starting (PID: {os.getpid()})")
            
            # Initialize browser ONCE
            self._init_browser()
            
            # Initialize database connection
            self._init_database()
            
            # Main task loop
            while not self.shutdown_event.is_set():
                # Check if we need to recycle browser
                if self._should_recycle():
                    self._recycle_browser()
                
                # Get task from queue (timeout 5s to check shutdown periodically)
                try:
                    task: SearchTask = self.task_queue.get(timeout=5)
                except:
                    # Queue empty or timeout - continue loop
                    continue
                
                # Check portal health before processing
                if not self.portal_monitor.should_allow_task():
                    backoff = self.portal_monitor.get_backoff_seconds()
                    self.logger.warning(f"Portal unhealthy, backing off {backoff}s")
                    # Put task back in queue
                    self.task_queue.put(task)
                    time.sleep(min(backoff, 30))  # Cap at 30s
                    continue
                
                # Process task
                try:
                    task.mark_started(self.worker_id)
                    self._process_task(task)
                    task.mark_completed()
                    
                    self.tasks_processed += 1
                    
                    # Update shared state
                    if 'tasks_completed' in self.shared_state:
                        self.shared_state['tasks_completed'] += 1
                    
                except Exception as e:
                    self.logger.error(f"Task failed: {task.get_summary()} - {str(e)[:100]}")
                    task.mark_failed(str(e))
                    self.tasks_failed += 1
                    
                    # Update shared state
                    if 'tasks_failed' in self.shared_state:
                        self.shared_state['tasks_failed'] += 1
                    
                    # Re-queue if retryable
                    if task.can_retry():
                        task.increment_retry()
                        self.task_queue.put(task)
                        self.logger.info(f"Re-queued task (retry {task.retry_count})")
            
            self.logger.info(f"✅ Worker {self.worker_id} finished (processed {self.tasks_processed} tasks)")
            
        except Exception as e:
            self.logger.error(f"Worker loop error: {traceback.format_exc()}")
        finally:
            self._cleanup()
    
    def _init_browser(self):
        """
        Initialize Playwright browser - CALLED ONCE PER WORKER.
        
        This is the ONLY place where a browser is created.
        Browser is reused for all tasks via context/page.
        """
        try:
            self.logger.info("Initializing Playwright browser...")
            
            # Start Playwright
            self.playwright = sync_playwright().start()
            
            # Launch browser (headless)
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-gpu',
                    '--disable-images',  # Faster page loads
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                ]
            )
            
            # Create persistent context
            self.context = self.browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            # Create single page (reused for all tasks)
            self.page = self.context.new_page()
            self.page.set_default_timeout(20000)  # 20s
            
            self.logger.info(f"✅ Browser initialized for worker {self.worker_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize browser: {e}")
            raise
    
    def _recycle_browser(self):
        """
        Planned browser recycling to prevent memory leaks.
        
        Closes old browser and creates new one in-place.
        This is NOT an emergency kill - it's planned maintenance.
        """
        self.logger.info(f"♻️  Recycling browser (processed {self.tasks_processed} tasks)")
        
        # Close old browser
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except Exception as e:
            self.logger.warning(f"Cleanup during recycle: {e}")
        
        # Reset instances
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        
        # Initialize new browser
        time.sleep(2)  # Brief pause
        self._init_browser()
        
        # Reset metrics
        self.tasks_processed = 0
        self.last_recycle_time = time.time()
        self._current_village = None
        self._current_survey = None
    
    def _should_recycle(self) -> bool:
        """
        Check if browser should be recycled.
        
        Recycle after:
        - 200 tasks processed, OR
        - 2 hours elapsed
        
        This prevents memory leaks in long runs.
        """
        return (
            self.tasks_processed >= 200 or
            (time.time() - self.last_recycle_time) > 7200  # 2 hours
        )
    
    def _init_database(self):
        """Initialize SQLite database connection (thread-safe)"""
        try:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self.db_conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.db_conn.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging
            self.logger.info(f"Database connected: {self.db_path}")
        except Exception as e:
            self.logger.error(f"Database init failed: {e}")
            raise
    
    def _process_task(self, task: SearchTask):
        """
        Process a single search task.
        
        Steps:
        1. Navigate to portal (if needed)
        2. Fill form (district → taluk → hobli → village → survey)
        3. Extract surnoc/hissa/period options
        4. For each combination, fetch owner data
        5. Save to database in real-time
        
        Args:
            task: SearchTask to process
        """
        self.logger.info(f"Processing: {task.get_summary()}")
        
        try:
            # Navigate to portal if not already there
            if 'landrecords.karnataka.gov.in' not in self.page.url:
                self.page.goto('https://landrecords.karnataka.gov.in/Service2/')
                self.page.wait_for_load_state('domcontentloaded')
            
            # Fill form (use smart navigation to avoid redundant selections)
            self._navigate_to_village(task)
            
            # Fill survey number
            self.page.fill('#ctl00_MainContent_txtCSurveyNo', str(task.survey_no))
            self.page.click('#ctl00_MainContent_btnCGo')
            self.page.wait_for_load_state('domcontentloaded')
            time.sleep(1)
            
            # Handle any alerts (session expired, etc.)
            had_alert, alert_text, is_portal_issue = self._handle_alert()
            if had_alert and is_portal_issue:
                raise Exception(f"Portal issue: {alert_text[:100]}")
            
            # Check if surnoc dropdown has options
            surnoc_options = self._get_dropdown_options('#ctl00_MainContent_ddlCSurnocNo')
            if not surnoc_options or len(surnoc_options) <= 1:
                # No surnoc options (empty survey)
                self.logger.debug(f"No data for survey {task.survey_no}")
                return
            
            # Process each surnoc → hissa → period combination
            for surnoc in surnoc_options[1:]:  # Skip first "Select" option
                self._select_dropdown('#ctl00_MainContent_ddlCSurnocNo', surnoc)
                time.sleep(0.5)
                
                hissa_options = self._get_dropdown_options('#ctl00_MainContent_ddlCHissaNo')
                for hissa in hissa_options[1:]:
                    self._select_dropdown('#ctl00_MainContent_ddlCHissaNo', hissa)
                    time.sleep(0.5)
                    
                    period_options = self._get_dropdown_options('#ctl00_MainContent_ddlCPeriod')
                    for period in period_options[1:]:
                        self._select_dropdown('#ctl00_MainContent_ddlCPeriod', period)
                        time.sleep(0.5)
                        
                        # Click fetch
                        self.page.click('#ctl00_MainContent_btnCFetchDetails')
                        self.page.wait_for_load_state('domcontentloaded')
                        
                        # Extract owner data
                        owners = self._extract_owners()
                        
                        # Save to database
                        for owner in owners:
                            self._save_record(task, surnoc, hissa, period, owner)
                            self.records_saved += 1
        
        except PlaywrightTimeout as e:
            self.logger.warning(f"Timeout during task: {str(e)[:100]}")
            raise
        except Exception as e:
            self.logger.error(f"Task processing error: {traceback.format_exc()}")
            raise
    
    def _navigate_to_village(self, task: SearchTask):
        """Smart navigation - only update changed dropdowns"""
        # TODO: Implement smart caching to avoid re-selecting unchanged dropdowns
        self._select_dropdown('#ctl00_MainContent_ddlCDistrict', task.district_name)
        time.sleep(1)
        self._select_dropdown('#ctl00_MainContent_ddlCTaluk', task.taluk_name)
        time.sleep(1)
        self._select_dropdown('#ctl00_MainContent_ddlCHobli', task.hobli_name)
        time.sleep(1)
        self._select_dropdown('#ctl00_MainContent_ddlCVillage', task.village_name)
        time.sleep(1)
    
    def _select_dropdown(self, selector: str, value: str):
        """Select dropdown option by visible text"""
        self.page.select_option(selector, label=value)
    
    def _get_dropdown_options(self, selector: str) -> List[str]:
        """Get all options from dropdown"""
        options = self.page.locator(f'{selector} option').all_text_contents()
        return options
    
    def _handle_alert(self) -> tuple:
        """
        Handle portal alerts and dialogs.
        
        Returns:
            (had_alert, alert_text, is_portal_issue)
        """
        try:
            # Check for JavaScript alerts
            page_content = self.page.content()
            
            # Common alert patterns
            SESSION_EXPIRED_PATTERNS = [
                'session expired',
                'session has expired',
                'ಸೆಷನ್ ಮುಗಿದಿದೆ',
                'please login again'
            ]
            
            PORTAL_ISSUE_PATTERNS = [
                'server error',
                'internal error',
                'maintenance',
                'temporarily unavailable',
                'ದೋಷ'
            ]
            
            # Check for alert dialogs
            alert_text = ''
            had_alert = False
            
            # Check visible alerts on page
            for selector in ['.alert', '.alert-danger', '.alert-warning', '[role="alert"]']:
                try:
                    alert_elem = self.page.locator(selector).first
                    if alert_elem.is_visible(timeout=500):
                        alert_text = alert_elem.text_content()
                        had_alert = True
                        break
                except:
                    continue
            
            if had_alert:
                alert_lower = alert_text.lower()
                
                # Check if session expired
                if any(pattern in alert_lower for pattern in SESSION_EXPIRED_PATTERNS):
                    self.logger.warning(f"🔄 Session expired detected")
                    return (True, alert_text, True)
                
                # Check if portal issue
                if any(pattern in alert_lower for pattern in PORTAL_ISSUE_PATTERNS):
                    self.logger.warning(f"⚠️  Portal issue: {alert_text[:100]}")
                    return (True, alert_text, True)
                
                # Other alert
                self.logger.warning(f"Alert: {alert_text[:100]}")
                return (True, alert_text, False)
            
            return (False, '', False)
            
        except Exception as e:
            self.logger.debug(f"Alert check error: {e}")
            return (False, '', False)
    
    def _extract_owners(self) -> List[Dict]:
        """
        Extract owner data from results table (ROBUST multi-strategy extraction).
        
        Returns:
            List of owner records: [{'name': ..., 'extent': ..., 'khatah': ...}, ...]
        """
        from bs4 import BeautifulSoup
        import re
        
        owners = []
        try:
            page_source = self.page.content()
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # CRITICAL: Remove form elements that might be mistaken for data
            for unwanted in soup.find_all(['select', 'nav', 'header', 'footer', 'button', 'input', 'script', 'style']):
                unwanted.decompose()
            
            # Keywords for identifying results table
            RESULT_KEYWORDS = ['Owner', 'ಮಾಲೀಕರ', 'Extent', 'ವಿಸ್ತೀರ್ಣ', 'Khata', 'ಖಾತಾ', 'Name', 'ಹೆಸರು']
            FORM_KEYWORDS = ['Select District', 'Select Taluk', 'Select Hobli', 'Select Village']
            SKIP_PATTERNS = re.compile(r'^(Sl\.?\s*No\.?|ಕ್ರಮ|ಸಂ|#|\d{1,3})$', re.IGNORECASE)
            
            # Find the results table
            results_table = None
            for table in soup.find_all('table'):
                table_text = table.get_text()
                
                # Skip form tables
                if any(keyword in table_text for keyword in FORM_KEYWORDS):
                    continue
                
                # Check for result keywords
                result_score = sum(1 for keyword in RESULT_KEYWORDS if keyword in table_text)
                if result_score >= 2:  # At least 2 result keywords
                    results_table = table
                    break
            
            if not results_table:
                self.logger.debug("No results table found")
                return []
            
            # Extract data from results table
            rows = results_table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue
                
                cell_texts = [c.get_text(strip=True) for c in cells]
                
                # Skip serial number rows
                if SKIP_PATTERNS.match(cell_texts[0]):
                    continue
                
                # Look for extent pattern (e.g., "0.12.0" or "0-12-0")
                extent_pattern = re.compile(r'\d+[-\.]\d+[-\.]\d+')
                has_extent = any(extent_pattern.search(text) for text in cell_texts)
                
                if has_extent and len(cell_texts) >= 2:
                    # Extract owner, extent, khatah
                    owner_name = cell_texts[0] if cell_texts[0] else ''
                    extent = cell_texts[1] if len(cell_texts) > 1 else ''
                    khatah = cell_texts[2] if len(cell_texts) > 2 else ''
                    
                    # Filter out header-like text
                    if owner_name and not any(k in owner_name for k in RESULT_KEYWORDS):
                        owners.append({
                            'name': owner_name,
                            'extent': extent,
                            'khatah': khatah
                        })
            
            self.logger.debug(f"Extracted {len(owners)} owners")
            
        except Exception as e:
            self.logger.error(f"Owner extraction error: {e}")
        
        return owners
    
    def _save_record(self, task: SearchTask, surnoc: str, hissa: str, period: str, owner: Dict):
        """Save record to SQLite database"""
        try:
            # Check if owner name matches search criteria
            is_match = self._is_owner_match(owner['name'], task.owner_name, task.owner_variants)
            
            cursor = self.db_conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO land_records (
                    session_id, district, taluk, hobli, village,
                    survey_no, surnoc, hissa, period,
                    owner_name, extent, khatah, is_match, worker_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                task.session_id,
                task.district_name,
                task.taluk_name,
                task.hobli_name,
                task.village_name,
                task.survey_no,
                surnoc,
                hissa,
                period,
                owner['name'],
                owner['extent'],
                owner['khatah'],
                1 if is_match else 0,
                self.worker_id
            ))
            self.db_conn.commit()
            
            if is_match:
                self.logger.info(f"✨ MATCH: {owner['name']} - {task.get_summary()}")
            
        except Exception as e:
            self.logger.error(f"Database save error: {e}")
    
    def _is_owner_match(self, owner_name: str, search_name: str, variants: List[str]) -> bool:
        """
        Check if owner name matches search criteria.
        
        Uses:
        - Exact match (case-insensitive)
        - Fuzzy match (Levenshtein distance)
        - Variant matching
        """
        if not search_name:
            return False
        
        owner_lower = owner_name.lower()
        search_lower = search_name.lower()
        
        # Exact match
        if search_lower in owner_lower or owner_lower in search_lower:
            return True
        
        # Check variants
        for variant in variants:
            if variant.lower() in owner_lower:
                return True
        
        # TODO: Add fuzzy matching (Levenshtein distance)
        
        return False
    
    def _cleanup(self):
        """Cleanup resources on shutdown"""
        self.logger.info(f"Cleaning up worker {self.worker_id}...")
        
        # Close Playwright resources
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except Exception as e:
            self.logger.warning(f"Playwright cleanup error: {e}")
        
        # Close database
        try:
            if self.db_conn:
                self.db_conn.close()
        except Exception as e:
            self.logger.warning(f"Database cleanup error: {e}")
        
        self.logger.info(f"✅ Worker {self.worker_id} cleanup complete")


if __name__ == '__main__':
    # Test worker in standalone mode
    import multiprocessing as mp
    from task_models import SearchTask
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-7s | %(message)s'
    )
    
    # Create test task
    task = SearchTask(
        session_id='test_001',
        district_code='01',
        district_name='BANGALORE URBAN',
        taluk_code='02',
        taluk_name='BANGALORE NORTH',
        hobli_code='03',
        hobli_name='YELAHANKA',
        village_code='12345',
        village_name='Test Village',
        survey_no=1,
        owner_name='Test Owner'
    )
    
    # Create queue and shared state
    queue = mp.Queue()
    queue.put(task)
    
    shutdown = mp.Event()
    shared_state = mp.Manager().dict({'tasks_completed': 0, 'tasks_failed': 0})
    
    # Run worker
    print("Starting test worker...")
    PlaywrightWorker.run(0, queue, shutdown, shared_state)
    
    print("\n✅ Worker test complete!")

