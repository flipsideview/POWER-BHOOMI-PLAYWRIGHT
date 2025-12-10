#!/usr/bin/env python3
"""
Karnataka Bhoomi - Owner Search using Browser Automation
========================================================
Uses Selenium to automate browser interaction with the Service2 portal.

Owner: ಚೌಡೆ ಬೈರೆ ಗೌಡ (CHOWDE BYRE GOWDA)
District: BANGALORE RURAL (Code: 21)
Taluk: HOSKOTE (Code: 4)

Strategy:
1. Open Service2 portal
2. For each village in Hoskote Taluk:
   - Fill in District, Taluk, Hobli, Village
   - Submit form (without survey number to get all RTCs)
   - Search results for owner name
   - Save matching RTCs
"""

import time
import json
import csv
import logging
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Optional
import re

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

OWNER_NAME_KANNADA = "ಚೌಡೆ ಬೈರೆ ಗೌಡ"
OWNER_NAME_ENGLISH = "CHOWDE BYRE GOWDA"
OWNER_SEARCH_VARIANTS = [
    "ಚೌಡೆ ಬೈರೆ ಗೌಡ",
    "ಚೌಡೆ",
    "ಬೈರೆ ಗೌಡ",
    "CHOWDE",
    "BYRE GOWDA",
    "BYREGOWDA",
]

SERVICE2_URL = "https://landrecords.karnataka.gov.in/Service2/"
ECHAWADI_URL = "https://rdservices.karnataka.gov.in/echawadi/Home"

# Hoskote Taluk
DISTRICT_CODE = "21"
DISTRICT_NAME = "BANGALORE RURAL"
TALUK_CODE = "4"
TALUK_NAME = "HOSKOTE"

# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class MatchingRTC:
    village_code: str
    village_name: str
    hobli_code: str
    hobli_name: str
    survey_no: str
    owner_name: str
    extent: str
    owner_category: str
    source: str
    timestamp: str


# ═══════════════════════════════════════════════════════════════════════════════
# API CLIENT (for village list)
# ═══════════════════════════════════════════════════════════════════════════════

class BhoomiAPI:
    """Client to get village list from eChawadi API"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        })
    
    def get_hoblis(self) -> List[dict]:
        """Get all hoblis in Hoskote Taluk"""
        url = f"{ECHAWADI_URL}/LoadHobli"
        try:
            response = self.session.post(
                url,
                json={'pDistCode': DISTRICT_CODE, 'pTalukCode': TALUK_CODE},
                headers={'Content-Type': 'application/json; charset=utf-8'},
                verify=False, timeout=30
            )
            result = response.text
            if result.startswith('"'):
                result = json.loads(result)
            if isinstance(result, str):
                result = json.loads(result)
            return result.get('data', [])
        except Exception as e:
            logger.error(f"Error getting hoblis: {e}")
            return []
    
    def get_villages(self, hobli_code: str) -> List[dict]:
        """Get all villages in a hobli"""
        url = f"{ECHAWADI_URL}/LoadVillage"
        try:
            response = self.session.post(
                url,
                json={
                    'pDistCode': DISTRICT_CODE,
                    'pTalukCode': TALUK_CODE,
                    'pHobliCode': hobli_code
                },
                headers={'Content-Type': 'application/json; charset=utf-8'},
                verify=False, timeout=30
            )
            result = response.text
            if result.startswith('"'):
                result = json.loads(result)
            if isinstance(result, str):
                result = json.loads(result)
            return result.get('data', [])
        except Exception as e:
            logger.error(f"Error getting villages: {e}")
            return []


# ═══════════════════════════════════════════════════════════════════════════════
# BROWSER AUTOMATION
# ═══════════════════════════════════════════════════════════════════════════════

class BhoomiPortalAutomation:
    """Automate Bhoomi Service2 portal using Selenium"""
    
    def __init__(self, headless: bool = False):
        if not SELENIUM_AVAILABLE:
            raise ImportError("Selenium not installed. Run: pip install selenium")
        
        self.options = Options()
        if headless:
            self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--window-size=1920,1080')
        
        self.driver = None
        self.matches: List[MatchingRTC] = []
    
    def start(self):
        """Start browser"""
        self.driver = webdriver.Chrome(options=self.options)
        self.driver.implicitly_wait(10)
        logger.info("Browser started")
    
    def stop(self):
        """Stop browser"""
        if self.driver:
            self.driver.quit()
            logger.info("Browser stopped")
    
    def navigate_to_portal(self):
        """Navigate to Service2 portal"""
        self.driver.get(SERVICE2_URL)
        time.sleep(3)  # Wait for page load
        logger.info(f"Navigated to {SERVICE2_URL}")
    
    def select_dropdown(self, dropdown_id: str, value: str, wait_time: int = 5):
        """Select value from dropdown"""
        try:
            dropdown = WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.ID, dropdown_id))
            )
            select = Select(dropdown)
            select.select_by_value(value)
            time.sleep(1)  # Wait for dependent dropdowns to load
            return True
        except Exception as e:
            logger.error(f"Error selecting {dropdown_id}: {e}")
            return False
    
    def search_village(self, hobli_code: str, village_code: str) -> List[str]:
        """
        Search for all RTCs in a village and return page content
        """
        try:
            # Select District
            self.select_dropdown('ctl00_MainContent_ddlDistrict', DISTRICT_CODE)
            time.sleep(1)
            
            # Select Taluk
            self.select_dropdown('ctl00_MainContent_ddlTaluk', TALUK_CODE)
            time.sleep(1)
            
            # Select Hobli
            self.select_dropdown('ctl00_MainContent_ddlHobli', hobli_code)
            time.sleep(1)
            
            # Select Village
            self.select_dropdown('ctl00_MainContent_ddlVillage', village_code)
            time.sleep(1)
            
            # Click Go button (without survey number to get all)
            go_button = self.driver.find_element(By.ID, 'ctl00_MainContent_btnGo')
            go_button.click()
            time.sleep(3)
            
            # Get page content
            return self.driver.page_source
            
        except Exception as e:
            logger.error(f"Error searching village {village_code}: {e}")
            return ""
    
    def check_for_owner(self, page_content: str) -> bool:
        """Check if page content contains owner name"""
        for variant in OWNER_SEARCH_VARIANTS:
            if variant in page_content:
                return True
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# SIMPLE TEXT-BASED SEARCH (Alternative approach)
# ═══════════════════════════════════════════════════════════════════════════════

def generate_village_search_data():
    """Generate a JSON file with all villages for manual/browser search"""
    api = BhoomiAPI()
    
    all_data = {
        'owner_to_search': OWNER_NAME_KANNADA,
        'district': {'code': DISTRICT_CODE, 'name': DISTRICT_NAME},
        'taluk': {'code': TALUK_CODE, 'name': TALUK_NAME},
        'hoblis': []
    }
    
    hoblis = api.get_hoblis()
    logger.info(f"Found {len(hoblis)} hoblis")
    
    for hobli in hoblis:
        hobli_code = str(int(float(hobli['hobli_code'])))
        hobli_name = hobli.get('hobli_name_kn', '')
        
        hobli_data = {
            'code': hobli_code,
            'name': hobli_name,
            'villages': []
        }
        
        villages = api.get_villages(hobli_code)
        logger.info(f"Hobli {hobli_code} ({hobli_name}): {len(villages)} villages")
        
        for village in villages:
            village_data = {
                'code': str(int(float(village['village_code']))),
                'name_kn': village.get('village_name_kn', ''),
                'name_en': village.get('village_name_en', ''),
                'lgd_code': str(int(float(village.get('LGDCODE', 0))))
            }
            hobli_data['villages'].append(village_data)
        
        all_data['hoblis'].append(hobli_data)
        time.sleep(0.5)
    
    # Save to JSON
    with open('hoskote_villages.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    # Generate search URLs
    urls_file = 'search_urls.txt'
    with open(urls_file, 'w', encoding='utf-8') as f:
        f.write(f"# Owner Search URLs for: {OWNER_NAME_KANNADA}\n")
        f.write(f"# District: {DISTRICT_NAME}, Taluk: {TALUK_NAME}\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n\n")
        
        total = 0
        for hobli in all_data['hoblis']:
            f.write(f"\n# Hobli: {hobli['name']} (Code: {hobli['code']})\n")
            f.write(f"# Villages: {len(hobli['villages'])}\n")
            
            for village in hobli['villages']:
                total += 1
                # eChawadi URL format
                url = f"https://rdservices.karnataka.gov.in/echawadi/#dist={DISTRICT_CODE}&taluk={TALUK_CODE}&hobli={hobli['code']}&village={village['code']}"
                f.write(f"{total}. {village['name_kn']} ({village['name_en']})\n")
                f.write(f"   URL: {url}\n")
        
        f.write(f"\n# Total villages to search: {total}\n")
    
    logger.info(f"Saved village data to hoskote_villages.json")
    logger.info(f"Saved search URLs to {urls_file}")
    
    return all_data


def print_manual_search_guide():
    """Print guide for manual search"""
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    MANUAL SEARCH GUIDE                                       ║
║           Owner: ಚೌಡೆ ಬೈರೆ ಗೌಡ (CHOWDE BYRE GOWDA)                          ║
╠══════════════════════════════════════════════════════════════════════════════╣

Since automated API search is restricted, here's how to search manually:

STEP 1: Open the Portal
─────────────────────────
https://landrecords.karnataka.gov.in/Service2/

STEP 2: Select Location
─────────────────────────
District : BANGALORE RURAL
Taluk    : HOSKOTE
Hobli    : [Select one by one]
Village  : [Select one by one]

STEP 3: Search
─────────────────────────
- Leave "Survey Number" BLANK
- Click "Go" button
- This will show ALL RTCs for that village

STEP 4: Find Owner
─────────────────────────
- Press Ctrl+F (or Cmd+F on Mac)
- Search for: ಚೌಡೆ ಬೈರೆ ಗೌಡ
- Note down any matching Survey Numbers

STEP 5: Repeat
─────────────────────────
Go through all 294 villages in Hoskote Taluk systematically.

╠══════════════════════════════════════════════════════════════════════════════╣
║  HOBLI LIST (5 hoblis, 294 villages total):                                  ║
║                                                                              ║
║  1. ಕಸಬಾ (Kasaba) - 41 villages                                             ║
║  2. ಅನುಗೊಂಡನಹಳ್ಳಿ (Anugondanahalli) - 42 villages                            ║
║  3. ಜಡಿಗೇನಹಳ್ಳಿ (Jadigenahalli) - 69 villages                               ║
║  4. ಸೂಲಿಬೆಲೆ (Sulibele) - 69 villages                                       ║
║  5. ನಂದಗುಡಿ (Nandagudi) - 73 villages                                       ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  TIP: Use the generated 'hoskote_villages.json' file to track your progress  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description=f'Search for owner: {OWNER_NAME_KANNADA} in Hoskote Taluk',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--generate', '-g', action='store_true',
                       help='Generate village list JSON and search URLs')
    parser.add_argument('--guide', action='store_true',
                       help='Show manual search guide')
    parser.add_argument('--browser', '-b', action='store_true',
                       help='Run browser automation (requires Selenium)')
    parser.add_argument('--headless', action='store_true',
                       help='Run browser in headless mode')
    
    args = parser.parse_args()
    
    if args.generate:
        generate_village_search_data()
    elif args.guide:
        print_manual_search_guide()
    elif args.browser:
        if not SELENIUM_AVAILABLE:
            print("Selenium not installed!")
            print("Install with: pip install selenium")
            print("\nAlternatively, use --generate to create a village list for manual search")
            return
        
        automation = BhoomiPortalAutomation(headless=args.headless)
        try:
            automation.start()
            automation.navigate_to_portal()
            # Add more automation logic here
            input("Press Enter to close browser...")
        finally:
            automation.stop()
    else:
        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║     OWNER SEARCH SCRIPT                                                      ║
║     Owner: {OWNER_NAME_KANNADA}                                             ║
╠══════════════════════════════════════════════════════════════════════════════╣

Options:
  --generate, -g    Generate village list JSON and search URLs
  --guide           Show manual search guide
  --browser, -b     Run browser automation (requires Selenium)
  
Example:
  python owner_search_browser.py --generate
  python owner_search_browser.py --guide
  
╚══════════════════════════════════════════════════════════════════════════════╝
""")


if __name__ == "__main__":
    main()

