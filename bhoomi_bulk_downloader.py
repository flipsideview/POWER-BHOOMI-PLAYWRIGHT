#!/usr/bin/env python3
"""
Karnataka Bhoomi Land Records Bulk Downloader
============================================
This script provides bulk download capabilities for Karnataka land records
from the official Bhoomi portal using discovered API endpoints.

Key APIs Discovered:
- eChawadi API: https://rdservices.karnataka.gov.in/echawadi/Home/
- BhoomiMaps API: https://rdservices.karnataka.gov.in/BhoomiMaps/
- Main RTC Service: https://landrecords.karnataka.gov.in/Service2/

Author: Bulk Downloader for Karnataka Land Records
"""

import requests
import json
import os
import time
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path
import urllib3

# Disable SSL warnings (some govt sites have certificate issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class LocationData:
    """Data class for location hierarchy"""
    district_code: int
    district_name: str
    taluk_code: Optional[int] = None
    taluk_name: Optional[str] = None
    hobli_code: Optional[int] = None
    hobli_name: Optional[str] = None
    village_code: Optional[int] = None
    village_name: Optional[str] = None


class BhoomiAPI:
    """
    Karnataka Bhoomi Land Records API Client
    
    Discovered API Endpoints:
    - eChawadi: For RTC and mutation data
    - BhoomiMaps: For sketches and maps
    - Service2: Main RTC portal (ASP.NET WebForms)
    """
    
    BASE_URLS = {
        'echawadi': 'https://rdservices.karnataka.gov.in/echawadi/Home',
        'bhoomimaps': 'https://rdservices.karnataka.gov.in/BhoomiMaps',
        'service2': 'https://landrecords.karnataka.gov.in/Service2',
        'service64': 'https://landrecords.karnataka.gov.in/service64',  # Khata Extract
        'recordroom': 'https://recordroom.karnataka.gov.in/Service4',  # Bhu Suraksha
    }
    
    def __init__(self, output_dir: str = "./downloads"):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def _make_request(self, url: str, method: str = 'GET', 
                      data: Optional[Dict] = None, json_data: Optional[Dict] = None,
                      verify: bool = False) -> Optional[Dict]:
        """Make HTTP request and parse JSON response"""
        try:
            if method == 'GET':
                response = self.session.get(url, verify=verify, timeout=30)
            else:
                if json_data:
                    response = self.session.post(
                        url, 
                        json=json_data,
                        headers={'Content-Type': 'application/json; charset=utf-8'},
                        verify=verify,
                        timeout=30
                    )
                elif data:
                    response = self.session.post(url, data=data, verify=verify, timeout=30)
                else:
                    response = self.session.post(url, verify=verify, timeout=30)
            
            response.raise_for_status()
            
            # Handle double-encoded JSON (common in .NET APIs)
            result = response.text
            if result.startswith('"') and result.endswith('"'):
                result = json.loads(result)
            if isinstance(result, str):
                result = json.loads(result)
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return None

    # =================== District/Taluk/Hobli/Village APIs ===================
    
    def get_all_districts(self) -> List[Dict]:
        """
        Fetch all districts from Karnataka
        
        API: GET https://rdservices.karnataka.gov.in/echawadi/Home/LoadDistrict
        
        Returns list of: {district_code, district_name_kn}
        """
        url = f"{self.BASE_URLS['echawadi']}/LoadDistrict"
        result = self._make_request(url)
        
        if result and 'data' in result:
            logger.info(f"Found {len(result['data'])} districts")
            return result['data']
        return []
    
    def get_taluks(self, district_code: int) -> List[Dict]:
        """
        Fetch all taluks for a district
        
        API: POST https://rdservices.karnataka.gov.in/echawadi/Home/LoadTaluk
        Params: {pDistCode: "<code>"}
        
        Returns list of: {taluka_code, taluka_name_kn}
        """
        url = f"{self.BASE_URLS['echawadi']}/LoadTaluk"
        result = self._make_request(url, method='POST', 
                                   json_data={'pDistCode': str(district_code)})
        
        if result and 'data' in result:
            logger.info(f"Found {len(result['data'])} taluks for district {district_code}")
            return result['data']
        return []
    
    def get_hoblis(self, district_code: int, taluk_code: int) -> List[Dict]:
        """
        Fetch all hoblis for a taluk
        
        API: POST https://rdservices.karnataka.gov.in/echawadi/Home/LoadHobli
        Params: {pDistCode: "<code>", pTalukCode: "<code>"}
        
        Returns list of: {hobli_code, hobli_name_kn}
        """
        url = f"{self.BASE_URLS['echawadi']}/LoadHobli"
        result = self._make_request(url, method='POST', 
                                   json_data={
                                       'pDistCode': str(district_code),
                                       'pTalukCode': str(taluk_code)
                                   })
        
        if result and 'data' in result:
            logger.info(f"Found {len(result['data'])} hoblis for taluk {taluk_code}")
            return result['data']
        return []
    
    def get_villages(self, district_code: int, taluk_code: int, hobli_code: int) -> List[Dict]:
        """
        Fetch all villages for a hobli
        
        API: POST https://rdservices.karnataka.gov.in/echawadi/Home/LoadVillage
        Params: {pDistCode, pTalukCode, pHobliCode}
        
        Returns list of: {village_code, village_name_kn, LGDCODE}
        """
        url = f"{self.BASE_URLS['echawadi']}/LoadVillage"
        result = self._make_request(url, method='POST', 
                                   json_data={
                                       'pDistCode': str(district_code),
                                       'pTalukCode': str(taluk_code),
                                       'pHobliCode': str(hobli_code)
                                   })
        
        if result and 'data' in result:
            logger.info(f"Found {len(result['data'])} villages for hobli {hobli_code}")
            return result['data']
        return []

    # =================== RTC/Survey Data APIs ===================
    
    def get_village_survey_data(self, village_code: int) -> Optional[List]:
        """
        Get all survey numbers and coordinates for a village
        
        API: POST https://rdservices.karnataka.gov.in/echawadi/Home/FnVlgSurveyNoData
        Params: {VlgwardCode: "<code>"}
        
        Returns survey polygon data with coordinates
        """
        url = f"{self.BASE_URLS['echawadi']}/FnVlgSurveyNoData"
        try:
            response = self.session.post(
                url,
                data={'VlgwardCode': str(village_code)},
                verify=False,
                timeout=60
            )
            result = response.text
            if result and result != '""':
                if result.startswith('"'):
                    result = json.loads(result)
                return json.loads(result) if isinstance(result, str) else result
        except Exception as e:
            logger.error(f"Error getting survey data: {e}")
        return None
    
    def get_rtc_report(self, report_params: Dict) -> Optional[bytes]:
        """
        Get RTC report from API
        
        API: POST https://rdservices.karnataka.gov.in/echawadi/Home/GetReportsFromAPI
        
        Params (as FormData):
        - Report_Type: 12 (RTC) or 21 (other)
        - censusDistCode, censusTalukCode
        - distCode, talukCode, hobliCode, villageCode
        - yearCode, tranNo, MutationType, AcquiType
        """
        url = f"{self.BASE_URLS['echawadi']}/GetReportsFromAPI"
        try:
            response = self.session.post(url, data=report_params, verify=False, timeout=120)
            if response.status_code == 200:
                return response.content
        except Exception as e:
            logger.error(f"Error getting RTC report: {e}")
        return None

    # =================== Bulk Download Methods ===================
    
    def build_location_hierarchy(self) -> Dict:
        """
        Build complete hierarchy of all locations in Karnataka
        
        Structure: {
            district_code: {
                'name': district_name,
                'taluks': {
                    taluk_code: {
                        'name': taluk_name,
                        'hoblis': {
                            hobli_code: {
                                'name': hobli_name,
                                'villages': [{village_code, village_name, LGDCODE}, ...]
                            }
                        }
                    }
                }
            }
        }
        """
        hierarchy = {}
        
        # Get all districts
        districts = self.get_all_districts()
        
        for district in districts:
            dist_code = int(district['district_code'])
            dist_name = district['district_name_kn']
            
            hierarchy[dist_code] = {
                'name': dist_name,
                'taluks': {}
            }
            
            # Get taluks for this district
            taluks = self.get_taluks(dist_code)
            time.sleep(0.5)  # Rate limiting
            
            for taluk in taluks:
                tlk_code = int(taluk['taluka_code'])
                tlk_name = taluk['taluka_name_kn']
                
                hierarchy[dist_code]['taluks'][tlk_code] = {
                    'name': tlk_name,
                    'hoblis': {}
                }
                
                # Get hoblis for this taluk
                hoblis = self.get_hoblis(dist_code, tlk_code)
                time.sleep(0.3)
                
                for hobli in hoblis:
                    hbl_code = int(hobli['hobli_code'])
                    hbl_name = hobli['hobli_name_kn']
                    
                    # Get villages for this hobli
                    villages = self.get_villages(dist_code, tlk_code, hbl_code)
                    time.sleep(0.3)
                    
                    hierarchy[dist_code]['taluks'][tlk_code]['hoblis'][hbl_code] = {
                        'name': hbl_name,
                        'villages': villages
                    }
            
            logger.info(f"Completed district: {dist_name}")
        
        return hierarchy
    
    def save_hierarchy_to_json(self, filename: str = "karnataka_locations.json"):
        """Save the complete location hierarchy to JSON file"""
        hierarchy = self.build_location_hierarchy()
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(hierarchy, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved location hierarchy to {filepath}")
        return filepath
    
    def download_village_survey_data(self, district_code: int, taluk_code: int, 
                                     hobli_code: int, village_code: int,
                                     village_name: str = "unknown"):
        """Download survey data for a specific village"""
        survey_data = self.get_village_survey_data(village_code)
        
        if survey_data:
            filename = f"survey_{district_code}_{taluk_code}_{hobli_code}_{village_code}.json"
            filepath = self.output_dir / "surveys" / filename
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(survey_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved survey data for village {village_name}")
            return filepath
        
        return None


class BhoomiDistricts:
    """
    Static district code mapping from Service2
    These are the codes used in the main Bhoomi RTC portal
    """
    CODES = {
        'BELAGAVI': 1,
        'BAGALKOTE': 2,
        'VIJAYAPURA': 3,
        'KALABURAGI': 4,
        'BIDAR': 5,
        'RAICHUR': 6,
        'KOPPAL': 7,
        'GADAG': 8,
        'DHARWAD': 9,
        'UTTAR_KANNADA': 10,
        'HAVERI': 11,
        'BALLARI': 12,
        'CHITRADURGA': 13,
        'DAVANAGERE': 14,
        'SHIVAMOGGA': 15,
        'UDUPI': 16,
        'CHIKKAMAGALURU': 17,
        'TUMAKURU': 18,
        'KOLAR': 19,
        'BENGALURU': 20,
        'BANGALORE_RURAL': 21,
        'MANDYA': 22,
        'HASSAN': 23,
        'DAKSHINA_KANNADA': 24,
        'KODAGU': 25,
        'MYSORE': 26,
        'CHAMARAJANAGARA': 27,
        'CHIKKABALLAPUR': 28,
        'RAMANAGARA': 29,  # Also BENGALURU_SOUTH
        'YADAGIR': 30,
        'VIJAYANAGARA': 31,
    }
    
    @classmethod
    def get_code(cls, district_name: str) -> int:
        """Get district code by name"""
        key = district_name.upper().replace(' ', '_')
        return cls.CODES.get(key, 0)


def print_api_documentation():
    """Print discovered API documentation"""
    doc = """
    ╔══════════════════════════════════════════════════════════════════════════════╗
    ║          KARNATAKA BHOOMI LAND RECORDS - DISCOVERED API DATABASE             ║
    ╠══════════════════════════════════════════════════════════════════════════════╣
    ║                                                                              ║
    ║  BASE SERVICES:                                                              ║
    ║  ─────────────                                                               ║
    ║  • Main RTC Portal:     https://landrecords.karnataka.gov.in/Service2/       ║
    ║  • eChawadi Service:    https://rdservices.karnataka.gov.in/echawadi/        ║
    ║  • BhoomiMaps (Beta):   https://rdservices.karnataka.gov.in/BhoomiMaps/      ║
    ║  • Survey Sketch:       https://rdservices.karnataka.gov.in/service84/       ║
    ║  • Khata Extract:       https://landrecords.karnataka.gov.in/service64/      ║
    ║  • Record Room (Bhu Suraksha): https://recordroom.karnataka.gov.in/Service4  ║
    ║  • Mojini (Survey Doc): https://bhoomojini.karnataka.gov.in/oscitizen/       ║
    ║                                                                              ║
    ╠══════════════════════════════════════════════════════════════════════════════╣
    ║                                                                              ║
    ║  API ENDPOINTS (eChawadi - No Auth Required):                                ║
    ║  ───────────────────────────────────────────                                 ║
    ║                                                                              ║
    ║  1. GET Districts:                                                           ║
    ║     GET /echawadi/Home/LoadDistrict                                          ║
    ║     Response: {"data": [{"district_code": 1, "district_name_kn": "..."}]}    ║
    ║                                                                              ║
    ║  2. GET Taluks:                                                              ║
    ║     POST /echawadi/Home/LoadTaluk                                            ║
    ║     Body: {"pDistCode": "20"}                                                ║
    ║     Response: {"data": [{"taluka_code": 1, "taluka_name_kn": "..."}]}        ║
    ║                                                                              ║
    ║  3. GET Hoblis:                                                              ║
    ║     POST /echawadi/Home/LoadHobli                                            ║
    ║     Body: {"pDistCode": "20", "pTalukCode": "1"}                             ║
    ║     Response: {"data": [{"hobli_code": 1, "hobli_name_kn": "..."}]}          ║
    ║                                                                              ║
    ║  4. GET Villages:                                                            ║
    ║     POST /echawadi/Home/LoadVillage                                          ║
    ║     Body: {"pDistCode": "20", "pTalukCode": "1", "pHobliCode": "1"}          ║
    ║     Response: {"data": [{"village_code": 11, "village_name_kn": "...",       ║
    ║                          "LGDCODE": 938315}]}                                ║
    ║                                                                              ║
    ║  5. GET Survey Data (GeoJSON-like):                                          ║
    ║     POST /echawadi/Home/FnVlgSurveyNoData                                    ║
    ║     Body: VlgwardCode=<village_code>                                         ║
    ║     Response: [[{Vlg, VlgCode, SurveyNo, Lat, Lng}, ...], ...]               ║
    ║                                                                              ║
    ║  6. GET RTC Reports (PDF):                                                   ║
    ║     POST /echawadi/Home/GetReportsFromAPI                                    ║
    ║     Body (FormData): Report_Type, distCode, talukCode, hobliCode,            ║
    ║                      villageCode, yearCode, tranNo, etc.                     ║
    ║                                                                              ║
    ╠══════════════════════════════════════════════════════════════════════════════╣
    ║                                                                              ║
    ║  SERVICES REQUIRING AUTHENTICATION (OTP):                                    ║
    ║  ─────────────────────────────────────────                                   ║
    ║  • Mojini (Survey Documents) - Mobile OTP                                    ║
    ║  • Bhu Suraksha (Record Room) - Mobile OTP                                   ║
    ║  • i-RTC Wallet - Aadhaar/Mobile OTP                                         ║
    ║                                                                              ║
    ╠══════════════════════════════════════════════════════════════════════════════╣
    ║                                                                              ║
    ║  DISTRICT CODES (from Service2):                                             ║
    ║  ───────────────────────────────                                             ║
    ║  BELAGAVI=1, BAGALKOTE=2, VIJAYAPURA=3, KALABURAGI=4, BIDAR=5               ║
    ║  RAICHUR=6, KOPPAL=7, GADAG=8, DHARWAD=9, UTTAR_KANNADA=10                   ║
    ║  HAVERI=11, BALLARI=12, CHITRADURGA=13, DAVANAGERE=14, SHIVAMOGGA=15         ║
    ║  UDUPI=16, CHIKKAMAGALURU=17, TUMAKURU=18, KOLAR=19, BENGALURU=20            ║
    ║  BANGALORE_RURAL=21, MANDYA=22, HASSAN=23, DAKSHINA_KANNADA=24               ║
    ║  KODAGU=25, MYSORE=26, CHAMARAJANAGARA=27, CHIKKABALLAPUR=28                 ║
    ║  RAMANAGARA/BENGALURU_SOUTH=29, YADAGIR=30, VIJAYANAGARA=31                  ║
    ║                                                                              ║
    ╚══════════════════════════════════════════════════════════════════════════════╝
    """
    print(doc)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Karnataka Bhoomi Land Records Bulk Downloader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python bhoomi_bulk_downloader.py --docs              # Show API documentation
  python bhoomi_bulk_downloader.py --districts         # List all districts
  python bhoomi_bulk_downloader.py --taluks 20         # List taluks in Bengaluru
  python bhoomi_bulk_downloader.py --build-hierarchy   # Build full location JSON
        """
    )
    
    parser.add_argument('--docs', action='store_true', 
                       help='Show discovered API documentation')
    parser.add_argument('--districts', action='store_true',
                       help='Fetch and display all districts')
    parser.add_argument('--taluks', type=int, metavar='DIST_CODE',
                       help='Fetch taluks for a district code')
    parser.add_argument('--hoblis', nargs=2, type=int, metavar=('DIST', 'TALUK'),
                       help='Fetch hoblis for district and taluk')
    parser.add_argument('--villages', nargs=3, type=int, metavar=('DIST', 'TALUK', 'HOBLI'),
                       help='Fetch villages for district, taluk, and hobli')
    parser.add_argument('--survey', type=int, metavar='VILLAGE_CODE',
                       help='Fetch survey data for a village')
    parser.add_argument('--build-hierarchy', action='store_true',
                       help='Build complete location hierarchy JSON (takes time)')
    parser.add_argument('--output', default='./downloads',
                       help='Output directory (default: ./downloads)')
    
    args = parser.parse_args()
    
    if args.docs:
        print_api_documentation()
        return
    
    api = BhoomiAPI(output_dir=args.output)
    
    if args.districts:
        districts = api.get_all_districts()
        print("\n" + "="*60)
        print("KARNATAKA DISTRICTS")
        print("="*60)
        for d in districts:
            print(f"  Code: {int(d['district_code']):2d} | Name: {d['district_name_kn']}")
        print("="*60)
        print(f"Total: {len(districts)} districts")
        
    elif args.taluks:
        taluks = api.get_taluks(args.taluks)
        print(f"\nTaluks in District {args.taluks}:")
        print("-"*50)
        for t in taluks:
            print(f"  Code: {int(t['taluka_code']):2d} | Name: {t['taluka_name_kn']}")
            
    elif args.hoblis:
        hoblis = api.get_hoblis(args.hoblis[0], args.hoblis[1])
        print(f"\nHoblis in District {args.hoblis[0]}, Taluk {args.hoblis[1]}:")
        print("-"*50)
        for h in hoblis:
            print(f"  Code: {int(h['hobli_code']):2d} | Name: {h['hobli_name_kn']}")
            
    elif args.villages:
        villages = api.get_villages(args.villages[0], args.villages[1], args.villages[2])
        print(f"\nVillages in District {args.villages[0]}, Taluk {args.villages[1]}, Hobli {args.villages[2]}:")
        print("-"*70)
        for v in villages:
            lgd = int(v.get('LGDCODE', 0))
            print(f"  Code: {int(v['village_code']):4d} | LGD: {lgd:6d} | Name: {v['village_name_kn']}")
            
    elif args.survey:
        print(f"\nFetching survey data for village {args.survey}...")
        filepath = api.download_village_survey_data(0, 0, 0, args.survey)
        if filepath:
            print(f"Saved to: {filepath}")
        else:
            print("No survey data found or error occurred")
            
    elif args.build_hierarchy:
        print("\nBuilding complete location hierarchy...")
        print("This will take several minutes. Please wait...")
        filepath = api.save_hierarchy_to_json()
        print(f"\nHierarchy saved to: {filepath}")
        
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

