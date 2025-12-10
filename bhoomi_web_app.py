#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                   KARNATAKA BHOOMI OWNER SEARCH TOOL                         ║
║                      World-Class Browser-Based Interface                      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  A sophisticated tool for searching land records across Karnataka             ║
║  Uses official Bhoomi Portal APIs for authentic data                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Author: Bhoomi Search Tool
Version: 2.0.0
"""

from flask import Flask, render_template_string, jsonify, request
from flask_cors import CORS
import requests
import json
import time
import logging
from datetime import datetime
import urllib3
import threading
import queue

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# API CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

ECHAWADI_BASE = "https://rdservices.karnataka.gov.in/echawadi/Home"
SERVICE2_URL = "https://landrecords.karnataka.gov.in/Service2/"

# Global search state
search_state = {
    'running': False,
    'progress': 0,
    'current_location': '',
    'records_found': 0,
    'matches_found': 0,
    'all_records': [],
    'matches': [],
    'log': []
}

# ═══════════════════════════════════════════════════════════════════════════════
# BHOOMI API CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class BhoomiAPI:
    """Client for Karnataka Bhoomi eChawadi API"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/json; charset=utf-8',
        })
    
    def _make_request(self, endpoint, data=None, method='POST'):
        """Make API request and handle double-encoded JSON"""
        url = f"{ECHAWADI_BASE}/{endpoint}"
        try:
            if method == 'GET':
                response = self.session.get(url, verify=False, timeout=30)
            else:
                response = self.session.post(url, json=data, verify=False, timeout=30)
            
            result = response.text
            # Handle double-encoded JSON (common in .NET APIs)
            if result.startswith('"') and result.endswith('"'):
                result = json.loads(result)
            if isinstance(result, str):
                result = json.loads(result)
            return result
        except Exception as e:
            logger.error(f"API Error: {e}")
            return None
    
    def get_districts(self):
        """Fetch all districts"""
        result = self._make_request('LoadDistrict', method='GET')
        if result and 'data' in result:
            return sorted(result['data'], key=lambda x: x.get('district_name_kn', ''))
        return []
    
    def get_taluks(self, district_code):
        """Fetch taluks for a district"""
        result = self._make_request('LoadTaluk', {'pDistCode': str(district_code)})
        if result and 'data' in result:
            return sorted(result['data'], key=lambda x: x.get('taluka_name_kn', ''))
        return []
    
    def get_hoblis(self, district_code, taluk_code):
        """Fetch hoblis for a taluk"""
        result = self._make_request('LoadHobli', {
            'pDistCode': str(district_code),
            'pTalukCode': str(taluk_code)
        })
        if result and 'data' in result:
            return sorted(result['data'], key=lambda x: x.get('hobli_name_kn', ''))
        return []
    
    def get_villages(self, district_code, taluk_code, hobli_code):
        """Fetch villages for a hobli"""
        result = self._make_request('LoadVillage', {
            'pDistCode': str(district_code),
            'pTalukCode': str(taluk_code),
            'pHobliCode': str(hobli_code)
        })
        if result and 'data' in result:
            return sorted(result['data'], key=lambda x: x.get('village_name_kn', ''))
        return []

api = BhoomiAPI()

# ═══════════════════════════════════════════════════════════════════════════════
# BEAUTIFUL HTML TEMPLATE
# ═══════════════════════════════════════════════════════════════════════════════

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ಭೂಮಿ | Karnataka Land Records Search</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=Noto+Sans+Kannada:wght@400;500;600;700&display=swap" rel="stylesheet">
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
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Outfit', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            background-image: 
                radial-gradient(ellipse at 20% 20%, rgba(245, 158, 11, 0.08) 0%, transparent 50%),
                radial-gradient(ellipse at 80% 80%, rgba(217, 119, 6, 0.05) 0%, transparent 50%),
                linear-gradient(180deg, var(--bg-primary) 0%, #0f172a 100%);
        }
        
        .kannada {
            font-family: 'Noto Sans Kannada', sans-serif;
        }
        
        /* Header */
        .header {
            padding: 1.5rem 2rem;
            background: linear-gradient(180deg, rgba(26, 35, 50, 0.9) 0%, transparent 100%);
            border-bottom: 1px solid rgba(245, 158, 11, 0.1);
            position: sticky;
            top: 0;
            z-index: 100;
            backdrop-filter: blur(20px);
        }
        
        .header-content {
            max-width: 1400px;
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
            width: 50px;
            height: 50px;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.5rem;
            box-shadow: 0 4px 20px var(--accent-glow);
        }
        
        .logo-text h1 {
            font-size: 1.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-primary), #fcd34d);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: -0.5px;
        }
        
        .logo-text p {
            font-size: 0.75rem;
            color: var(--text-secondary);
            letter-spacing: 2px;
            text-transform: uppercase;
        }
        
        .govt-badge {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            padding: 0.5rem 1rem;
            background: rgba(16, 185, 129, 0.1);
            border: 1px solid rgba(16, 185, 129, 0.3);
            border-radius: 8px;
        }
        
        .govt-badge span {
            font-size: 0.85rem;
            color: var(--success);
        }
        
        /* Main Container */
        .main-container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
            display: grid;
            grid-template-columns: 400px 1fr;
            gap: 2rem;
        }
        
        /* Search Panel */
        .search-panel {
            background: var(--bg-card);
            border-radius: 20px;
            padding: 2rem;
            border: 1px solid var(--border-color);
            height: fit-content;
            position: sticky;
            top: 100px;
        }
        
        .panel-title {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        
        .panel-title::before {
            content: '';
            width: 4px;
            height: 24px;
            background: var(--accent-primary);
            border-radius: 2px;
        }
        
        .form-group {
            margin-bottom: 1.25rem;
        }
        
        .form-label {
            display: block;
            font-size: 0.85rem;
            font-weight: 500;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .form-select, .form-input {
            width: 100%;
            padding: 0.875rem 1rem;
            background: var(--bg-input);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            color: var(--text-primary);
            font-family: inherit;
            font-size: 0.95rem;
            transition: all 0.2s ease;
            appearance: none;
            cursor: pointer;
        }
        
        .form-select {
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='24' height='24' viewBox='0 0 24 24' fill='none' stroke='%239ca3af' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 1rem center;
            background-size: 18px;
            padding-right: 2.5rem;
        }
        
        .form-select:focus, .form-input:focus {
            outline: none;
            border-color: var(--accent-primary);
            box-shadow: 0 0 0 3px var(--accent-glow);
        }
        
        .form-select:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        .form-select option {
            background: var(--bg-secondary);
            color: var(--text-primary);
            padding: 0.5rem;
        }
        
        /* Search Input */
        .search-input-wrapper {
            position: relative;
        }
        
        .search-input-wrapper .form-input {
            padding-left: 3rem;
        }
        
        .search-input-wrapper::before {
            content: '🔍';
            position: absolute;
            left: 1rem;
            top: 50%;
            transform: translateY(-50%);
            font-size: 1.1rem;
        }
        
        /* Buttons */
        .btn-primary {
            width: 100%;
            padding: 1rem 1.5rem;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
            border: none;
            border-radius: 12px;
            color: var(--bg-primary);
            font-family: inherit;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 1px;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            margin-top: 1.5rem;
        }
        
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 30px var(--accent-glow);
        }
        
        .btn-primary:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none;
        }
        
        .btn-stop {
            background: linear-gradient(135deg, var(--error), #dc2626);
        }
        
        /* Results Panel */
        .results-panel {
            background: var(--bg-card);
            border-radius: 20px;
            padding: 2rem;
            border: 1px solid var(--border-color);
        }
        
        .results-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1.5rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border-color);
        }
        
        /* Stats Cards */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        
        .stat-card {
            background: var(--bg-input);
            border-radius: 12px;
            padding: 1.25rem;
            text-align: center;
            border: 1px solid var(--border-color);
        }
        
        .stat-value {
            font-size: 2rem;
            font-weight: 700;
            color: var(--accent-primary);
            line-height: 1;
        }
        
        .stat-label {
            font-size: 0.75rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-top: 0.5rem;
        }
        
        /* Progress */
        .progress-container {
            margin-bottom: 1.5rem;
        }
        
        .progress-info {
            display: flex;
            justify-content: space-between;
            margin-bottom: 0.5rem;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }
        
        .progress-bar {
            height: 8px;
            background: var(--bg-input);
            border-radius: 4px;
            overflow: hidden;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary));
            border-radius: 4px;
            transition: width 0.3s ease;
            width: 0%;
        }
        
        .current-location {
            font-size: 0.9rem;
            color: var(--text-muted);
            padding: 0.75rem;
            background: var(--bg-input);
            border-radius: 8px;
            margin-top: 0.75rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        /* Results Table */
        .results-table-container {
            overflow-x: auto;
            border-radius: 12px;
            border: 1px solid var(--border-color);
        }
        
        .results-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        
        .results-table th {
            background: var(--bg-secondary);
            padding: 1rem;
            text-align: left;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.5px;
            border-bottom: 1px solid var(--border-color);
            position: sticky;
            top: 0;
        }
        
        .results-table td {
            padding: 0.875rem 1rem;
            border-bottom: 1px solid var(--border-color);
            color: var(--text-primary);
        }
        
        .results-table tr:hover td {
            background: rgba(245, 158, 11, 0.05);
        }
        
        .results-table tr.match td {
            background: rgba(16, 185, 129, 0.1);
        }
        
        .owner-name {
            font-weight: 500;
            color: var(--text-primary);
        }
        
        .owner-name.match {
            color: var(--success);
            font-weight: 600;
        }
        
        /* Empty State */
        .empty-state {
            text-align: center;
            padding: 4rem 2rem;
            color: var(--text-muted);
        }
        
        .empty-state-icon {
            font-size: 4rem;
            margin-bottom: 1rem;
            opacity: 0.5;
        }
        
        .empty-state h3 {
            font-size: 1.25rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }
        
        /* Log Container */
        .log-container {
            margin-top: 1rem;
            max-height: 200px;
            overflow-y: auto;
            background: var(--bg-input);
            border-radius: 8px;
            padding: 1rem;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.8rem;
        }
        
        .log-entry {
            padding: 0.25rem 0;
            color: var(--text-muted);
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        
        .log-entry.success {
            color: var(--success);
        }
        
        .log-entry.error {
            color: var(--error);
        }
        
        /* Scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: var(--bg-input);
        }
        
        ::-webkit-scrollbar-thumb {
            background: var(--border-color);
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: var(--text-muted);
        }
        
        /* Loading Spinner */
        .spinner {
            width: 20px;
            height: 20px;
            border: 2px solid transparent;
            border-top-color: currentColor;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        /* Tabs */
        .tabs {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }
        
        .tab {
            padding: 0.75rem 1.5rem;
            background: transparent;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            color: var(--text-secondary);
            font-family: inherit;
            font-size: 0.9rem;
            cursor: pointer;
            transition: all 0.2s ease;
        }
        
        .tab.active {
            background: var(--accent-primary);
            border-color: var(--accent-primary);
            color: var(--bg-primary);
            font-weight: 600;
        }
        
        .tab:hover:not(.active) {
            border-color: var(--accent-primary);
            color: var(--text-primary);
        }
        
        /* Badge */
        .badge {
            display: inline-block;
            padding: 0.25rem 0.5rem;
            background: var(--accent-primary);
            color: var(--bg-primary);
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            margin-left: 0.5rem;
        }
        
        /* Responsive */
        @media (max-width: 1200px) {
            .main-container {
                grid-template-columns: 1fr;
            }
            
            .search-panel {
                position: static;
            }
            
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }
        
        @media (max-width: 768px) {
            .header-content {
                flex-direction: column;
                gap: 1rem;
            }
            
            .main-container {
                padding: 1rem;
            }
            
            .search-panel, .results-panel {
                padding: 1.5rem;
            }
        }
    </style>
</head>
<body>
    <header class="header">
        <div class="header-content">
            <div class="logo">
                <div class="logo-icon">🏛️</div>
                <div class="logo-text">
                    <h1 class="kannada">ಭೂಮಿ</h1>
                    <p>Karnataka Land Records</p>
                </div>
            </div>
            <div class="govt-badge">
                <span>✓</span>
                <span>Official Data Source</span>
            </div>
        </div>
    </header>
    
    <main class="main-container">
        <aside class="search-panel">
            <h2 class="panel-title">Search Land Records</h2>
            
            <div class="form-group">
                <label class="form-label">Owner Name <span class="kannada">(ಮಾಲೀಕರ ಹೆಸರು)</span></label>
                <div class="search-input-wrapper">
                    <input type="text" id="ownerName" class="form-input kannada" placeholder="Enter owner name...">
                </div>
            </div>
            
            <div class="form-group">
                <label class="form-label">District <span class="kannada">(ಜಿಲ್ಲೆ)</span></label>
                <select id="district" class="form-select">
                    <option value="">Loading districts...</option>
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
            
            <button id="searchBtn" class="btn-primary">
                <span>🔍</span>
                <span>Start Search</span>
            </button>
        </aside>
        
        <section class="results-panel">
            <div class="results-header">
                <h2 class="panel-title">Search Results</h2>
                <button id="exportBtn" class="btn-primary" style="width: auto; margin: 0; padding: 0.75rem 1.5rem;">
                    📥 Export CSV
                </button>
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value" id="totalRecords">0</div>
                    <div class="stat-label">Total Records</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="matchCount">0</div>
                    <div class="stat-label">Matches Found</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="villagesSearched">0</div>
                    <div class="stat-label">Villages</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="surveysChecked">0</div>
                    <div class="stat-label">Surveys</div>
                </div>
            </div>
            
            <div class="progress-container" id="progressContainer" style="display: none;">
                <div class="progress-info">
                    <span>Search Progress</span>
                    <span id="progressPercent">0%</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" id="progressFill"></div>
                </div>
                <div class="current-location" id="currentLocation">
                    <span class="spinner"></span>
                    <span>Starting search...</span>
                </div>
            </div>
            
            <div class="tabs">
                <button class="tab active" data-tab="matches">Matches <span class="badge" id="matchBadge">0</span></button>
                <button class="tab" data-tab="all">All Records <span class="badge" id="allBadge">0</span></button>
                <button class="tab" data-tab="log">Activity Log</button>
            </div>
            
            <div id="matchesTab" class="tab-content">
                <div class="results-table-container" style="max-height: 500px; overflow-y: auto;">
                    <table class="results-table">
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
                        </tbody>
                    </table>
                </div>
                <div class="empty-state" id="matchesEmpty">
                    <div class="empty-state-icon">🔍</div>
                    <h3>No matches found yet</h3>
                    <p>Start a search to find matching land records</p>
                </div>
            </div>
            
            <div id="allTab" class="tab-content" style="display: none;">
                <div class="results-table-container" style="max-height: 500px; overflow-y: auto;">
                    <table class="results-table">
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
                        <tbody id="allBody">
                        </tbody>
                    </table>
                </div>
                <div class="empty-state" id="allEmpty">
                    <div class="empty-state-icon">📋</div>
                    <h3>No records fetched yet</h3>
                    <p>Start a search to collect land records</p>
                </div>
            </div>
            
            <div id="logTab" class="tab-content" style="display: none;">
                <div class="log-container" id="logContainer">
                    <div class="log-entry">Ready to search...</div>
                </div>
            </div>
        </section>
    </main>
    
    <script>
        // State
        let searchRunning = false;
        let allRecords = [];
        let matches = [];
        let currentTab = 'matches';
        
        // DOM Elements
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
                if (hobliCode) loadVillages(distCode, talukCode, hobliCode);
                else resetDropdowns(['village']);
            });
            
            searchBtn.addEventListener('click', toggleSearch);
            
            // Tab switching
            document.querySelectorAll('.tab').forEach(tab => {
                tab.addEventListener('click', () => switchTab(tab.dataset.tab));
            });
            
            // Export
            document.getElementById('exportBtn').addEventListener('click', exportCSV);
        }
        
        async function loadDistricts() {
            try {
                const res = await fetch('/api/districts');
                const data = await res.json();
                
                districtSelect.innerHTML = '<option value="">Select District</option>';
                data.forEach(d => {
                    const name = d.district_name_kn || d.district_name || d.district_code;
                    districtSelect.innerHTML += `<option value="${d.district_code}">${name}</option>`;
                });
            } catch (e) {
                console.error('Error loading districts:', e);
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
                    const name = t.taluka_name_kn || t.taluka_name || t.taluka_code;
                    talukSelect.innerHTML += `<option value="${t.taluka_code}">${name}</option>`;
                });
                talukSelect.disabled = false;
            } catch (e) {
                console.error('Error loading taluks:', e);
            }
        }
        
        async function loadHoblis(distCode, talukCode) {
            resetDropdowns(['hobli', 'village']);
            hobliSelect.innerHTML = '<option value="">Loading...</option>';
            
            try {
                const res = await fetch(`/api/hoblis/${distCode}/${talukCode}`);
                const data = await res.json();
                
                hobliSelect.innerHTML = '<option value="">Select Hobli</option>';
                // Add "All Hoblis" option
                hobliSelect.innerHTML += '<option value="all">🔍 All Hoblis (Search Entire Taluk)</option>';
                data.forEach(h => {
                    const name = h.hobli_name_kn || h.hobli_name || h.hobli_code;
                    hobliSelect.innerHTML += `<option value="${h.hobli_code}">${name}</option>`;
                });
                hobliSelect.disabled = false;
            } catch (e) {
                console.error('Error loading hoblis:', e);
            }
        }
        
        async function loadVillages(distCode, talukCode, hobliCode) {
            resetDropdowns(['village']);
            
            // If "All Hoblis" selected, set village to "all" automatically
            if (hobliCode === 'all') {
                villageSelect.innerHTML = '<option value="all">🔍 All Villages (All Hoblis)</option>';
                villageSelect.disabled = false;
                return;
            }
            
            villageSelect.innerHTML = '<option value="">Loading...</option>';
            
            try {
                const res = await fetch(`/api/villages/${distCode}/${talukCode}/${hobliCode}`);
                const data = await res.json();
                
                villageSelect.innerHTML = '<option value="">Select Village</option>';
                // Add "All Villages" option
                villageSelect.innerHTML += '<option value="all">🔍 All Villages (in this Hobli)</option>';
                data.forEach(v => {
                    const name = v.village_name_kn || v.village_name || v.village_code;
                    villageSelect.innerHTML += `<option value="${v.village_code}">${name}</option>`;
                });
                villageSelect.disabled = false;
            } catch (e) {
                console.error('Error loading villages:', e);
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
                alert('Please enter an owner name to search');
                return;
            }
            
            const districtCode = districtSelect.value;
            const talukCode = talukSelect.value;
            const hobliCode = hobliSelect.value;
            const villageCode = villageSelect.value;
            const maxSurvey = parseInt(maxSurveyInput.value) || 200;
            
            if (!districtCode || !talukCode) {
                alert('Please select at least District and Taluk');
                return;
            }
            
            // If hobli not selected, default to "all"
            const finalHobliCode = hobliCode || 'all';
            const finalVillageCode = villageCode || 'all';
            
            searchRunning = true;
            allRecords = [];
            matches = [];
            
            searchBtn.innerHTML = '<span class="spinner"></span><span>Stop Search</span>';
            searchBtn.classList.add('btn-stop');
            document.getElementById('progressContainer').style.display = 'block';
            
            addLog('Starting search for: ' + ownerName);
            
            try {
                const res = await fetch('/api/search/start', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        owner_name: ownerName,
                        district_code: districtCode,
                        taluk_code: talukCode,
                        hobli_code: finalHobliCode,
                        village_code: finalVillageCode,
                        max_survey: maxSurvey
                    })
                });
                
                // Poll for updates
                pollSearchStatus();
                
            } catch (e) {
                console.error('Search error:', e);
                addLog('Error: ' + e.message, 'error');
                stopSearch();
            }
        }
        
        async function pollSearchStatus() {
            if (!searchRunning) return;
            
            try {
                const res = await fetch('/api/search/status');
                const status = await res.json();
                
                // Update UI
                document.getElementById('totalRecords').textContent = status.records_found;
                document.getElementById('matchCount').textContent = status.matches_found;
                document.getElementById('progressFill').style.width = status.progress + '%';
                document.getElementById('progressPercent').textContent = status.progress + '%';
                document.getElementById('currentLocation').innerHTML = 
                    `<span class="spinner"></span><span>${status.current_location || 'Searching...'}</span>`;
                
                // Update badges
                document.getElementById('matchBadge').textContent = status.matches_found;
                document.getElementById('allBadge').textContent = status.records_found;
                
                // Update tables
                if (status.all_records && status.all_records.length > 0) {
                    allRecords = status.all_records;
                    updateTable('allBody', allRecords);
                    document.getElementById('allEmpty').style.display = 'none';
                }
                
                if (status.matches && status.matches.length > 0) {
                    matches = status.matches;
                    updateTable('matchesBody', matches, true);
                    document.getElementById('matchesEmpty').style.display = 'none';
                }
                
                // Add logs
                if (status.log && status.log.length > 0) {
                    const logContainer = document.getElementById('logContainer');
                    status.log.forEach(log => {
                        addLog(log);
                    });
                }
                
                if (status.running) {
                    setTimeout(pollSearchStatus, 2000);
                } else {
                    searchComplete();
                }
                
            } catch (e) {
                console.error('Poll error:', e);
                setTimeout(pollSearchStatus, 3000);
            }
        }
        
        function searchComplete() {
            searchRunning = false;
            searchBtn.innerHTML = '<span>🔍</span><span>Start Search</span>';
            searchBtn.classList.remove('btn-stop');
            document.getElementById('currentLocation').innerHTML = 
                '<span>✅</span><span>Search complete!</span>';
            addLog('Search completed!', 'success');
        }
        
        async function stopSearch() {
            try {
                await fetch('/api/search/stop', {method: 'POST'});
            } catch (e) {}
            searchComplete();
        }
        
        function updateTable(tableId, records, isMatch = false) {
            const tbody = document.getElementById(tableId);
            tbody.innerHTML = records.map(r => `
                <tr class="${isMatch ? 'match' : ''}">
                    <td>${r.village || ''}</td>
                    <td>${r.survey_no || ''}</td>
                    <td>${r.hissa || ''}</td>
                    <td class="owner-name ${isMatch ? 'match' : ''} kannada">${r.owner_name || ''}</td>
                    <td>${r.extent || ''}</td>
                    <td>${r.khatah || ''}</td>
                </tr>
            `).join('');
        }
        
        function switchTab(tab) {
            currentTab = tab;
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelector(`[data-tab="${tab}"]`).classList.add('active');
            
            document.querySelectorAll('.tab-content').forEach(c => c.style.display = 'none');
            document.getElementById(tab + 'Tab').style.display = 'block';
        }
        
        function addLog(message, type = '') {
            const container = document.getElementById('logContainer');
            const entry = document.createElement('div');
            entry.className = 'log-entry ' + type;
            entry.textContent = new Date().toLocaleTimeString() + ' - ' + message;
            container.insertBefore(entry, container.firstChild);
            
            // Limit log entries
            while (container.children.length > 100) {
                container.removeChild(container.lastChild);
            }
        }
        
        function exportCSV() {
            const data = currentTab === 'matches' ? matches : allRecords;
            if (data.length === 0) {
                alert('No data to export');
                return;
            }
            
            const headers = ['district', 'taluk', 'hobli', 'village', 'survey_no', 'surnoc', 'hissa', 'period', 'owner_name', 'extent', 'khatah', 'timestamp'];
            const csv = [
                headers.join(','),
                ...data.map(r => headers.map(h => `"${(r[h] || '').toString().replace(/"/g, '""')}"`).join(','))
            ].join('\\n');
            
            const blob = new Blob([csv], {type: 'text/csv'});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `bhoomi_${currentTab}_${new Date().toISOString().slice(0,10)}.csv`;
            a.click();
        }
    </script>
</body>
</html>
'''

# ═══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

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
    """Start a new search in background thread"""
    global search_state
    
    data = request.json
    
    # Reset state
    search_state = {
        'running': True,
        'progress': 0,
        'current_location': 'Starting...',
        'records_found': 0,
        'matches_found': 0,
        'all_records': [],
        'matches': [],
        'log': [f"Search started for: {data.get('owner_name')}"]
    }
    
    # Start background search thread
    thread = threading.Thread(target=background_search, args=(data,))
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'started'})

@app.route('/api/search/status')
def search_status():
    """Get current search status"""
    return jsonify(search_state)

@app.route('/api/search/stop', methods=['POST'])
def stop_search():
    """Stop the current search"""
    global search_state
    search_state['running'] = False
    return jsonify({'status': 'stopped'})

def background_search(params):
    """Background search using Selenium"""
    global search_state
    
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        from bs4 import BeautifulSoup
        import re
        
        owner_name = params.get('owner_name', '')
        owner_variants = [owner_name, owner_name.upper(), owner_name.lower()]
        max_survey = params.get('max_survey', 200)
        
        # Element IDs
        IDS = {
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
        
        def extract_owners(page_source):
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
                                if re.search(r'\d+\.\d+\.\d+', row_text):
                                    owners.append({
                                        'owner_name': cell_texts[0] if cell_texts else '',
                                        'extent': cell_texts[1] if len(cell_texts) > 1 else '',
                                        'khatah': cell_texts[2] if len(cell_texts) > 2 else '',
                                    })
            except:
                pass
            return owners
        
        # Start browser
        search_state['log'].append("Starting Chrome browser...")
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        
        try:
            driver.get(SERVICE2_URL)
            time.sleep(5)
            
            # Get location names from dropdowns
            dist_sel = Select(driver.find_element(By.ID, IDS['district']))
            dist_opts = {str(int(float(o.get_attribute('value')))): o.text for o in dist_sel.options if o.get_attribute('value')}
            
            district_name = dist_opts.get(params.get('district_code', ''), 'Unknown')
            
            # Select district
            dist_sel.select_by_value(params['district_code'])
            time.sleep(3)
            
            # Select taluk
            taluk_sel = Select(driver.find_element(By.ID, IDS['taluk']))
            taluk_opts = {str(int(float(o.get_attribute('value')))): o.text for o in taluk_sel.options if o.get_attribute('value')}
            taluk_name = taluk_opts.get(params.get('taluk_code', ''), 'Unknown')
            taluk_sel.select_by_value(params['taluk_code'])
            time.sleep(3)
            
            # Get all hoblis for this taluk
            hobli_sel = Select(driver.find_element(By.ID, IDS['hobli']))
            all_hoblis = [(o.get_attribute('value'), o.text) for o in hobli_sel.options 
                         if o.get_attribute('value') and 'Select' not in o.text]
            
            # Filter hoblis based on selection
            hobli_code_param = params.get('hobli_code', 'all')
            if hobli_code_param == 'all':
                hoblis_to_search = all_hoblis
                search_state['log'].append(f"Searching ALL {len(hoblis_to_search)} hoblis in {taluk_name}")
            else:
                hoblis_to_search = [(h, n) for h, n in all_hoblis if h == hobli_code_param]
            
            # Build list of all villages to search
            all_villages_to_search = []
            for hobli_code, hobli_name in hoblis_to_search:
                driver.get(SERVICE2_URL)
                time.sleep(2)
                Select(driver.find_element(By.ID, IDS['district'])).select_by_value(params['district_code'])
                time.sleep(2)
                Select(driver.find_element(By.ID, IDS['taluk'])).select_by_value(params['taluk_code'])
                time.sleep(2)
                Select(driver.find_element(By.ID, IDS['hobli'])).select_by_value(hobli_code)
                time.sleep(2)
                
                village_sel = Select(driver.find_element(By.ID, IDS['village']))
                villages_in_hobli = [(o.get_attribute('value'), o.text, hobli_code, hobli_name) 
                                    for o in village_sel.options 
                                    if o.get_attribute('value') and 'Select' not in o.text]
                
                # Filter villages if specific one selected
                village_code_param = params.get('village_code', 'all')
                if village_code_param != 'all' and village_code_param:
                    villages_in_hobli = [(v, vn, h, hn) for v, vn, h, hn in villages_in_hobli if v == village_code_param]
                
                all_villages_to_search.extend(villages_in_hobli)
            
            total_villages = len(all_villages_to_search)
            search_state['log'].append(f"Total villages to search: {total_villages}")
            
            for vi, (village_code, village_name, hobli_code, hobli_name) in enumerate(all_villages_to_search):
                if not search_state['running']:
                    break
                
                search_state['current_location'] = f"{district_name} > {taluk_name} > {hobli_name} > {village_name}"
                search_state['log'].append(f"Searching village: {village_name}")
                
                empty_count = 0
                
                for survey_no in range(1, max_survey + 1):
                    if not search_state['running']:
                        break
                    
                    try:
                        driver.get(SERVICE2_URL)
                        time.sleep(2)
                        
                        Select(driver.find_element(By.ID, IDS['district'])).select_by_value(params['district_code'])
                        time.sleep(2)
                        Select(driver.find_element(By.ID, IDS['taluk'])).select_by_value(params['taluk_code'])
                        time.sleep(2)
                        Select(driver.find_element(By.ID, IDS['hobli'])).select_by_value(hobli_code)
                        time.sleep(2)
                        Select(driver.find_element(By.ID, IDS['village'])).select_by_value(village_code)
                        time.sleep(2)
                        
                        driver.find_element(By.ID, IDS['survey_no']).send_keys(str(survey_no))
                        
                        # Click GO using JavaScript
                        go_btn = driver.find_element(By.ID, IDS['go_btn'])
                        driver.execute_script("arguments[0].click();", go_btn)
                        time.sleep(8)
                        
                        surnoc_sel = Select(driver.find_element(By.ID, IDS['surnoc']))
                        surnoc_opts = [o.text for o in surnoc_sel.options if "Select" not in o.text]
                        
                        if not surnoc_opts:
                            empty_count += 1
                            if empty_count > 30:
                                break
                            continue
                        
                        empty_count = 0
                        
                        for surnoc in surnoc_opts:
                            if not search_state['running']:
                                break
                            
                            surnoc_sel = Select(driver.find_element(By.ID, IDS['surnoc']))
                            surnoc_sel.select_by_visible_text(surnoc)
                            time.sleep(3)
                            
                            hissa_sel = Select(driver.find_element(By.ID, IDS['hissa']))
                            hissa_opts = [o.text for o in hissa_sel.options if "Select" not in o.text]
                            
                            for hissa in hissa_opts:
                                if not search_state['running']:
                                    break
                                
                                try:
                                    hissa_sel = Select(driver.find_element(By.ID, IDS['hissa']))
                                    hissa_sel.select_by_visible_text(hissa)
                                    time.sleep(2)
                                    
                                    period_sel = Select(driver.find_element(By.ID, IDS['period']))
                                    period_opts = [o.text for o in period_sel.options if "Select" not in o.text]
                                    if period_opts:
                                        period_sel.select_by_visible_text(period_opts[0])
                                        time.sleep(1)
                                    
                                    # Click Fetch Details
                                    fetch_btn = driver.find_element(By.ID, IDS['fetch_btn'])
                                    driver.execute_script("arguments[0].click();", fetch_btn)
                                    time.sleep(6)
                                    
                                    owners = extract_owners(driver.page_source)
                                    
                                    for owner in owners:
                                        record = {
                                            'district': district_name,
                                            'taluk': taluk_name,
                                            'hobli': hobli_name,
                                            'village': village_name,
                                            'survey_no': survey_no,
                                            'surnoc': surnoc,
                                            'hissa': hissa,
                                            'period': period_opts[0] if period_opts else '',
                                            'owner_name': owner['owner_name'],
                                            'extent': owner['extent'],
                                            'khatah': owner['khatah'],
                                            'timestamp': datetime.now().isoformat()
                                        }
                                        
                                        search_state['all_records'].append(record)
                                        search_state['records_found'] = len(search_state['all_records'])
                                        
                                        # Check for match
                                        if any(v in owner['owner_name'] for v in owner_variants):
                                            search_state['matches'].append(record)
                                            search_state['matches_found'] = len(search_state['matches'])
                                            search_state['log'].append(f"🎯 MATCH: {owner['owner_name']} in Survey {survey_no}")
                                    
                                    # Reload for next hissa
                                    driver.get(SERVICE2_URL)
                                    time.sleep(2)
                                    Select(driver.find_element(By.ID, IDS['district'])).select_by_value(params['district_code'])
                                    time.sleep(2)
                                    Select(driver.find_element(By.ID, IDS['taluk'])).select_by_value(params['taluk_code'])
                                    time.sleep(2)
                                    Select(driver.find_element(By.ID, IDS['hobli'])).select_by_value(hobli_code)
                                    time.sleep(2)
                                    Select(driver.find_element(By.ID, IDS['village'])).select_by_value(village_code)
                                    time.sleep(2)
                                    driver.find_element(By.ID, IDS['survey_no']).send_keys(str(survey_no))
                                    go_btn = driver.find_element(By.ID, IDS['go_btn'])
                                    driver.execute_script("arguments[0].click();", go_btn)
                                    time.sleep(5)
                                    Select(driver.find_element(By.ID, IDS['surnoc'])).select_by_visible_text(surnoc)
                                    time.sleep(2)
                                    
                                except Exception:
                                    continue
                    
                    except Exception:
                        empty_count += 1
                        if empty_count > 30:
                            break
                
                # Update progress
                search_state['progress'] = int((vi + 1) / total_villages * 100)
        
        finally:
            driver.quit()
        
        search_state['running'] = False
        search_state['log'].append("✅ Search complete!")
        
    except Exception as e:
        search_state['running'] = False
        search_state['log'].append(f"Error: {str(e)}")
        logger.error(f"Search error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("""
    ╔══════════════════════════════════════════════════════════════════════════════╗
    ║                   KARNATAKA BHOOMI OWNER SEARCH TOOL                         ║
    ╠══════════════════════════════════════════════════════════════════════════════╣
    ║                                                                              ║
    ║   🌐 Open your browser and navigate to:                                      ║
    ║                                                                              ║
    ║       http://localhost:5001                                                  ║
    ║                                                                              ║
    ╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    app.run(debug=True, host='0.0.0.0', port=5001)

