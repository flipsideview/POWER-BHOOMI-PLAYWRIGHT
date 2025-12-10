# 🏛️ POWER-BHOOMI v2.0 | Parallel Search Engine

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/Flask-2.3+-green.svg" alt="Flask">
  <img src="https://img.shields.io/badge/Selenium-4.0+-orange.svg" alt="Selenium">
  <img src="https://img.shields.io/badge/Platform-Windows%20%7C%20macOS%20%7C%20Linux-lightgrey.svg" alt="Platform">
</p>

A powerful browser-based tool for searching land records (RTC) across Karnataka using the official Bhoomi Portal. Features **4x faster parallel search** with multiple browser workers!

## ✨ Features

- ⚡ **Parallel Search Engine** - 4 browser workers running simultaneously (4x faster)
- 🌐 **Web-Based Interface** - Beautiful dark-themed UI accessible via browser
- 📍 **Cascading Dropdowns** - District → Taluk → Hobli → Village (live data from eChawadi API)
- 🔍 **Owner Search** - Search by owner name in Kannada or English
- 📊 **Real-time Records Table** - See all fetched records as they're found
- 🎯 **Match Highlighting** - Instantly view records matching your search
- 📥 **CSV Export** - Export all records or matching records
- 🇮🇳 **Kannada Support** - Full Kannada language support
- 🌍 **All Hoblis/Villages** - Search across all locations at once

---

## 🚀 Windows 11 Installation (Step-by-Step)

### Step 1: Install Python

1. Download Python from: https://www.python.org/downloads/
2. **IMPORTANT**: During installation, check ✅ **"Add Python to PATH"**
3. Click "Install Now"
4. Verify installation by opening **Command Prompt** (Win + R → type `cmd` → Enter):
   ```cmd
   python --version
   ```
   Should show: `Python 3.11.x` or similar

### Step 2: Install Google Chrome

1. Download from: https://www.google.com/chrome/
2. Install Chrome (it will be auto-detected by the tool)

### Step 3: Clone the Repository

Open **Command Prompt** (Win + R → type `cmd` → Enter):

```cmd
cd %USERPROFILE%\Desktop
git clone https://github.com/flipsideview/POWER-BHOOMI.git
cd POWER-BHOOMI
```

**If you don't have Git installed:**
1. Download from: https://git-scm.com/download/win
2. Install with default options
3. Close and reopen Command Prompt, then run the commands above

### Step 4: Create Virtual Environment

```cmd
python -m venv venv
```

### Step 5: Activate Virtual Environment

```cmd
venv\Scripts\activate
```

You should see `(venv)` at the beginning of your command prompt.

### Step 6: Install Dependencies

```cmd
pip install -r requirements.txt
```

### Step 7: Run the Application

**For Standard Version:**
```cmd
python bhoomi_web_app.py
```

**For Parallel Search Version (4x Faster):**
```cmd
python bhoomi_web_app_v2.py
```

### Step 8: Open in Browser

Open your browser and go to: **http://localhost:5001**

---

## 📋 Quick Reference (Windows)

```cmd
:: Navigate to project folder
cd %USERPROFILE%\Desktop\POWER-BHOOMI

:: Activate virtual environment
venv\Scripts\activate

:: Run parallel version (recommended)
python bhoomi_web_app_v2.py
```

### Create a Start Script (Optional)

Create a file named `START.bat` on your Desktop:

```batch
@echo off
cd %USERPROFILE%\Desktop\POWER-BHOOMI
call venv\Scripts\activate
echo Starting POWER-BHOOMI v2.0...
echo.
echo Open browser to: http://localhost:5001
echo.
python bhoomi_web_app_v2.py
pause
```

Double-click `START.bat` to launch the application!

---

## 🖥️ macOS/Linux Installation

```bash
# Clone the repository
git clone https://github.com/flipsideview/POWER-BHOOMI.git
cd POWER-BHOOMI

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run parallel version
python bhoomi_web_app_v2.py
```

Open browser: **http://localhost:5001**

---

## 📖 How to Use

1. **Enter Owner Name** - Type the name in Kannada (ಕನ್ನಡ) or English
2. **Select District** - Choose from dropdown
3. **Select Taluk** - Loads automatically after district
4. **Select Hobli** - Choose specific or "All Hoblis"
5. **Select Village** - Choose specific or "All Villages"
6. **Set Max Survey** - Number of survey numbers to check (default: 200)
7. **Start Search** - Click "⚡ Start Parallel Search"
8. **View Results** - Click tabs to switch between:
   - 📋 **All Records** - Every land record found
   - 🎯 **Matches** - Only records matching your search name
9. **Export** - Click "📥 Export CSV" to download results

---

## 📁 Project Structure

```
POWER-BHOOMI/
├── bhoomi_web_app_v2.py    # Parallel search version (recommended)
├── bhoomi_web_app.py       # Standard single-browser version
├── bhoomi_bulk_downloader.py # API utilities
├── Bhoomi_Owner_Search.ipynb # Jupyter notebook version
├── requirements.txt        # Python dependencies
├── README.md              # This file
└── .gitignore             # Git ignore rules
```

---

## 🔧 Troubleshooting

### "python is not recognized"
- Reinstall Python and check ✅ "Add Python to PATH"

### "No module named flask"
- Make sure virtual environment is activated: `venv\Scripts\activate`
- Run: `pip install -r requirements.txt`

### Chrome doesn't open
- Make sure Google Chrome is installed
- The tool uses `webdriver-manager` to auto-download ChromeDriver

### Port 5001 already in use
- Close other applications using port 5001
- Or change port in the Python file

### Search is slow
- Use `bhoomi_web_app_v2.py` for 4x parallel search
- Reduce "Max Survey Number" to search fewer surveys

---

## 🌐 API Endpoints (Internal)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main web interface |
| `/api/districts` | GET | Get all Karnataka districts |
| `/api/taluks/<district_code>` | GET | Get taluks for a district |
| `/api/hoblis/<dist>/<taluk>` | GET | Get hoblis for a taluk |
| `/api/villages/<dist>/<taluk>/<hobli>` | GET | Get villages for a hobli |
| `/api/search/start` | POST | Start owner search |
| `/api/search/status` | GET | Get search status & records |
| `/api/search/stop` | POST | Stop current search |

---

## ⚠️ Disclaimer

This tool is for **educational and research purposes only**. It accesses publicly available data from the Karnataka Bhoomi portal. Please use responsibly and respect the portal's terms of service.

---

## 📝 License

MIT License - feel free to use and modify.

---

<p align="center">
  <strong>POWER-BHOOMI v2.0</strong> | Made with ❤️ for Karnataka Land Records
</p>
