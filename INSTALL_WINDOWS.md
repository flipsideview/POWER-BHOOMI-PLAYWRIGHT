# POWER-BHOOMI Windows Installation Guide

## 📋 Prerequisites

Before installing, ensure you have:

- **Windows 10 or later** (Windows 11 recommended)
- **Python 3.8 or higher** ([Download here](https://www.python.org/downloads/))
- **Git for Windows** ([Download here](https://git-scm.com/download/win)) - Optional, for cloning repository
- **At least 8GB RAM** (16GB recommended for optimal performance)
- **2GB free disk space**
- **Stable internet connection**

---

## 🚀 Step-by-Step Installation

### Step 1: Install Python

1. Download Python from [python.org](https://www.python.org/downloads/)
2. **IMPORTANT**: During installation, check ✅ **"Add Python to PATH"**
3. Click "Install Now"
4. Verify installation:
   ```cmd
   python --version
   ```
   Should show: `Python 3.8.x` or higher

### Step 2: Download POWER-BHOOMI

**Option A: Using Git (Recommended)**
```cmd
git clone https://github.com/flipsideview/POWER-BHOOMI-PLAYWRIGHT.git
cd POWER-BHOOMI-PLAYWRIGHT
```

**Option B: Download ZIP**
1. Go to: https://github.com/flipsideview/POWER-BHOOMI-PLAYWRIGHT
2. Click "Code" → "Download ZIP"
3. Extract ZIP to a folder (e.g., `C:\POWER-BHOOMI-PLAYWRIGHT`)
4. Open Command Prompt in that folder

### Step 3: Create Virtual Environment

Open **Command Prompt** or **PowerShell** in the project folder:

```cmd
python -m venv venv
```

This creates a `venv` folder with isolated Python environment.

### Step 4: Activate Virtual Environment

**In Command Prompt:**
```cmd
venv\Scripts\activate
```

**In PowerShell:**
```powershell
venv\Scripts\Activate.ps1
```

If you get an execution policy error in PowerShell, run:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

You should see `(venv)` at the start of your command prompt.

### Step 5: Upgrade Pip

```cmd
python -m pip install --upgrade pip
```

### Step 6: Install Python Dependencies

```cmd
pip install -r requirements_playwright.txt
```

This will install:
- Flask (web framework)
- Playwright (browser automation)
- BeautifulSoup4 (HTML parsing)
- And other required packages

**Note:** This may take 5-10 minutes depending on your internet speed.

### Step 7: Install Playwright Browsers

Playwright needs browser binaries. Install Chromium:

```cmd
playwright install chromium
```

**Important:** This downloads ~200MB of browser files. Ensure stable internet connection.

### Step 8: Verify Installation

Check that everything is installed:

```cmd
python -c "import playwright; import flask; print('✓ All dependencies installed')"
```

---

## 🎯 Running the Application

### Start the Server

Make sure you're in the project folder with virtual environment activated:

```cmd
python Bhoomi_playwright_windows.py
```

You should see:
```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║   POWER-BHOOMI v6.2 WINDOWS - NETWORK RECOVERY (WAIT+RETRY) (12 WORKERS)            ║
...
 * Running on http://127.0.0.1:5001
```

### Open in Browser

Open your web browser and go to:
```
http://localhost:5001
```

You should see the POWER-BHOOMI interface.

---

## 📝 Quick Start Guide

1. **Enter Owner Name**: Type the landowner's name you want to search
2. **Select Location**: Choose District, Taluk, Hobli, and Village
3. **Set Survey Range**: Default is 1-1000 (adjust if needed)
4. **Click "Start Search"**: The application will begin searching
5. **Monitor Progress**: Watch real-time progress in the dashboard
6. **Download Results**: Click "Download CSV" when search completes

---

## 🔧 Troubleshooting

### Issue: "python is not recognized"

**Solution:**
- Python is not in PATH. Reinstall Python and check ✅ "Add Python to PATH"
- Or use full path: `C:\Python39\python.exe` (adjust version number)

### Issue: "playwright install" fails

**Solution:**
- Check internet connection
- Try: `playwright install chromium --with-deps`
- If still fails, download manually from Playwright website

### Issue: "Port 5001 already in use"

**Solution:**
- Another instance is running. Close it first:
  ```cmd
  netstat -ano | findstr :5001
  taskkill /PID <PID_NUMBER> /F
  ```
- Or change port in the code (search for `5001` and replace)

### Issue: Chrome processes not closing

**Solution:**
- The Windows version handles this automatically
- If stuck, manually kill:
  ```cmd
  taskkill /F /IM chrome.exe
  taskkill /F /IM chromedriver.exe
  ```

### Issue: "ModuleNotFoundError"

**Solution:**
- Virtual environment not activated. Run: `venv\Scripts\activate`
- Or reinstall dependencies: `pip install -r requirements_playwright.txt`

### Issue: Network errors during search

**Solution:**
- v6.2 automatically waits for network recovery (up to 5 minutes)
- Check your internet connection
- The application will retry automatically when network is restored

---

## 📂 Project Structure

```
POWER-BHOOMI-PLAYWRIGHT/
├── Bhoomi_playwright_windows.py  ← Windows version (USE THIS)
├── bhoomi_playwright_v6.py        ← macOS/Linux version
├── requirements_playwright.txt    ← Python dependencies
├── venv/                          ← Virtual environment (created)
└── config.yaml                    ← Configuration file
```

---

## ⚙️ Configuration

Edit `config.yaml` to customize:
- Number of workers (default: 12)
- Timeouts
- Retry settings
- Database location

**Note:** Default settings are optimized for production use.

---

## 🗄️ Data Storage

- **Database**: `C:\Users\<YourUsername>\Documents\POWER-BHOOMI\bhoomi_data.db`
- **CSV Exports**: Downloaded from web interface
- **Logs**: Displayed in console and web interface

---

## 🛑 Stopping the Application

Press `Ctrl+C` in the command prompt to stop the server.

Or close the command prompt window (will stop the server).

---

## 🔄 Updating

To update to latest version:

```cmd
git pull origin main
```

Then reinstall dependencies if needed:
```cmd
pip install -r requirements_playwright.txt --upgrade
playwright install chromium
```

---

## 📞 Support

- **GitHub Issues**: https://github.com/flipsideview/POWER-BHOOMI-PLAYWRIGHT/issues
- **Documentation**: See `ARCHITECTURE.md` for technical details

---

## ✅ Installation Checklist

- [ ] Python 3.8+ installed with PATH enabled
- [ ] Project folder downloaded/cloned
- [ ] Virtual environment created (`venv`)
- [ ] Virtual environment activated (see `(venv)` in prompt)
- [ ] Dependencies installed (`pip install -r requirements_playwright.txt`)
- [ ] Playwright browsers installed (`playwright install chromium`)
- [ ] Application starts without errors
- [ ] Browser opens to `http://localhost:5001`

---

## 🎉 You're Ready!

Once you see the POWER-BHOOMI interface in your browser, you're all set to start searching land records!

**Happy Searching! 🚀**

