# 🏛️ POWER-BHOOMI | Karnataka Land Records Search Tool

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/Flask-2.3+-green.svg" alt="Flask">
  <img src="https://img.shields.io/badge/Selenium-4.0+-orange.svg" alt="Selenium">
  <img src="https://img.shields.io/badge/Platform-Windows%20%7C%20macOS%20%7C%20Linux-lightgrey.svg" alt="Platform">
</p>

A powerful browser-based tool for searching land records (RTC) across Karnataka using the official Bhoomi Portal APIs.

## ✨ Features

- 🌐 **Web-Based Interface** - Beautiful dark-themed UI accessible via browser
- 📍 **Cascading Dropdowns** - District → Taluk → Hobli → Village (live data from eChawadi API)
- 🔍 **Owner Search** - Search by owner name in Kannada or English
- 📊 **Real-time Progress** - Live updates during search
- 📥 **CSV Export** - Export all records or matching records
- 🇮🇳 **Kannada Support** - Full Kannada language support

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Google Chrome browser
- Internet connection

### Installation

```bash
# Clone the repository
git clone https://github.com/flipsideview/POWER-BHOOMI.git
cd POWER-BHOOMI

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the application
python bhoomi_web_app.py
```

### Open in Browser

Navigate to: **http://localhost:5001**

## 📖 Usage

1. **Enter Owner Name** - Type the name in Kannada (ಕನ್ನಡ) or English
2. **Select Location** - Choose District → Taluk → Hobli → Village
3. **Set Max Survey** - Number of survey numbers to check (default: 200)
4. **Start Search** - Click the search button
5. **View Results** - See matches in real-time
6. **Export** - Download results as CSV

## 🖼️ Screenshots

### Main Interface
![Bhoomi Search Tool](screenshots/main_interface.png)

### Search Results
![Search Results](screenshots/search_results.png)

## 📁 Project Structure

```
POWER-BHOOMI/
├── bhoomi_web_app.py       # Main Flask web application
├── bhoomi_bulk_downloader.py # API utilities & bulk download
├── Bhoomi_Owner_Search.ipynb # Jupyter notebook version
├── requirements.txt        # Python dependencies
├── README.md              # This file
└── .gitignore             # Git ignore rules
```

## 🔧 Configuration

Edit the following in `bhoomi_web_app.py`:

```python
# Change port (default: 5001)
app.run(debug=True, host='0.0.0.0', port=5001)
```

## 🌐 API Endpoints (Internal)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main web interface |
| `/api/districts` | GET | Get all Karnataka districts |
| `/api/taluks/<district_code>` | GET | Get taluks for a district |
| `/api/hoblis/<dist>/<taluk>` | GET | Get hoblis for a taluk |
| `/api/villages/<dist>/<taluk>/<hobli>` | GET | Get villages for a hobli |
| `/api/search/start` | POST | Start owner search |
| `/api/search/status` | GET | Get search status |
| `/api/search/stop` | POST | Stop current search |

## 🛠️ Technologies Used

- **Backend**: Flask, Python
- **Frontend**: HTML5, CSS3, JavaScript
- **Automation**: Selenium WebDriver
- **Data Source**: Karnataka eChawadi API
- **Styling**: Custom CSS with Kannada font support

## ⚠️ Disclaimer

This tool is for **educational and research purposes only**. It accesses publicly available data from the Karnataka Bhoomi portal. Please use responsibly and respect the portal's terms of service.

## 📝 License

MIT License - feel free to use and modify.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📞 Support

For issues or questions, please open a GitHub issue.

---

<p align="center">
  Made with ❤️ for Karnataka Land Records
</p>
