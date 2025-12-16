# 🔄 Resume Search Guide - POWER-BHOOMI

**Your searches are automatically saved and can be resumed anytime!**

---

## 📁 Where Your Searches Are Saved

### Primary Storage Location:
```
/Users/sks/Documents/POWER-BHOOMI/bhoomi_data.db
```

**Database Size:** Currently 8.4 MB  
**Total Records:** 6,593 land records  
**Total Sessions:** 16 search sessions

### Backup CSV Files:
```
~/Downloads/bhoomi_search_YYYYMMDD_HHMMSS_all.csv
~/Downloads/bhoomi_search_YYYYMMDD_HHMMSS_matches.csv
```

---

## 🔍 Your Current Sessions

Based on your database, here are your search sessions:

| Session ID | Owner Search | Status | Records | Villages |
|------------|-------------|--------|---------|----------|
| search_20251216_173639 | god | running | 0 | 31 pending |
| search_20251216_170513 | rao | running | 0 | 1 pending |
| search_20251216_135702 | SHIVA | stopped | 655 | completed |
| search_20251216_135642 | SHIVA | running | 0 | 1 pending |
| search_20251216_134118 | shiva | running | 0 | 1 pending |
| search_20251216_131025 | SHIVA | running | 0 | 1 pending |

**Note:** Sessions marked "running" were interrupted and can be resumed!

---

## 🎯 How to Resume a Search

### Method 1: Using the Web Interface (Recommended)

#### Step 1: Check Available Sessions
```bash
# View all resumable sessions
curl http://localhost:5001/api/db/resumable | python3 -m json.tool
```

#### Step 2: Open Browser
```
http://localhost:5001
```

#### Step 3: Look for Resume Button
The web interface should have a "Resume Search" option that shows:
- Previous search sessions
- Progress status
- Option to continue

**Note:** The current UI may not have the resume button visible. See Method 2 below.

---

## 📊 Method 2: Check Progress via Database

### View Session Details:

```bash
# Get details of a specific session
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT * FROM search_sessions 
WHERE session_id = 'search_20251216_170513_6e463bee';
"
```

### View Village Progress:

```bash
# See which villages were completed/pending
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT 
    village_name,
    status,
    last_survey_no,
    max_survey_no,
    records_found,
    started_at
FROM village_progress
WHERE session_id = 'search_20251216_170513_6e463bee'
ORDER BY id;
"
```

### View Records Found:

```bash
# See all records from a specific session
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT 
    village,
    survey_no,
    hissa,
    owner_name,
    extent
FROM land_records
WHERE session_id = 'search_20251216_170513_6e463bee'
LIMIT 20;
"
```

---

## 🔄 Method 3: Manual Resume (Most Reliable)

Since automatic resume may not be fully implemented in the UI, here's how to continue manually:

### Step 1: Identify What Was Searched

```bash
# Find out which survey numbers were already checked
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT 
    village,
    COUNT(DISTINCT survey_no) as surveys_checked,
    MIN(survey_no) as first_survey,
    MAX(survey_no) as last_survey
FROM land_records
WHERE session_id = 'YOUR_SESSION_ID'
GROUP BY village;
"
```

### Step 2: Start New Search from Last Survey

1. Open http://localhost:5001
2. Select same location (District, Taluk, Hobli, Village)
3. Instead of starting from Survey 1, start from the **last survey + 1**
4. Set max survey to 200

**Example:**
- If last survey checked was 45
- Start new search from survey 46
- This avoids duplicate processing

---

## 📈 Understanding Session Status

### Session Statuses:

| Status | Meaning | Can Resume? |
|--------|---------|-------------|
| **running** | Search was interrupted | ✅ Yes |
| **crashed** | Browser/system crashed | ✅ Yes |
| **completed** | Finished successfully | ❌ No need |
| **stopped** | User stopped manually | ✅ Yes |

### Progress Tracking:

The database tracks:
- ✅ Which surveys were checked
- ✅ How many records found
- ✅ Last survey number processed
- ✅ Which villages completed
- ✅ Which villages pending

**All progress is saved in real-time!** No data loss even if browser crashes.

---

## 🎯 Most Recent Searches Analysis

### Search 1: Owner "god" - 31 Villages
```
Session: search_20251216_173639_df4c6fcc
Status: running (interrupted)
Progress: 0 records (likely just started)
Villages: 31 pending
```

**To Resume:**
- Same search parameters
- All 31 villages still need to be searched

### Search 2: Owner "rao" - 1 Village  
```
Session: search_20251216_170513_6e463bee
Status: running (interrupted)
Progress: 0 records
Villages: 1 pending (UDYAVARA)
```

**To Resume:**
- This is the search we were monitoring earlier
- Village UDYAVARA needs to be completed
- Started at Survey 1, check database for last survey

### Search 3: Owner "SHIVA" - SUCCESS ✅
```
Session: search_20251216_135702_43b06827
Status: stopped
Progress: 655 records found, 1 match!
```

**This search completed successfully!** View results:

```bash
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT village, survey_no, owner_name, extent
FROM land_records
WHERE session_id = 'search_20251216_135702_43b06827'
AND is_match = 1;
"
```

---

## 💡 Pro Tips

### 1. Check Last Survey Before Resuming

```bash
# Find the last survey processed for a village
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT 
    village,
    MAX(survey_no) as last_survey,
    COUNT(*) as records
FROM land_records
WHERE session_id = 'YOUR_SESSION_ID'
  AND village = 'YOUR_VILLAGE'
GROUP BY village;
"
```

### 2. Export Session Data to CSV

```bash
# Export all records from a session
sqlite3 -header -csv ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT * FROM land_records
WHERE session_id = 'YOUR_SESSION_ID'
" > ~/Downloads/session_export.csv
```

### 3. View Search Summary

```bash
# Get quick stats for a session
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT 
    session_id,
    owner_name,
    COUNT(*) as total_records,
    SUM(is_match) as matches,
    COUNT(DISTINCT village) as villages_searched,
    COUNT(DISTINCT survey_no) as surveys_checked
FROM land_records
WHERE session_id = 'YOUR_SESSION_ID'
GROUP BY session_id;
"
```

---

## 🔧 Troubleshooting

### Problem: Can't Find Resume Button

**Solution:** The UI may not have a visible resume button. Use Method 3 (Manual Resume) instead.

### Problem: Don't Know Which Session to Resume

**Solution:** Check the database:
```bash
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT 
    session_id,
    owner_name,
    village_name,
    started_at,
    total_records
FROM search_sessions
WHERE status = 'running'
ORDER BY started_at DESC;
"
```

### Problem: Forgot Search Parameters

**Solution:** Query the session:
```bash
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT 
    owner_name,
    district_name,
    taluk_name,
    hobli_name,
    village_name,
    max_survey
FROM search_sessions
WHERE session_id = 'YOUR_SESSION_ID';
"
```

---

## 📋 Quick Reference Commands

### View All Sessions:
```bash
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "SELECT * FROM search_sessions ORDER BY started_at DESC;"
```

### View Resumable Sessions:
```bash
curl http://localhost:5001/api/db/resumable | python3 -m json.tool
```

### View Session Progress:
```bash
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT * FROM village_progress 
WHERE session_id = 'YOUR_SESSION_ID'
ORDER BY id;
"
```

### Count Records by Session:
```bash
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "
SELECT session_id, COUNT(*) as records
FROM land_records
GROUP BY session_id
ORDER BY records DESC;
"
```

---

## 🎯 Best Practices

### 1. **Always Note Your Session ID**
When starting a search, note the session_id from the logs:
```
17:05:13 | INFO | 📝 Created session: search_20251216_170513_6e463bee
```

### 2. **Check Progress Regularly**
Use the database queries to monitor progress instead of relying on UI only.

### 3. **Export Important Results**
Periodically export completed sessions to CSV for backup.

### 4. **Clean Up Old Sessions**
Delete old/failed sessions to keep database clean:
```sql
DELETE FROM search_sessions WHERE status = 'crashed' AND started_at < date('now', '-7 days');
```

---

## 🚀 Starting Fresh vs Resuming

### Start Fresh If:
- ✅ Testing new parameters
- ✅ Different owner name
- ✅ Different location
- ✅ Previous search had errors

### Resume If:
- ✅ Browser crashed
- ✅ Computer restarted  
- ✅ Accidentally stopped search
- ✅ Want to continue where left off

---

## 📞 Need Help?

**Check Session Status:**
```bash
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db "SELECT * FROM search_sessions WHERE session_id = 'YOUR_ID';"
```

**View Logs:**
```bash
tail -100 /Users/sks/.cursor/projects/Users-sks-Desktop-cursor-BHOOMI/terminals/189594.txt
```

**Access Database:**
```bash
sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db
```

---

**Remember:** All searches are automatically saved in real-time. Even if your browser crashes, your data is safe! 🛡️
