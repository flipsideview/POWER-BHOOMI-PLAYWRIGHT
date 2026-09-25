# Record Room (RRS) Tools — How to Use

Two tools for the Karnataka **Bhu Suraksha record-room portal**
(https://recordroom.karnataka.gov.in/Service4/RRS/RRSRequest), which holds scanned
revenue files (mutation registers, RTC files, land-reform files, survey files, ...).

| Tool | What it does |
|---|---|
| `bhoomi_rrs_catalog.py` | Builds a **catalog of every file the record room lists**, village by village, for one taluk, one district or the whole state. Free: it only searches. Output is a SQLite database plus CSV exports you can sort and filter in Excel. |
| `bhoomi_rrs_downloader.py` | Works on **one village**: searches by survey number, indexes what it finds, and downloads the PDFs of documents you have **already paid for** from the portal's Dashboard. |

Neither tool pays for anything or bypasses payment. Documents cost Rs 10 per page on the
portal; the intended workflow is: catalog → shortlist in Excel → pay for the shortlist in
the portal → fetch the paid PDFs with the downloader.

---

## 1. Setup (once)

Both tools use the project's existing virtual environment. If it is not set up yet:

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements_playwright.txt
./venv/bin/playwright install chromium
```

Run every command below from the project folder (`POWER-BHOOMI-PLAYWRIGHT`).

### 1.1 Log in once

The portal needs a **mobile number + OTP** login. The tools never type credentials; you
do it once in the browser window they open:

```bash
./venv/bin/python bhoomi_rrs_downloader.py login
```

A Chromium window opens on the portal. Enter your mobile number, tap **Generate OTP**,
enter the OTP, click **Login**. The tool notices the login, saves the session to
`rrs_state.json`, and later runs skip the OTP until the portal expires the session.

### 1.2 Save your applicant profile once

The portal refuses every search until the **applicant details** (first step of its wizard)
have been saved. In the same Chromium window, on the first wizard step:

1. Click **ತಿದ್ದು / Edit** (the fields are locked until you do).
2. Fill the empty mandatory fields (Address Line‑1 was the empty one).
3. Click **ಉಳಿಸಿ / Save** and complete any OTP prompt.

If you skip this, every run stops with
`Applicant details not found. Please fill the applicant details and continue.`

### 1.3 One login at a time

The portal keeps **one live session per mobile number**. Logging in anywhere else
(your phone, Chrome, another tab) logs the tools out. A running crawl then pauses and asks
you to log in again in its own window — nothing is lost, but it does not progress until
you do.

---

## 2. Catalog: `bhoomi_rrs_catalog.py`

### 2.1 Run it

```bash
# whole state (all 31 districts, ~32,600 villages)
caffeinate -dimsu ./venv/bin/python bhoomi_rrs_catalog.py crawl

# one district (code or English name)
./venv/bin/python bhoomi_rrs_catalog.py crawl --district "Bangalore Rural"
./venv/bin/python bhoomi_rrs_catalog.py crawl --district 21

# one taluk (needs exactly one --district; taluk is the portal's code)
./venv/bin/python bhoomi_rrs_catalog.py crawl --district "Bangalore Rural" --taluk 4
```

`caffeinate -dimsu` keeps the Mac awake while it runs. Keep the laptop **plugged in with
the lid open**; closing the lid can still put it to sleep.

What happens:

1. Logs in (or waits for you to) and creates a document request on the portal.
2. Lists every taluk → hobli → village of the chosen districts from the portal's own menus.
3. Queries each village once and stores every file listed for it.
4. Prints one line per village with an ETA.

Speed is roughly 1–3 seconds per village. Hoskote taluk (294 villages) took about
6 minutes; the whole state is many hours.

### 2.2 Stop, resume, retry

- **Ctrl‑C** stops it cleanly. Progress is saved after every village.
- **Run the same command again** to continue where it left off. Finished villages are
  skipped, and the village list is not rebuilt.
- A village that fails 4 times is marked `error` and skipped. At the end, retry those:

  ```bash
  ./venv/bin/python bhoomi_rrs_catalog.py crawl --retry-errors
  ```

Options: `--delay 2` (seconds between villages, default 1), `--limit 50` (stop after
N villages, for testing).

### 2.3 Check progress

```bash
./venv/bin/python bhoomi_rrs_catalog.py status
```

Shows, per district: villages known, villages with files, empty, error, pending, and the
number of file listings; plus the total of unique files catalogued.

### 2.4 Export to CSV

```bash
# everything crawled so far, one CSV
./venv/bin/python bhoomi_rrs_catalog.py export

# one district
./venv/bin/python bhoomi_rrs_catalog.py export --district 21 --out ~/Downloads/bangalore_rural.csv

# one CSV per district (recommended for the whole state — Excel cannot open
# more than ~1,048,000 rows in one file)
./venv/bin/python bhoomi_rrs_catalog.py export --split
```

Default location: `~/Downloads/BHOOMI_RRS/`. Files open in Excel with Kannada intact.

Columns:

| Column | Meaning |
|---|---|
| `district_code`, `district`, `district_en` | District code, Kannada name, English name |
| `taluk_code`, `taluk`, `hobli_code`, `hobli`, `village_code`, `village` | Location (Kannada names, portal codes) |
| `file_id` | The portal's ID for the file. Use it to find the same card on the portal |
| `office_code`, `office` | Office holding the file (e.g. Taluk Office, DC Office) |
| `file_no` | File / register number as written on the card |
| `subject` | Subject line of the file |
| `year` | Financial year, e.g. `2015-2016` |
| `type` | `File` or `Register` |
| `survey_hint` | Survey numbers found in the subject text (e.g. `7/3`, `65, 117`). Only present when the subject mentions them (~70% of rows) |
| `survey_tags` | Number of survey-number cards the portal shows for this file in this village |

The portal's cards carry no official survey-number field, so `survey_hint` is extracted
from the subject text. For exact per-survey results on a shortlisted village, use the
downloader (section 3).

### 2.5 Where the data lives

`~/Documents/POWER-BHOOMI/rrs_catalog.db` (SQLite). Tables: `districts`, `villages`
(with crawl status), `files` (one row per unique file), `village_files` (which files are
listed in which village). Delete this file to start a fresh catalog.

---

## 3. Downloader: `bhoomi_rrs_downloader.py`

### 3.1 Search a village by survey number

```bash
./venv/bin/python bhoomi_rrs_downloader.py download \
    --district "Bangalore Rural" --taluk "Hoskote" --hobli "Kasaba" \
    --village 1 --surveys "1,5,10-15"
```

- District, taluk, hobli and village accept **English names, Kannada names or portal
  codes** (English is matched to the Kannada menu entries automatically; if a name is
  ambiguous the tool lists the candidates and asks for the code).
- `--surveys` accepts single numbers, comma lists, ranges, and hissa forms like `12/*/3`.
- Output: `~/Downloads/BHOOMI_RRS/<village>/rrs_search_index.csv` with one row per
  (survey, document), plus the raw results page per survey as `SyN_results.html`.

Optional: `--file-reg File|Register`, `--file-reg-type "<type text>"`,
`--desc "<request description>"`, `--out <folder>`.

### 3.2 Fetch documents you have paid for

Pay for the documents in the portal (view the document → add pages to wishlist → pay).
Then:

```bash
./venv/bin/python bhoomi_rrs_downloader.py download \
    --district "Bangalore Rural" --taluk "Hoskote" --hobli "Kasaba" \
    --village 1 --surveys "1" --check-dashboard
```

`--check-dashboard` opens your portal Dashboard and downloads every ready PDF into the
output folder.

### 3.3 Other commands

```bash
./venv/bin/python bhoomi_rrs_downloader.py login   # log in / refresh the saved session
./venv/bin/python bhoomi_rrs_downloader.py map     # dump the portal form structure (for maintenance)
```

---

## 4. Troubleshooting

| Message | What to do |
|---|---|
| `LOGIN REQUIRED — enter your MOBILE NUMBER and OTP` | Log in **in the Chromium window the tool opened** (blue Chromium icon, "controlled by automated test software" banner). Not in Chrome. It waits 10 minutes; if it times out, run the command again. |
| `Applicant details not found` | Do section 1.2 once. |
| `Session lost … log in again` during a crawl | Someone logged in elsewhere with the same number, or the session expired. Log in in the tool's window; the crawl resumes by itself. |
| `Request already exist` | Harmless. |
| `'X' not found in taluk. Options: …` | Use the code shown in the options list, or the Kannada name. |
| `Portal lost the active request` | Harmless; a new request is created automatically. |
| Excel shows `???` for Kannada | Open the CSV via *Data → From Text/CSV* and choose UTF‑8, or use the exported file directly (it includes a UTF‑8 BOM). |

Files the tools create in the project folder — **do not commit these**:
`rrs_state.json` (your login session), `rrs_profile/` (browser profile with the session),
`rrs_page_map.json`, `rrs_page.html`, `rrs_debug_*.html`.

---

## 5. What the tools deliberately do not do

- They never enter your mobile number, OTP or any payment details.
- They never add pages to the wishlist or pay.
- They do not capture or download document contents that have not been paid for.
