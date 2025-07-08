
# 🏠 Redfin Property Lead Scraper

This Scrapy spider (`RedfinLeadsSpider`) automates the extraction of real estate property details and associated agent information from [Redfin.com](https://www.redfin.com). It uses **Selenium** via `undetected_chromedriver` for JavaScript rendering and simulates real-user interactions to bypass detection.

---

## 🚀 Features

- Searches Redfin for each property in the input CSV.
- Extracts detailed property info including:
  - Full property address
  - CRMLS & GPSMLS numbers
  - Agent Names, DRE Numbers, Phone, Email & Company
- Prevents re-scraping of previously scraped entries.
- Logs scraping progress and errors in a `logs` folder.
- Saves results in `output` folder in a timestamped CSV file.

---

## 🚀 Quick Start

### 1. 📦 Prerequisites

Ensure the following are installed:

- Python 3.8+ [Download Python](https://www.python.org/downloads/)
- Google Chrome browser

Install Google Chrome Driver automatically via [undetected-chromedriver](https://github.com/ultrafunkamsterdam/undetected-chromedriver).

### 2. 📁 Folder Structure

```
project/
│
├── input/                      # Place your input CSV here (format: Redfin *.csv)
├── output/                     # Scraped results will be stored here
├── logs/                       # Execution logs will be saved here
├── spiders/
│   └── redfin_spider.py        # The main spider script
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

### 3. 📥 Install Dependencies

```bash
pip install -r requirements.txt
```

`requirements.txt` should include:
```
scrapy
selenium
python-dotenv
undetected-chromedriver
```

### 4. 📄 Prepare Input File

Place a file inside the `input` folder named like: `Redfin YourFileName.csv`

Format should include the following columns:
```
Required Columns in Input CSV:

| Address | City | State | Zip |
|---------|------|-------|-----|

Example row:
123 Main St, Los Angeles, CA, 90001

```

### 5. ▶️ Run the Spider

```bash
scrapy crawl Redfin
```

- Results will be saved in the `output/` folder as `Redfin Properties Details.csv`
- Logs will be saved in the `logs/` folder with the current timestamp

---

## 📌 Notes

- Script automatically skips already-scraped records.
- You can stop the scraper anytime, and it will resume from where it left off based on already-scraped entries.
- Logging enabled for:
  - Successfully scraped items
  - Skipped or errored addresses
  
- Chrome browser window remains open for visual inspection (can be minimized or headless if modified).
- Chrome is launched in **undetected mode** for stealth; avoid opening other Chrome windows while running.
- Customizable in `custom_settings` for output format, retries, and timeout.

---

## 🧠 Developer Notes

The scraping logic uses Selenium with Chrome (headful mode by default), parses content using Scrapy `Selector`, and manages CSV I/O and logs via custom logic.

To debug Selenium:
```python
self.driver.save_screenshot("debug.png")
print(self.driver.page_source)
```

---

## 📧 Contact

For questions or custom scraping solutions, contact: **Scrape Byte**
