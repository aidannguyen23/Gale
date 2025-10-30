# DOL Foreign Labor Data Scraper

This project automatically downloads and organizes all public disclosure files from the **U.S. Department of Labor’s Office of Foreign Labor Certification (OFLC)** data portal:

[https://www.dol.gov/agencies/eta/foreign-labor/performance](https://www.dol.gov/agencies/eta/foreign-labor/performance)

---

## Overview

The scraper collects downloadable datasets (Excel, CSV, PDF, DOCX) for major OFLC programs:

- **PERM Program**
- **LCA Program (H-1B, H-1B1, E-3)**
- **H-2A Program**
- **H-2B Program**
- **Prevailing Wage Program**
- **CW-1 Program**
- **Legacy Annual Reports**

Each file is saved in a structured directory:

```
data/
  ├── PERM Program/
  │   ├── 2024/
  │   │   ├── perm_disclosure_data_fy2024.xlsx
  │   │   └── perm_record_layout_fy2024.pdf
  ├── H-2A Program/
  │   └── 2023/
  │       └── h-2a_disclosure_data_fy2023_q4.xlsx
  └── ...
```

---

## Features

- **Automatic Discovery:** Parses the OFLC portal to find all downloadable files.
- **Organized Storage:** Files grouped by *Program → Year → Filename*.
- **Deduplication:** Maintains a `manifest.json` with SHA256, ETag, and timestamp for incremental updates.
- **Crash-Safe:** Manifest saved after every successful download.
- **Polite Crawling:** Requests are throttled and headers checked before re-downloading.

---

## Requirements

Python 3.9+  
Install dependencies:

```bash
pip install -r requirements.txt
```

Typical packages:
```
requests
beautifulsoup4
```

---

## Usage

Run the scraper manually:

```bash
python scrape.py
```

Or schedule recurring runs with the provided cron script:

```bash
python cron.py
```

This will:
- Execute one immediate scrape.
- Continue running daily at midnight (configurable in `cron.py`).

---

## Manifest Example

Each downloaded file is recorded in `data/manifest.json`:

```json
{
  "https://www.dol.gov/.../perm_disclosure_data_fy2024.xlsx": {
    "program": "PERM Program",
    "filename": "perm_disclosure_data_fy2024.xlsx",
    "year": "2024",
    "saved_path": "data/PERM Program/2024/perm_disclosure_data_fy2024.xlsx",
    "sha256": "abcdef123456...",
    "timestamp": "2025-10-30T08:22:22.94Z",
    "status": "active"
  }
}
```

---

## Notes

- The scraper is designed to be **incremental** — re-running it only fetches new or changed files.
- Works entirely offline after download; no API key required.
- Safe to stop and resume at any time.

---

## License

MIT License © 2025
