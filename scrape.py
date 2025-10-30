"""
Scrape all downloadable files by program (PERM, LCA, H-2A, etc.)
from: https://www.dol.gov/agencies/eta/foreign-labor/performance

Features:
- Groups files by Program // Year // File
- Deduplicates using manifest.json
- Marks and separates legacy “Annual Report” data
"""

import os
import re
import json
import hashlib
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit, urlunsplit
from datetime import datetime, timezone

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

BASE_URL = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
SAVE_DIR = "data"
VALID_EXTS = (".xlsx", ".csv", ".pdf", ".docx", ".doc", ".zip")

PROGRAM_MAP = {
    "perm": "PERM Program",
    "lca": "LCA Program",
    "pw": "Prevailing Wage Program",
    "h-2a": "H-2A Program",
    "h2a": "H-2A Program",
    "h-2b": "H-2B Program",
    "h2b": "H-2B Program",
    "cw-1": "CW-1 Program",
    "cw1": "CW-1 Program",
}

os.makedirs(SAVE_DIR, exist_ok=True)
manifest_path = os.path.join(SAVE_DIR, "manifest.json")

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

def normalize_url(url: str) -> str:
    """Normalize URL so duplicates always match."""
    parsed = urlsplit(url.strip())
    return urlunsplit((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        parsed.path.rstrip('/'),
        parsed.query,
        parsed.fragment,
    ))

def load_manifest():
    """Load manifest.json if it exists, else return empty dict."""
    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f:
            try:
                data = json.load(f)
                print(f"Loaded {len(data)} entries from manifest.")
                return data
            except json.JSONDecodeError:
                print("Manifest corrupted — resetting.")
                return {}
    return {}

def save_manifest(manifest):
    """Write manifest.json at end of run."""
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

def clean_program_name(name):
    """Sanitize and truncate overly long folder names."""
    name = re.sub(r"[\\/*?:\"<>|]", "_", name.strip())
    if len(name) > 80:
        name = name[:80]
    if "office" in name.lower() and "program" not in name.lower():
        name = "Miscellaneous OFLC Data"
    return name

def extract_year(filename):
    """Extract fiscal or calendar year from filename."""
    match = re.search(r"fy\s*(\d{2,4})", filename, re.IGNORECASE)
    if match:
        token = match.group(1)
        return f"20{token}" if len(token) == 2 else token
    match = re.search(r"(19|20)\d{2}", filename)
    return match.group(0) if match else "unknown_year"

# ---------------------------------------------------------------------
# Scrape setup
# ---------------------------------------------------------------------

print(f"Fetching: {BASE_URL}")
response = requests.get(BASE_URL)
soup = BeautifulSoup(response.text, "html.parser")

manifest = load_manifest()
download_links = []
current_program = None

# ---------------------------------------------------------------------
# Parse HTML and collect download links
# ---------------------------------------------------------------------

for element in soup.find_all(["strong", "b", "h3", "a"]):
    text = element.get_text(strip=True) if element else ""
    lower_text = text.lower()

    # detect program or legacy sections
    if "annual report" in lower_text:
        current_program = "Legacy Annual Reports"
        print(f"[Detected Legacy Section]: {current_program}")
    elif any(k in lower_text for k in ["program", "disclosure", "oflc"]):
        current_program = clean_program_name(text)
        print(f"[Detected Program Section]: {current_program}")

    # detect file links
    elif element.name == "a" and element.has_attr("href") and current_program:
        href = element["href"].lower()
        if not any(href.endswith(ext) for ext in VALID_EXTS):
            continue

        full_url = urljoin(BASE_URL, href)
        filename = href.split("/")[-1]
        year = extract_year(filename)

        # normalize program name
        for key, val in PROGRAM_MAP.items():
            if key in current_program.lower():
                current_program = val
                break

        download_links.append({
            "program": current_program,
            "url": normalize_url(full_url),
            "filename": filename,
            "year": year
        })

print(f"\nFound {len(download_links)} total downloadable files.")

# ---------------------------------------------------------------------
# Download files (with deduplication)
# ---------------------------------------------------------------------

for item in download_links:
    program = item["program"]
    url = item["url"]
    filename = item["filename"]
    year = item["year"]

    safe_program = clean_program_name(program)
    program_dir = os.path.join(SAVE_DIR, safe_program)
    year_dir = os.path.join(program_dir, year)
    os.makedirs(year_dir, exist_ok=True)
    filepath = os.path.join(year_dir, filename)

    # skip if already in manifest and unchanged
    if url in manifest:
        try:
            head = requests.head(url, timeout=10)
            etag = head.headers.get("ETag")
            last_modified = head.headers.get("Last-Modified")

            cached = manifest[url]
            if etag and cached.get("etag") == etag:
                print(f"Skipping (unchanged ETag): {url}")
                continue
            if last_modified and cached.get("last_modified") == last_modified:
                print(f"Skipping (unchanged Last-Modified): {url}")
                continue
        except Exception:
            print(f"Skipping (manifested but couldn't verify freshness): {url}")
            continue

    # also skip if same file path already stored
    if any(entry.get("saved_path") == filepath for entry in manifest.values()):
        print(f"Skipping (duplicate file path): {filepath}")
        continue

    # download
    print(f"Downloading ({safe_program}, {year}): {url}")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        with open(filepath, "wb") as f:
            f.write(r.content)

        digest = hashlib.sha256(r.content).hexdigest()
        status = "legacy" if "legacy" in safe_program.lower() else "active"

        manifest[url] = {
            "program": safe_program,
            "filename": filename,
            "year": year,
            "saved_path": filepath,
            "sha256": digest,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "etag": r.headers.get("ETag"),
            "last_modified": r.headers.get("Last-Modified"),
        }

        # save manifest immediately (so crash-safe)
        save_manifest(manifest)
        print(f"Saved to: {filepath}")

    except Exception as e:
        print(f"Failed to download {url}: {e}")

# ---------------------------------------------------------------------
# Wrap-up
# ---------------------------------------------------------------------

save_manifest(manifest)
print("\nCompleted incremental scrape. Manifest updated.")
