"""
Scrape all downloadable files by program (PERM, LCA, H-2A, etc.)
from: https://www.dol.gov/agencies/eta/foreign-labor/performance

Features:
- Groups files by Program // Year // File
- Deduplicates using manifest.json
- Skips deprecated Annual Reports
- Enhanced debugging to catch missing files
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
VALID_EXTS = (".xlsx", ".csv", ".pdf", ".docx", ".doc", ".zip", ".xls")

# Skip deprecated annual reports
SKIP_PATTERNS = [
    "annual performance report",
    "fy 2016 report",
    "fy 2015 report",
    "fy 2014 report",
    "fy 2013 report",
    "fy 2012 report",
    "fy 2011 report",
    "fy 2010 report",
    "fy 2009 report",
    "fy 2007 report",
    "fy 2006 report",
]

PROGRAM_MAP = {
    "perm": "PERM Program",
    "lca": "LCA Program",
    "h-1b": "LCA Program",
    "h1b": "LCA Program",
    "pw": "Prevailing Wage Program",
    "prevailing": "Prevailing Wage Program",
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
    return name

def extract_year(filename):
    """Extract fiscal or calendar year from filename."""
    match = re.search(r"fy\s*(\d{2,4})", filename, re.IGNORECASE)
    if match:
        token = match.group(1)
        return f"20{token}" if len(token) == 2 else token
    match = re.search(r"(19|20)\d{2}", filename)
    return match.group(0) if match else "unknown_year"

def detect_program_from_filename(filename):
    """Detect program from filename as fallback."""
    filename_lower = filename.lower()
    
    # Check each program pattern
    for key, val in PROGRAM_MAP.items():
        if key in filename_lower:
            return val
    
    return None

def should_skip_file(filename, text_context=""):
    """Check if file should be skipped (deprecated annual reports)."""
    combined = (filename + " " + text_context).lower()
    
    # Skip if it matches deprecated patterns (but not "record layout" PDFs)
    if "record layout" not in combined and "record_layout" not in combined:
        for pattern in SKIP_PATTERNS:
            if pattern in combined:
                return True
        
        # Skip PDF annual reports specifically (but not layouts)
        if "annual" in combined and "report" in combined and filename.lower().endswith(".pdf"):
            return True
    
    return False

def parse_table_links(soup):
    """Parse download links from table format (used in Latest Quarterly Updates)."""
    table_links = []
    
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            
            # First cell typically has program name
            program_cell = cells[0].get_text(strip=True)
            
            # Remaining cells have file links
            for cell in cells[1:]:
                for link in cell.find_all("a", href=True):
                    href = link["href"]
                    if not any(href.lower().endswith(ext) for ext in VALID_EXTS):
                        continue
                    
                    filename = href.split("/")[-1]
                    
                    # Skip deprecated files
                    if should_skip_file(filename, program_cell):
                        print(f"[TABLE] Skipping deprecated: {filename}")
                        continue
                    
                    full_url = urljoin(BASE_URL, href)
                    year = extract_year(filename)
                    
                    # Detect program from table cell or filename
                    current_program = None
                    for key, val in PROGRAM_MAP.items():
                        if key in program_cell.lower() or key in filename.lower():
                            current_program = val
                            break
                    
                    if not current_program:
                        current_program = detect_program_from_filename(filename)
                    
                    if not current_program:
                        current_program = "Uncategorized"
                    
                    table_links.append({
                        "program": current_program,
                        "url": normalize_url(full_url),
                        "filename": filename,
                        "year": year
                    })
    
    return table_links

# ---------------------------------------------------------------------
# Scrape setup
# ---------------------------------------------------------------------

print(f"Fetching: {BASE_URL}")
response = requests.get(BASE_URL)
soup = BeautifulSoup(response.text, "html.parser")

manifest = load_manifest()
download_links = []
current_program = None
skip_mode = False

# ---------------------------------------------------------------------
# Parse HTML and collect download links
# ---------------------------------------------------------------------

# First, parse table-based links (Latest Quarterly Updates)
table_links = parse_table_links(soup)
print(f"Found {len(table_links)} files from tables.")
download_links.extend(table_links)

# Then parse ALL links on the page and intelligently categorize them
print("\nScanning all links on page...")
all_links = soup.find_all("a", href=True)

for link in all_links:
    href = link["href"]
    href_lower = href.lower()
    
    # Check if it's a valid file type
    if not any(href_lower.endswith(ext) for ext in VALID_EXTS):
        continue
    
    filename = href.split("/")[-1]
    
    # Skip deprecated files
    if should_skip_file(filename):
        continue
    
    # Try to detect program from filename
    program = detect_program_from_filename(filename)
    
    # If no program detected, try to look at surrounding context
    if not program:
        # Look at parent elements for context
        parent = link.find_parent(["td", "p", "li", "div"])
        if parent:
            context = parent.get_text(strip=True)
            for key, val in PROGRAM_MAP.items():
                if key in context.lower():
                    program = val
                    break
    
    # Look backwards in the document for the nearest heading
    if not program:
        # Find the nearest preceding heading
        for prev in link.find_all_previous(["h2", "h3", "h4", "strong", "b"]):
            text = prev.get_text(strip=True).lower()
            for key, val in PROGRAM_MAP.items():
                if key in text and "annual" not in text:
                    program = val
                    break
            if program:
                break
    
    if not program:
        program = "Uncategorized"
    
    full_url = urljoin(BASE_URL, href)
    normalized_url = normalize_url(full_url)
    year = extract_year(filename)
    
    # Check if we already have this URL
    if any(item["url"] == normalized_url for item in download_links):
        continue
    
    download_links.append({
        "program": program,
        "url": normalized_url,
        "filename": filename,
        "year": year
    })

print(f"\nFound {len(download_links)} total downloadable files (excluding deprecated annual reports).")

# ---------------------------------------------------------------------
# Download files (with deduplication)
# ---------------------------------------------------------------------

downloaded_count = 0
skipped_count = 0

for item in download_links:
    program = item["program"]
    url = item["url"]
    filename = item["filename"]
    year = item["year"]

    safe_program = clean_program_name(program)
    
    # Always use Program/Year/File hierarchy
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
                skipped_count += 1
                continue
            if last_modified and cached.get("last_modified") == last_modified:
                skipped_count += 1
                continue
        except Exception:
            skipped_count += 1
            continue

    # also skip if same file path already stored
    if any(entry.get("saved_path") == filepath for entry in manifest.values()):
        skipped_count += 1
        continue

    # download
    print(f"Downloading ({safe_program}/{year}): {filename}")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        with open(filepath, "wb") as f:
            f.write(r.content)

        digest = hashlib.sha256(r.content).hexdigest()

        manifest[url] = {
            "program": safe_program,
            "filename": filename,
            "year": year,
            "saved_path": filepath,
            "sha256": digest,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "etag": r.headers.get("ETag"),
            "last_modified": r.headers.get("Last-Modified"),
        }

        # save manifest immediately (so crash-safe)
        save_manifest(manifest)
        print(f"✓ Saved to: {filepath}")
        downloaded_count += 1

    except Exception as e:
        print(f"✗ Failed to download {url}: {e}")

# ---------------------------------------------------------------------
# Wrap-up
# ---------------------------------------------------------------------

save_manifest(manifest)
print(f"\n{'='*60}")
print(f"Scrape completed!")
print(f"Downloaded: {downloaded_count} files")
print(f"Skipped: {skipped_count} files")
print(f"Total in manifest: {len(manifest)} files")
print(f"{'='*60}")