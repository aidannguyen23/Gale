# Scraping all the LCA Programs (H-1B, H-1B1, E-3) from: https://www.dol.gov/agencies/eta/foreign-labor/performance
import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import re

# the base url for scraping
BASE_URL = "https://www.dol.gov/agencies/eta/foreign-labor/performance" 
# the local folder i'm saving it to
SAVE_DIR = "data"
# creates the folder if it doesn't exist, does nothing if it already does
os.makedirs(SAVE_DIR, exist_ok = True)

# sends an HTTP GET request to the page and stores the response (html, status code, headers)
response = requests.get(BASE_URL)
# parses the html so you can navigate it and extract elements like <a> tags
soup = BeautifulSoup(response.text, "html.parser")

# find all links that end with .xlsx and contain "LCA" in the filename
xlsx_links = []
# <a> anchors are hyperlinks 
for a in soup.find_all("a", href=True):
    # pulls the link target (URL string) from the tag
    href = a["href"]
    if href.lower().endswith(".xlsx") and (("lca") in href.lower() or "h-1b" in href.lower() or "h1b" in href.lower()):
        # turns relative links (like /sites/...) into complete absolute URLs (e.g., https://www.dol.gov/sites/...)
        xlsx_links.append(urljoin(BASE_URL, href))

print(f"Found {len(xlsx_links)} LCA Excel files.")

for link in xlsx_links:
    os.makedirs(SAVE_DIR, exist_ok=True)

    # extract filename from URL
    filename = link.split("/")[-1]

    # find year (e.g., 2022, 2019, etc.) using regex
    match = re.search(r"(20\d{2}|19\d{2})", filename)
    if match:
        year = match.group(1)
    else:
        year = "unknown_year"

    # create subdirectory for that year
    year_dir = os.path.join(SAVE_DIR, year)
    os.makedirs(year_dir, exist_ok=True)

    # full save path
    filepath = os.path.join(year_dir, filename)

    # skip if already downloaded
    if os.path.exists(filepath):
        print(f"Skipping (already downloaded): {filepath}")
        continue

    # download and save
    print(f"Downloading: {link}")
    r = requests.get(link)
    with open(filepath, "wb") as f:
        f.write(r.content)
