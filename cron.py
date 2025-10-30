"""
cron.py — Automates DOL data scraping on a recurring schedule.
"""

import os
import time
import schedule
import subprocess
from datetime import datetime

# --- Configuration ---
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_PATH = os.path.join(PROJECT_DIR, "scrape.py")
LOG_DIR = os.path.join(PROJECT_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)


def run_scraper():
    """Run the DOL scraper and log output to a timestamped file."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = os.path.join(LOG_DIR, f"scrape_{timestamp}.log")

    print(f"[{timestamp}] Starting scraper run...")
    with open(log_path, "w") as log_file:
        subprocess.run(["python3", SCRIPT_PATH], stdout=log_file, stderr=log_file)
    print(f"[{timestamp}] Run completed. Log saved at: {log_path}")


# --- Scheduling options ---

# Run once daily at midnight
schedule.every().day.at("00:00").do(run_scraper)

# Optional: for testing
# schedule.every(6).hours.do(run_scraper)
# schedule.every(10).minutes.do(run_scraper)

print("Cron scheduler started. Running initial scrape now.\n")

# --- Run immediately once on startup ---
run_scraper()

# --- Scheduler loop ---
while True:
    schedule.run_pending()
    time.sleep(60)
