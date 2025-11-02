"""
cron.py — Automates DOL data scraping on a recurring schedule.

Features
- Runs scrape.py quarterly to match DOL update cycle
- Streams logs to terminal AND a timestamped file
- Retries with exponential backoff on failure
- Deduplication via manifest.json handles everything
"""

import os
import time
import schedule
import subprocess
from datetime import datetime

# --- Configuration ----------------------------------------------------

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_PATH = os.path.join(PROJECT_DIR, "scrape.py")
LOG_DIR = os.path.join(PROJECT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Backoff behavior on failure of scrape.py
MAX_RETRIES = 5                # total attempts per scheduled run (1 initial + 4 retries)
BACKOFF_BASE_SECONDS = 60      # 1 minute base
BACKOFF_MAX_SECONDS = 30 * 60  # cap at 30 minutes

# Quarterly schedule (DOL publishes data quarterly)
RUN_AT_LOCAL = "07:00"         # local time
QUARTERLY_MONTHS = [1, 4, 7, 10]  # Jan, Apr, Jul, Oct (after each quarter ends)
RUN_DAY_OF_MONTH = 15          # Run mid-month to ensure DOL has published

# --- Helpers ----------------------------------------------------------

def _open_log_file():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = os.path.join(LOG_DIR, f"scrape_{timestamp}.log")
    f = open(log_path, "w", encoding="utf-8")
    return f, log_path

def _stream_process(cmd, log_file):
    """
    Run a subprocess and stream stdout+stderr line-by-line
    to both terminal and the given log_file.
    Returns the process returncode.
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running: {' '.join(cmd)}")
    log_file.write(f"COMMAND: {' '.join(cmd)}\n")
    log_file.flush()

    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    ) as proc:
        for line in proc.stdout:
            print(line, end="")
            log_file.write(line)
        proc.wait()
        return proc.returncode

def run_scraper():
    """
    Run the scraper with retries and exponential backoff.
    Streams output to terminal and log file.
    """
    start_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file, log_path = _open_log_file()
    print(f"[{start_ts}] Starting scraper run… (logs: {log_path})")

    try:
        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            rc = _stream_process(["python3", SCRIPT_PATH], log_file)

            if rc == 0:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scrape completed successfully.")
                log_file.write("STATUS: success\n")
                return

            # Compute backoff for next attempt
            sleep_seconds = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), BACKOFF_MAX_SECONDS)
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                f"Scrape failed (exit {rc}). Attempt {attempt}/{MAX_RETRIES}. "
                f"Retrying in {sleep_seconds} seconds…"
            )
            log_file.write(
                f"STATUS: failure exit={rc}, attempt={attempt}/{MAX_RETRIES}, backoff={sleep_seconds}s\n"
            )
            log_file.flush()
            time.sleep(sleep_seconds)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] All retries exhausted. See log: {log_path}")
        log_file.write("STATUS: exhausted\n")

    finally:
        log_file.close()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Run finished. Log saved: {log_path}")

def should_run_quarterly():
    """
    Check if today is a quarterly run day.
    Runs on the 15th of Jan, Apr, Jul, Oct (mid-month after quarter ends).
    """
    now = datetime.now()
    return now.month in QUARTERLY_MONTHS and now.day == RUN_DAY_OF_MONTH

def run_scraper_if_quarter():
    """Only run if it's the scheduled quarterly day."""
    if should_run_quarterly():
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Quarterly run triggered.")
        run_scraper()
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Not a quarterly run day. Skipping.")

# --- Scheduling -------------------------------------------------------

print("="*60)
print("DOL Data Scraper - Quarterly Cron Scheduler")
print("="*60)
print(f"Schedule: {RUN_DAY_OF_MONTH}th of {QUARTERLY_MONTHS} at {RUN_AT_LOCAL}")
print(f"Next quarters: Jan 15, Apr 15, Jul 15, Oct 15")
print("="*60)

# Run immediately on startup (dedup will skip unchanged files)
print("\nRunning initial scrape...")
run_scraper()

# Schedule daily checks for quarterly runs
schedule.every().day.at(RUN_AT_LOCAL).do(run_scraper_if_quarter)

print(f"\nScheduler active. Checking daily at {RUN_AT_LOCAL} for quarterly runs.")

# Main loop
while True:
    schedule.run_pending()
    time.sleep(3600)  # Check every hour