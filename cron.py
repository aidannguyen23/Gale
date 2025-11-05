"""
cron.py — Automates DOL data scraping on a recurring schedule.

Features:
- Runs scrape.py quarterly to match DOL update cycle
- Integrates with scrape.py's built-in logging system
- Retries with exponential backoff on failure
- Cleans up old logs automatically
- Sends notifications on persistent failures
- Uses proper Python environment detection
"""

import os
import sys
import time
import schedule
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path
import glob

# --- Configuration ----------------------------------------------------

PROJECT_DIR = Path(__file__).parent.absolute()
SCRIPT_PATH = PROJECT_DIR / "scrape.py"
CLEANUP_SCRIPT_PATH = PROJECT_DIR / "cleanup.py"
DATA_DIR = PROJECT_DIR / "data"
LOG_DIR = DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Backoff behavior on failure of scrape.py
MAX_RETRIES = 5                # total attempts per scheduled run (1 initial + 4 retries)
BACKOFF_BASE_SECONDS = 60      # 1 minute base
BACKOFF_MAX_SECONDS = 30 * 60  # cap at 30 minutes

# Quarterly schedule (DOL publishes data quarterly)
RUN_AT_LOCAL = "07:00"         # local time
QUARTERLY_MONTHS = [1, 4, 7, 10]  # Jan, Apr, Jul, Oct (after each quarter ends)
RUN_DAY_OF_MONTH = 15          # Run mid-month to ensure DOL has published

# Log retention (cleanup logs older than this)
LOG_RETENTION_DAYS = 90        # Keep 3 months of logs

# Cleanup schedule - run weekly to remove stale manifest entries
CLEANUP_SCHEDULE_DAY = "monday"
CLEANUP_TIME = "03:00"

# --- Logging Setup ----------------------------------------------------

def setup_cron_logging():
    """Setup logging for the cron scheduler itself."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"cron_{timestamp}.log"
    
    logger = logging.getLogger("cron")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Cron logging to: {log_file}")
    return logger

logger = setup_cron_logging()

# --- Helpers ----------------------------------------------------------

def get_python_executable():
    """Get the current Python executable path."""
    return sys.executable

def cleanup_old_logs():
    """Remove log files older than LOG_RETENTION_DAYS."""
    cutoff_date = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)
    removed_count = 0
    
    logger.info(f"Cleaning up logs older than {LOG_RETENTION_DAYS} days...")
    
    for log_file in LOG_DIR.glob("*.log"):
        try:
            # Get file modification time
            mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
            
            if mtime < cutoff_date:
                log_file.unlink()
                removed_count += 1
                logger.debug(f"Removed old log: {log_file.name}")
        except Exception as e:
            logger.warning(f"Failed to remove {log_file.name}: {e}")
    
    if removed_count > 0:
        logger.info(f"Removed {removed_count} old log files")
    else:
        logger.info("No old logs to remove")

def run_script_with_retry(script_path: Path, script_name: str):
    """
    Run a script with retries and exponential backoff.
    Returns True if successful, False if all retries exhausted.
    """
    python_exe = get_python_executable()
    
    logger.info(f"Starting {script_name}...")
    logger.info(f"Command: {python_exe} {script_path}")
    
    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        
        try:
            # Run the script and capture output
            # Note: scrape.py has its own logging, so we just capture exit code
            result = subprocess.run(
                [python_exe, str(script_path)],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if result.returncode == 0:
                logger.info(f"{script_name} completed successfully")
                return True
            
            # Log failure details
            logger.error(f"{script_name} failed with exit code {result.returncode}")
            if result.stderr:
                logger.error(f"Error output: {result.stderr[:500]}")  # First 500 chars
            
        except subprocess.TimeoutExpired:
            logger.error(f"{script_name} timed out after 1 hour")
        except Exception as e:
            logger.error(f"{script_name} raised exception: {e}")
        
        # Calculate backoff for next attempt
        if attempt < MAX_RETRIES:
            sleep_seconds = min(
                BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), 
                BACKOFF_MAX_SECONDS
            )
            logger.warning(
                f"Attempt {attempt}/{MAX_RETRIES} failed. "
                f"Retrying in {sleep_seconds} seconds..."
            )
            time.sleep(sleep_seconds)
    
    logger.error(f"All {MAX_RETRIES} retry attempts exhausted for {script_name}")
    send_failure_notification(script_name)
    return False

def send_failure_notification(script_name: str):
    """
    Send notification about persistent script failure.
    Implement your notification method here (email, Slack, PagerDuty, etc.)
    """
    message = (
        f"ALERT: {script_name} failed after {MAX_RETRIES} attempts\n"
        f"Time: {datetime.now()}\n"
        f"Check logs in: {LOG_DIR}"
    )
    
    # TODO: Implement actual notification (email, Slack, etc.)
    logger.critical("="*60)
    logger.critical("FAILURE NOTIFICATION")
    logger.critical(message)
    logger.critical("="*60)
    
    # Example: Write to a failure file that monitoring can pick up
    failure_file = DATA_DIR / "SCRAPE_FAILURE.txt"
    try:
        with open(failure_file, 'w') as f:
            f.write(message)
        logger.info(f"Failure flag written to: {failure_file}")
    except Exception as e:
        logger.error(f"Failed to write failure flag: {e}")

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
        logger.info("="*60)
        logger.info("QUARTERLY SCRAPE TRIGGERED")
        logger.info("="*60)
        success = run_script_with_retry(SCRIPT_PATH, "scrape.py")
        
        if success:
            # Clear any previous failure flags
            failure_file = DATA_DIR / "SCRAPE_FAILURE.txt"
            if failure_file.exists():
                failure_file.unlink()
                logger.info("Cleared previous failure flag")
    else:
        logger.debug(f"Not a quarterly run day. Skipping. (Today: {datetime.now().strftime('%b %d')})")

def run_cleanup():
    """Run the cleanup script to remove stale manifest entries."""
    logger.info("="*60)
    logger.info("RUNNING CLEANUP SCRIPT")
    logger.info("="*60)
    
    if not CLEANUP_SCRIPT_PATH.exists():
        logger.warning(f"Cleanup script not found: {CLEANUP_SCRIPT_PATH}")
        return
    
    run_script_with_retry(CLEANUP_SCRIPT_PATH, "cleanup.py")

def run_log_cleanup():
    """Scheduled log cleanup task."""
    logger.info("="*60)
    logger.info("RUNNING LOG CLEANUP")
    logger.info("="*60)
    cleanup_old_logs()

# --- Main Entry Point -------------------------------------------------

def main():
    """Main scheduler loop."""
    logger.info("="*60)
    logger.info("DOL Data Scraper - Quarterly Cron Scheduler")
    logger.info("="*60)
    logger.info(f"Scrape Schedule: {RUN_DAY_OF_MONTH}th of {QUARTERLY_MONTHS} at {RUN_AT_LOCAL}")
    logger.info(f"Cleanup Schedule: Every {CLEANUP_SCHEDULE_DAY.title()} at {CLEANUP_TIME}")
    logger.info(f"Log Retention: {LOG_RETENTION_DAYS} days")
    logger.info(f"Python: {get_python_executable()}")
    logger.info("="*60)
    
    # Run initial scrape on startup (dedup will skip unchanged files)
    logger.info("\nRunning initial scrape on startup...")
    run_script_with_retry(SCRIPT_PATH, "scrape.py")
    
    # Schedule quarterly scrape checks
    schedule.every().day.at(RUN_AT_LOCAL).do(run_scraper_if_quarter)
    
    # Schedule weekly cleanup of stale manifest entries
    schedule_func = getattr(schedule.every(), CLEANUP_SCHEDULE_DAY)
    schedule_func.at(CLEANUP_TIME).do(run_cleanup)
    
    # Schedule weekly log cleanup (same day as manifest cleanup, but 30 min later)
    cleanup_log_time = (
        datetime.strptime(CLEANUP_TIME, "%H:%M") + timedelta(minutes=30)
    ).strftime("%H:%M")
    schedule_func = getattr(schedule.every(), CLEANUP_SCHEDULE_DAY)
    schedule_func.at(cleanup_log_time).do(run_log_cleanup)
    
    logger.info(f"\nScheduler active:")
    logger.info(f"  - Daily check at {RUN_AT_LOCAL} for quarterly scrapes")
    logger.info(f"  - {CLEANUP_SCHEDULE_DAY.title()} at {CLEANUP_TIME} for cleanup")
    logger.info(f"  - {CLEANUP_SCHEDULE_DAY.title()} at {cleanup_log_time} for log cleanup")
    logger.info("\nPress Ctrl+C to stop.\n")
    
    # Main loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("\nScheduler stopped by user")
    except Exception as e:
        logger.exception(f"Scheduler crashed: {e}")
        raise

if __name__ == "__main__":
    main()