"""
cleanup.py — Removes stale entries from manifest when files no longer exist.

Features:
- Scans manifest for missing files on disk
- Removes stale entries atomically
- Creates backup before modification
- Detailed reporting of cleaned entries
- Safe: only removes entries where files are confirmed missing
"""

import os
import json
import shutil
import logging
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from typing import Dict, List, Set

# --- Configuration ----------------------------------------------------

PROJECT_DIR = Path(__file__).parent.absolute()
DATA_DIR = PROJECT_DIR / "data"
MANIFEST_PATH = DATA_DIR / "manifest.json"
LOG_DIR = DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# --- Logging Setup ----------------------------------------------------

def setup_logging():
    """Setup versioned logging for cleanup operations."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"cleanup_{timestamp}.log"
    
    logger = logging.getLogger("cleanup")
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
    
    logger.info(f"Cleanup logging to: {log_file}")
    return logger

logger = setup_logging()

# --- Utilities --------------------------------------------------------

def load_manifest() -> Dict:
    """Load manifest.json safely."""
    if not MANIFEST_PATH.exists():
        logger.error(f"Manifest not found: {MANIFEST_PATH}")
        return {}
    
    try:
        with open(MANIFEST_PATH, 'r') as f:
            manifest = json.load(f)
            logger.info(f"Loaded manifest with {len(manifest)} entries")
            return manifest
    except json.JSONDecodeError as e:
        logger.error(f"Manifest corrupted: {e}")
        
        # Try backup
        backup_path = Path(str(MANIFEST_PATH) + ".bak")
        if backup_path.exists():
            logger.info("Attempting to restore from backup...")
            try:
                with open(backup_path, 'r') as f:
                    manifest = json.load(f)
                    logger.info(f"Restored {len(manifest)} entries from backup")
                    return manifest
            except json.JSONDecodeError:
                logger.error("Backup also corrupted")
        
        return {}

def save_manifest_atomic(manifest: Dict):
    """
    Atomically save manifest.json with backup.
    Uses atomic write pattern: write to temp file, then rename.
    """
    # Create backup of existing manifest
    if MANIFEST_PATH.exists():
        backup_path = Path(str(MANIFEST_PATH) + ".bak")
        try:
            shutil.copy2(MANIFEST_PATH, backup_path)
            logger.debug(f"Created manifest backup: {backup_path}")
        except Exception as e:
            logger.error(f"Failed to create manifest backup: {e}")
            raise
    
    # Write to temporary file first
    temp_fd, temp_path = tempfile.mkstemp(
        dir=DATA_DIR,
        prefix=".manifest_",
        suffix=".json.tmp"
    )
    
    try:
        with os.fdopen(temp_fd, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        # Atomic rename
        shutil.move(temp_path, MANIFEST_PATH)
        logger.debug("Manifest saved atomically")
        
    except Exception as e:
        # Clean up temp file on error
        try:
            os.unlink(temp_path)
        except:
            pass
        logger.error(f"Failed to save manifest: {e}")
        raise

# --- Cleanup Logic ----------------------------------------------------

def find_stale_entries(manifest: Dict) -> Dict[str, Dict]:
    """
    Find manifest entries where the corresponding file no longer exists.
    Returns dict of {url: entry} for stale entries.
    """
    stale_entries = {}
    
    logger.info("Scanning manifest for stale entries...")
    
    for url, entry in manifest.items():
        saved_path = entry.get("saved_path")
        
        if not saved_path:
            logger.warning(f"Entry missing 'saved_path': {url}")
            stale_entries[url] = entry
            continue
        
        # Check if file exists
        if not os.path.exists(saved_path):
            logger.debug(f"File missing: {saved_path}")
            stale_entries[url] = entry
    
    return stale_entries

def find_orphaned_files(manifest: Dict) -> List[Path]:
    """
    Find files on disk that aren't tracked in the manifest.
    Returns list of orphaned file paths.
    """
    logger.info("Scanning filesystem for orphaned files...")
    
    # Get all tracked file paths from manifest
    tracked_paths = set()
    for entry in manifest.values():
        saved_path = entry.get("saved_path")
        if saved_path:
            tracked_paths.add(os.path.normpath(saved_path))
    
    # Scan filesystem
    orphaned = []
    valid_extensions = (".xlsx", ".csv", ".pdf", ".docx", ".doc", ".zip", ".xls")
    
    for root, dirs, files in os.walk(DATA_DIR):
        # Skip logs directory
        if "logs" in Path(root).parts:
            continue
        
        for file in files:
            # Skip manifest files
            if file in ["manifest.json", "manifest.json.bak"]:
                continue
            
            # Only check valid data files
            if not any(file.lower().endswith(ext) for ext in valid_extensions):
                continue
            
            filepath = os.path.normpath(os.path.join(root, file))
            
            if filepath not in tracked_paths:
                orphaned.append(Path(filepath))
    
    return orphaned

def cleanup_stale_entries(manifest: Dict, stale_entries: Dict[str, Dict]) -> Dict:
    """
    Remove stale entries from manifest.
    Returns cleaned manifest.
    """
    if not stale_entries:
        logger.info("No stale entries to clean")
        return manifest
    
    logger.info(f"Removing {len(stale_entries)} stale entries from manifest...")
    
    cleaned_manifest = manifest.copy()
    
    for url in stale_entries.keys():
        del cleaned_manifest[url]
    
    logger.info(f"Manifest cleaned: {len(manifest)} → {len(cleaned_manifest)} entries")
    
    return cleaned_manifest

def generate_report(stale_entries: Dict[str, Dict], orphaned_files: List[Path]):
    """Generate detailed cleanup report."""
    logger.info("\n" + "="*60)
    logger.info("CLEANUP REPORT")
    logger.info("="*60)
    
    # Stale entries report
    if stale_entries:
        logger.info(f"\nStale Manifest Entries: {len(stale_entries)}")
        logger.info("-" * 40)
        
        # Group by program
        by_program = {}
        for url, entry in stale_entries.items():
            program = entry.get("program", "Unknown")
            if program not in by_program:
                by_program[program] = []
            by_program[program].append(entry)
        
        for program, entries in sorted(by_program.items()):
            logger.info(f"\n  {program}: {len(entries)} files")
            for entry in entries[:5]:  # Show first 5
                logger.info(f"    - {entry.get('filename', 'unknown')}")
            if len(entries) > 5:
                logger.info(f"    ... and {len(entries) - 5} more")
    else:
        logger.info("\nNo stale manifest entries found ✓")
    
    # Orphaned files report
    if orphaned_files:
        logger.info(f"\nOrphaned Files (on disk but not in manifest): {len(orphaned_files)}")
        logger.info("-" * 40)
        
        # Group by directory
        by_dir = {}
        for filepath in orphaned_files:
            parent = filepath.parent
            if parent not in by_dir:
                by_dir[parent] = []
            by_dir[parent].append(filepath)
        
        for directory, files in sorted(by_dir.items()):
            logger.info(f"\n  {directory.relative_to(DATA_DIR)}: {len(files)} files")
            for filepath in files[:5]:  # Show first 5
                logger.info(f"    - {filepath.name}")
            if len(files) > 5:
                logger.info(f"    ... and {len(files) - 5} more")
        
        logger.info("\n  Note: Orphaned files are NOT automatically deleted.")
        logger.info("  Review them manually and delete if necessary.")
    else:
        logger.info("\nNo orphaned files found ✓")
    
    logger.info("\n" + "="*60)

def handle_orphaned_files(orphaned_files: List[Path], interactive: bool = False):
    """
    Handle orphaned files (files on disk not in manifest).
    
    Args:
        orphaned_files: List of orphaned file paths
        interactive: If True, ask user what to do. If False, just report.
    """
    if not orphaned_files:
        return
    
    if not interactive:
        logger.info("\nOrphaned files detected but not handling (non-interactive mode)")
        logger.info("Run with --interactive flag to handle them")
        return
    
    logger.info("\nWhat would you like to do with orphaned files?")
    logger.info("  1. Keep them (do nothing)")
    logger.info("  2. Delete them")
    logger.info("  3. Create manifest entries for them (rebuild)")
    
    # In automated mode, we should never delete or modify
    # This is just a placeholder for manual runs
    logger.warning("Interactive mode not implemented in automated runs")
    logger.info("Orphaned files will be left as-is")

# --- Main Entry Point -------------------------------------------------

def main():
    """Main cleanup routine."""
    logger.info("="*60)
    logger.info(f"Manifest Cleanup - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    # Load manifest
    manifest = load_manifest()
    if not manifest:
        logger.error("Cannot proceed without valid manifest")
        return 1
    
    original_count = len(manifest)
    logger.info(f"Original manifest entries: {original_count}")
    
    # Find stale entries (in manifest but file missing)
    stale_entries = find_stale_entries(manifest)
    
    # Find orphaned files (file exists but not in manifest)
    orphaned_files = find_orphaned_files(manifest)
    
    # Generate report
    generate_report(stale_entries, orphaned_files)
    
    # Clean up stale entries
    if stale_entries:
        logger.info("\nCleaning stale entries...")
        cleaned_manifest = cleanup_stale_entries(manifest, stale_entries)
        
        # Save cleaned manifest atomically
        try:
            save_manifest_atomic(cleaned_manifest)
            logger.info("✓ Manifest cleaned and saved successfully")
            logger.info(f"  Removed: {len(stale_entries)} entries")
            logger.info(f"  Remaining: {len(cleaned_manifest)} entries")
        except Exception as e:
            logger.error(f"✗ Failed to save cleaned manifest: {e}")
            return 1
    else:
        logger.info("\n✓ No cleanup needed - manifest is healthy")
    
    # Handle orphaned files (just report in automated mode)
    if orphaned_files:
        handle_orphaned_files(orphaned_files, interactive=False)
    
    logger.info("\n" + "="*60)
    logger.info("Cleanup completed successfully")
    logger.info("="*60)
    
    return 0

if __name__ == "__main__":
    exit(main())