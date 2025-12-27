import os
from datetime import datetime, timedelta
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleanup:
    def __init__(self, retention_days=30):
        self.retention_days = retention_days
        self.cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    def cleanup_raw_data(self):
        raw_dir = Path('data/raw')
        if not raw_dir.exists():
            logger.warning(f"Raw data directory not found")
            return
        
        removed_count = 0
        for file in raw_dir.glob('*.csv'):
            file_mtime = datetime.fromtimestamp(file.stat().st_mtime)
            if file_mtime < self.cutoff_date:
                file.unlink()
                removed_count += 1
                logger.info(f"Removed: {file.name}")
        
        logger.info(f"Cleanup complete: Removed {removed_count} files")

if __name__ == "__main__":
    cleanup = DataCleanup(retention_days=30)
    cleanup.cleanup_raw_data()
