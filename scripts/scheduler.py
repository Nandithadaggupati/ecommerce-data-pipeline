import schedule
import time
import logging
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    try:
        logger.info("Starting pipeline")
        subprocess.run(['python', 'scripts/pipeline_orchestrator.py'])
    except Exception as e:
        logger.error(str(e))

if __name__ == "__main__":
    schedule.every().day.at("02:00").do(run_pipeline)
    while True:
        schedule.run_pending()
        time.sleep(60)
