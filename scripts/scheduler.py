import schedule
import time
import subprocess
import os
import sys

def run_pipeline():
    print("Initiating scheduled pipeline run...")
    subprocess.run([sys.executable, "scripts/pipeline_orchestrator.py"])
    subprocess.run([sys.executable, "scripts/cleanup_old_data.py"])
    print("Scheduled tasks complete.")

if __name__ == "__main__":
    schedule.every().day.at("02:00").do(run_pipeline)
    print("Scheduler started. Waiting for next execution...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("Scheduler stopped.")
