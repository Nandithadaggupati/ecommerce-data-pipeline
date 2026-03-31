import os
import time
import datetime

def cleanup_files(directory, days=7, exclude_keywords=["summary", "report", "metadata"]):
    now = time.time()
    cutoff = now - (days * 86400)
    if not os.path.exists(directory): return
    for f in os.listdir(directory):
        path = os.path.join(directory, f)
        if os.path.isfile(path):
            # Skip important files
            if any(k in f.lower() for k in exclude_keywords): continue
            
            # Check modification time
            if os.path.getmtime(path) < cutoff:
                os.remove(path)
                print(f"Deleted old file: {path}")

if __name__ == "__main__":
    print("Running data retention cleanup...")
    cleanup_files("data/raw", days=7)
    cleanup_files("logs", days=30)
    print("Cleanup complete.")
