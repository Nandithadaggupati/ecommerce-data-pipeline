import subprocess
import time
import json
import datetime
import os
import sys

def run_step_with_retry(step_name, script_path, max_retries=3):
    print(f"\n--- Starting Step: {step_name} ---")
    start_time = time.time()
    for attempt in range(1, max_retries + 1):
        try:
            result = subprocess.run([sys.executable, script_path], capture_output=True, text=True, check=True)
            duration = round(time.time() - start_time, 2)
            print(f"[{step_name}] Success in {duration}s on attempt {attempt}")
            return {"status": "success", "duration_seconds": duration, "retry_attempts": attempt - 1, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            print(f"[{step_name}] Failed on attempt {attempt}: \n{e.stderr}")
            if attempt < max_retries:
                sleep_time = 2 ** attempt
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                duration = round(time.time() - start_time, 2)
                print(f"[{step_name}] Max retries exceeded.")
                return {"status": "failed", "duration_seconds": duration, "retry_attempts": attempt - 1, "error_message": e.stderr}

def orchestrate_pipeline():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    pipeline_start = time.time()
    
    steps = [
        {"name": "Data Generation", "path": "scripts/data_generation/generate_data.py"},
        {"name": "Data Ingestion", "path": "scripts/ingestion/ingest_to_staging.py"},
        {"name": "Data Quality Checks", "path": "scripts/quality_checks/validate_data.py"},
        {"name": "Staging to Production", "path": "scripts/transformation/staging_to_production.py"},
        {"name": "Load Warehouse", "path": "scripts/transformation/load_warehouse.py"},
        {"name": "Generate Analytics", "path": "scripts/transformation/generate_analytics.py"},
        {"name": "Pipeline Monitoring", "path": "scripts/monitoring/pipeline_monitor.py"}
    ]
    
    report = {
        "pipeline_execution_id": f"PIPE_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "start_time": datetime.datetime.now().isoformat(),
        "steps_executed": {},
        "errors": [],
        "warnings": []
    }
    
    pipeline_status = "success"
    
    for step in steps:
        result = run_step_with_retry(step["name"], step["path"])
        
        # Ensure 'result' avoids KeyError if a script lacks permissions or python path fails
        if result is None:
            result = {"status": "failed", "duration_seconds": 0, "retry_attempts": 0, "error_message": "Unknown error running script."}
            
        report["steps_executed"][step["name"]] = {
            "status": "success" if result["status"] == "success" else "failed",
            "duration_seconds": result.get("duration_seconds", 0),
            "retry_attempts": result.get("retry_attempts", 0),
            "error_message": result.get("error_message", None)
        }
        
        if result["status"] != "success":
            pipeline_status = "failed"
            report["errors"].append(f"{step['name']} failed permanently after max retries.")
            break
            
    report["status"] = pipeline_status
    report["end_time"] = datetime.datetime.now().isoformat()
    report["total_duration_seconds"] = round(time.time() - pipeline_start, 2)
    
    with open("data/processed/pipeline_execution_report.json", "w") as f:
        json.dump(report, f, indent=4)
        
    log_filename = f"logs/pipeline_orchestrator_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    with open(log_filename, "w") as f:
        f.write(json.dumps(report, indent=4))
        
    print(f"\nPipeline Finished with status: {pipeline_status}. Total duration: {report['total_duration_seconds']}s")

if __name__ == "__main__":
    orchestrate_pipeline()
