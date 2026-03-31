import json
import os
import datetime
import pandas as pd
from sqlalchemy import create_engine, text

def check_freshness(connection):
    try:
        q = text("SELECT MAX(created_at) FROM warehouse.fact_sales")
        dt = connection.execute(q).scalar()
        if dt:
            hours_lag = (datetime.datetime.now() - dt).total_seconds() / 3600
        else:
            hours_lag = 999
        return {
            "status": "warning" if hours_lag > 24 else "ok",
            "warehouse_latest_record": dt.isoformat() if dt else None,
            "max_lag_hours": round(hours_lag, 2)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def monitor_pipeline():
    from dotenv import load_dotenv
    load_dotenv()
    
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "ecommerce_db")
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        with engine.connect() as conn:
            freshness = check_freshness(conn)
            db_status = "ok"
    except Exception as e:
        freshness = {"status": "critical"}
        db_status = "error"
        
    report = {
        "monitoring_timestamp": datetime.datetime.now().isoformat(),
        "pipeline_health": "healthy" if db_status == "ok" else "critical",
        "checks": {
            "data_freshness": freshness,
            "database_connectivity": {
                "status": db_status
            }
        },
        "overall_health_score": 100 if db_status == "ok" else 0
    }
    
    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/monitoring_report.json", "w") as f:
        json.dump(report, f, indent=4)

if __name__ == "__main__":
    monitor_pipeline()
