"""
Main Pipeline Orchestrator
Runs the complete ETL workflow: generation → ingestion → quality → transformation → warehouse
"""
import json
import logging
from datetime import datetime
from pathlib import Path
import sys
import traceback

# Import pipeline modules
from scripts.data_generation.generate_data import main as generate_data
from scripts.ingestion.ingest_to_staging import main as ingest_to_staging
from scripts.quality_checks.validate_data import main as validate_data
from scripts.transformation.staging_to_production import main as staging_to_production
from scripts.transformation.load_warehouse import main as load_warehouse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrate the complete ETL pipeline with error handling and logging."""

    def __init__(self):
        self.pipeline_start = datetime.utcnow().isoformat()
        self.execution_log = {
            "pipeline_start": self.pipeline_start,
            "steps": {},
            "overall_status": "pending",
            "errors": [],
        }
        Path("logs").mkdir(exist_ok=True)
        Path("data/processed").mkdir(parents=True, exist_ok=True)

    def run_step(self, step_name: str, step_func, *args, **kwargs) -> bool:
        """
        Run a pipeline step with error handling and logging.
        Returns True if successful, False otherwise.
        """
        try:
            logger.info(f"Starting step: {step_name}")
            result = step_func(*args, **kwargs)
            self.execution_log["steps"][step_name] = {
                "status": "success",
                "timestamp": datetime.utcnow().isoformat(),
                "result": result if isinstance(result, (dict, list, str, int, float)) else str(result),
            }
            logger.info(f"✓ Completed step: {step_name}")
            return True

        except Exception as e:
            error_msg = f"{step_name} failed: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self.execution_log["steps"][step_name] = {
                "status": "failed",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
            }
            self.execution_log["errors"].append(error_msg)
            return False

    def execute_pipeline(self) -> dict:
        """
        Execute the complete pipeline in sequence.
        Stops at first failure or continues based on configuration.
        """
        logger.info("=" * 80)
        logger.info("ECOMMERCE DATA PIPELINE ORCHESTRATION STARTED")
        logger.info("=" * 80)

        all_success = True

        # Step 1: Data Generation
        logger.info("\n[STEP 1/5] Data Generation")
        if not self.run_step("data_generation", generate_data):
            all_success = False
            logger.warning("Data generation failed; attempting to continue with existing data...")

        # Step 2: Ingestion to Staging
        logger.info("\n[STEP 2/5] Ingestion to Staging Schema")
        if not self.run_step("ingestion_to_staging", ingest_to_staging):
            all_success = False
            logger.error("Ingestion failed; cannot proceed to next steps.")
            self.execution_log["overall_status"] = "failed"
            self._save_execution_log()
            return self.execution_log

        # Step 3: Data Quality Validation
        logger.info("\n[STEP 3/5] Data Quality Validation")
        if not self.run_step("data_quality_validation", validate_data):
            all_success = False
            logger.warning("Quality checks found issues; proceeding with transformation...")

        # Step 4: Staging → Production Transformation
        logger.info("\n[STEP 4/5] Staging to Production Transformation")
        if not self.run_step("staging_to_production_transform", staging_to_production):
            all_success = False
            logger.error("Transformation failed; cannot proceed to warehouse load.")
            self.execution_log["overall_status"] = "failed"
            self._save_execution_log()
            return self.execution_log

        # Step 5: Warehouse Loading & Aggregates
        logger.info("\n[STEP 5/5] Warehouse Schema Loading")
        if not self.run_step("warehouse_load", load_warehouse):
            all_success = False
            logger.warning("Warehouse load had issues but pipeline completed with data in production.")

        # Final status
        self.execution_log["pipeline_end"] = datetime.utcnow().isoformat()
        self.execution_log["overall_status"] = "success" if all_success else "completed_with_warnings"

        logger.info("\n" + "=" * 80)
        logger.info(f"PIPELINE EXECUTION COMPLETED: {self.execution_log['overall_status'].upper()}")
        logger.info("=" * 80)

        self._save_execution_log()
        self._print_summary()

        return self.execution_log

    def _save_execution_log(self):
        """Save pipeline execution log to JSON."""
        log_path = Path("data/processed/pipeline_execution_log.json")
        with open(log_path, "w") as f:
            json.dump(self.execution_log, f, indent=2)
        logger.info(f"Execution log saved to {log_path}")

    def _print_summary(self):
        """Print a human-readable summary."""
        logger.info("\nPIPELINE EXECUTION SUMMARY:")
        logger.info("-" * 80)
        for step_name, step_result in self.execution_log["steps"].items():
            status = step_result.get("status", "unknown").upper()
            symbol = "✓" if status == "SUCCESS" else "✗"
            logger.info(f"{symbol} {step_name}: {status}")

        if self.execution_log["errors"]:
            logger.info(f"\nTotal Errors: {len(self.execution_log['errors'])}")
            for error in self.execution_log["errors"]:
                logger.error(f"  - {error[:100]}...")

        logger.info("-" * 80)


def main():
    """Main entry point."""
    orchestrator = PipelineOrchestrator()
    result = orchestrator.execute_pipeline()
    
    # Exit with appropriate code
    sys.exit(0 if result["overall_status"] == "success" else 1)


if __name__ == "__main__":
    main()
