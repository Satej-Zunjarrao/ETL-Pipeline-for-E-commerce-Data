"""
Module: main.py
Description: Orchestrates the ETL pipeline by integrating all modules (extraction, transformation, loading, and visualization).
Author: Satej
"""

from data_extraction import main as extract_data
from data_transformation import transform_data
from data_loading import main as load_data
from visualization_setup import main as setup_visualization
from utils import log_info, log_error, measure_execution_time

@measure_execution_time
def run_etl_pipeline():
    """
    Executes the complete ETL pipeline: extraction, transformation, loading, and visualization.
    """
    try:
        log_info("Starting the ETL pipeline.")
        
        # Step 1: Data Extraction
        log_info("Step 1: Data Extraction")
        extract_data()
        
        # Step 2: Data Transformation
        log_info("Step 2: Data Transformation")
        transform_data("extracted_data.csv", "transformed_data.csv")
        
        # Step 3: Data Loading
        log_info("Step 3: Data Loading")
        load_data()
        
        # Step 4: Visualization Setup
        log_info("Step 4: Visualization Setup")
        setup_visualization()
        
        log_info("ETL pipeline execution completed successfully.")
    except Exception as e:
        log_error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    run_etl_pipeline()
