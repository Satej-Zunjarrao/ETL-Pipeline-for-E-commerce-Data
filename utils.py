"""
Module: utils.py
Description: Contains reusable utility functions for logging, error handling, and other common operations.
Author: Satej
"""

import logging
from datetime import datetime

# Configuration for logging
LOG_FILE = "etl_pipeline.log"
LOG_LEVEL = logging.INFO

# Initialize the logger
logging.basicConfig(
    filename=LOG_FILE,
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_info(message):
    """
    Logs an informational message to the log file.
    Args:
        message (str): The message to log.
    """
    logging.info(message)
    print(f"[INFO] {message}")


def log_error(message):
    """
    Logs an error message to the log file.
    Args:
        message (str): The message to log.
    """
    logging.error(message)
    print(f"[ERROR] {message}")


def measure_execution_time(func):
    """
    Decorator to measure the execution time of a function.
    Args:
        func (function): Function to measure.
    Returns:
        function: Wrapped function with execution time measurement.
    """
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        log_info(f"Execution time for {func.__name__}: {execution_time} seconds")
        return result
    return wrapper


def validate_dataframe(data, required_columns):
    """
    Validates if the DataFrame contains the required columns.
    Args:
        data (pd.DataFrame): DataFrame to validate.
        required_columns (list): List of required column names.
    Returns:
        bool: True if validation passes, False otherwise.
    """
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        log_error(f"Missing columns in DataFrame: {missing_columns}")
        return False
    return True
