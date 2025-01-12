"""
Module: config.py
Description: Centralized configuration for managing connections, file paths, and other settings.
Author: Satej
"""

# Database configuration
DATABASE_CONFIG = {
    "server": "satej-database-server",
    "database": "ecommerce",
    "username": "db_user",
    "password": "db_password"
}

# Snowflake configuration
SNOWFLAKE_CONFIG = {
    "user": "satej_user",
    "password": "satej_password",
    "account": "satej_account",
    "warehouse": "satej_warehouse",
    "database": "ecommerce",
    "schema": "public"
}

# AWS configuration
AWS_CONFIG = {
    "region_name": "us-west-2",
    "glue_job_name": "satej_realtime_etl_job",
    "s3_bucket": "satej-transformed-data"
}

# API configuration
API_CONFIG = {
    "endpoint": "https://api.satej.com/ecommerce_data",
    "api_key": "your_api_key"
}

# File paths
FILE_PATHS = {
    "raw_data_dir": "./data",
    "transformed_data_file": "transformed_data.csv",
    "tableau_export_dir": "./tableau_exports"
}

# General settings
LOGGING_CONFIG = {
    "log_file": "etl_pipeline.log",
    "log_level": "INFO"
}

# Helper function for path retrieval
def get_file_path(key):
    """
    Retrieves file paths based on the given key from FILE_PATHS.
    Args:
        key (str): Key for the file path.
    Returns:
        str: File path corresponding to the key.
    """
    return FILE_PATHS.get(key, None)
