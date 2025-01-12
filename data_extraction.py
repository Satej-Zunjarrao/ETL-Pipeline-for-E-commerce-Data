"""
Module: data_extraction.py
Description: This script handles the extraction of data from various sources, including databases, APIs, and flat files.
Author: Satej
"""

import os
import pandas as pd
import requests
import pyodbc

# Configuration: Replace these values with actual database credentials or API keys
DATABASE_CONFIG = {
    "server": "satej-database-server",
    "database": "ecommerce",
    "username": "db_user",
    "password": "db_password"
}

API_ENDPOINT = "https://api.satej.com/ecommerce_data"
API_KEY = "your_api_key"

DATA_DIR = "./data"  # Directory to store downloaded files


def extract_from_database():
    """
    Extracts data from a SQL database using pyodbc.
    Returns:
        pd.DataFrame: Extracted data as a pandas DataFrame.
    """
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={DATABASE_CONFIG['server']};"
            f"DATABASE={DATABASE_CONFIG['database']};"
            f"UID={DATABASE_CONFIG['username']};"
            f"PWD={DATABASE_CONFIG['password']}"
        )
        query = "SELECT * FROM transactions;"  # Example query
        data = pd.read_sql(query, conn)
        conn.close()
        return data
    except Exception as e:
        print(f"Error extracting from database: {e}")
        return pd.DataFrame()


def extract_from_api():
    """
    Extracts data from an API endpoint using requests.
    Returns:
        pd.DataFrame: Extracted data as a pandas DataFrame.
    """
    try:
        response = requests.get(API_ENDPOINT, headers={"Authorization": f"Bearer {API_KEY}"})
        response.raise_for_status()  # Raise HTTPError for bad responses
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"Error extracting from API: {e}")
        return pd.DataFrame()


def extract_from_csv():
    """
    Reads data from local CSV files stored in the specified directory.
    Returns:
        pd.DataFrame: Consolidated data from all CSV files.
    """
    try:
        data_frames = []
        for file in os.listdir(DATA_DIR):
            if file.endswith(".csv"):
                file_path = os.path.join(DATA_DIR, file)
                data_frames.append(pd.read_csv(file_path))
        return pd.concat(data_frames, ignore_index=True)
    except Exception as e:
        print(f"Error extracting from CSV files: {e}")
        return pd.DataFrame()


def main():
    """
    Main function to extract data from all sources and consolidate into a single DataFrame.
    """
    db_data = extract_from_database()
    api_data = extract_from_api()
    csv_data = extract_from_csv()

    # Combine all data sources
    combined_data = pd.concat([db_data, api_data, csv_data], ignore_index=True)
    combined_data.to_csv("extracted_data.csv", index=False)  # Save combined data for further processing
    print("Data extraction completed and saved to 'extracted_data.csv'.")


if __name__ == "__main__":
    main()
