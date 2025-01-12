"""
Module: data_transformation.py
Description: This script handles the cleaning, standardization, and transformation of raw data.
Author: Satej
"""

import pandas as pd

INPUT_FILE = "extracted_data.csv"  # File produced by the data_extraction.py script
OUTPUT_FILE = "transformed_data.csv"  # File to save the transformed data


def clean_data(data):
    """
    Cleans the data by handling missing values, removing duplicates, and fixing column types.
    Args:
        data (pd.DataFrame): Raw data.
    Returns:
        pd.DataFrame: Cleaned data.
    """
    # Drop duplicates
    data = data.drop_duplicates()

    # Handle missing values
    data = data.fillna({"customer_id": "Unknown", "transaction_value": 0.0})

    # Convert columns to appropriate data types
    data["transaction_date"] = pd.to_datetime(data["transaction_date"], errors="coerce")
    data["transaction_value"] = pd.to_numeric(data["transaction_value"], errors="coerce")
    
    return data


def enrich_data(data):
    """
    Enriches the data by adding calculated fields such as revenue per transaction.
    Args:
        data (pd.DataFrame): Cleaned data.
    Returns:
        pd.DataFrame: Enriched data.
    """
    # Add calculated fields
    data["revenue_per_transaction"] = data["transaction_value"] / data["quantity"]
    data["profit_margin"] = data["transaction_value"] * 0.2  # Example: assuming 20% margin

    return data


def transform_data(input_file, output_file):
    """
    Transforms the raw data into a cleaned and enriched format.
    Args:
        input_file (str): Path to the input file.
        output_file (str): Path to save the transformed data.
    """
    try:
        # Read raw data
        data = pd.read_csv(input_file)

        # Apply cleaning and enrichment
        cleaned_data = clean_data(data)
        transformed_data = enrich_data(cleaned_data)

        # Save transformed data
        transformed_data.to_csv(output_file, index=False)
        print(f"Data transformation completed and saved to '{output_file}'.")
    except Exception as e:
        print(f"Error transforming data: {e}")


if __name__ == "__main__":
    transform_data(INPUT_FILE, OUTPUT_FILE)
