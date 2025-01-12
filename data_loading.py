"""
Module: data_loading.py
Description: This script handles loading transformed data into a centralized data warehouse (e.g., Snowflake, AWS Redshift) with storage optimizations.
Author: Satej
"""

import snowflake.connector
import pandas as pd

# Configuration for Snowflake connection
SNOWFLAKE_CONFIG = {
    "user": "satej_user",
    "password": "satej_password",
    "account": "satej_account",
    "warehouse": "satej_warehouse",
    "database": "ecommerce",
    "schema": "public"
}

INPUT_FILE = "transformed_data.csv"  # File produced by data_transformation.py


def load_to_snowflake(data):
    """
    Loads transformed data into a Snowflake data warehouse.
    Args:
        data (pd.DataFrame): Transformed data to load.
    """
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        # Example: Create a table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id STRING,
                customer_id STRING,
                transaction_date TIMESTAMP,
                transaction_value FLOAT,
                quantity INT,
                revenue_per_transaction FLOAT,
                profit_margin FLOAT
            )
        """)

        # Insert data row by row
        for _, row in data.iterrows():
            cursor.execute("""
                INSERT INTO transactions VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

        conn.commit()
        print("Data successfully loaded into Snowflake.")
    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")
    finally:
        if conn:
            conn.close()


def main():
    """
    Main function to read transformed data and load it into Snowflake.
    """
    try:
        # Read transformed data
        data = pd.read_csv(INPUT_FILE)

        # Load data into Snowflake
        load_to_snowflake(data)
    except Exception as e:
        print(f"Error in data loading process: {e}")


if __name__ == "__main__":
    main()
