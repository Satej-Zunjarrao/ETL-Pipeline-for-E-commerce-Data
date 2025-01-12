"""
Module: visualization_setup.py
Description: Configures connections to visualization tools (Tableau, Power BI) and integrates pre-processed data for reporting.
Author: Satej
"""

import os
import pandas as pd
from sqlalchemy import create_engine

# Configuration for database connection (Snowflake example here; modify for Redshift or others)
VISUALIZATION_DB_CONFIG = {
    "user": "satej_user",
    "password": "satej_password",
    "account": "satej_account",
    "warehouse": "satej_warehouse",
    "database": "ecommerce",
    "schema": "public"
}

TABLEAU_EXPORT_DIR = "./tableau_exports"  # Directory to store Tableau-compatible data files


def create_db_engine():
    """
    Creates a SQLAlchemy engine for connecting to the visualization database.
    Returns:
        sqlalchemy.engine.Engine: Database connection engine.
    """
    try:
        engine_url = (
            f"snowflake://{VISUALIZATION_DB_CONFIG['user']}:{VISUALIZATION_DB_CONFIG['password']}"
            f"@{VISUALIZATION_DB_CONFIG['account']}/{VISUALIZATION_DB_CONFIG['database']}/{VISUALIZATION_DB_CONFIG['schema']}"
        )
        return create_engine(engine_url)
    except Exception as e:
        print(f"Error creating database engine: {e}")
        return None


def export_for_tableau(data, file_name="tableau_export.csv"):
    """
    Exports a DataFrame to a CSV file compatible with Tableau.
    Args:
        data (pd.DataFrame): Data to be exported.
        file_name (str): Name of the output file.
    """
    try:
        os.makedirs(TABLEAU_EXPORT_DIR, exist_ok=True)
        file_path = os.path.join(TABLEAU_EXPORT_DIR, file_name)
        data.to_csv(file_path, index=False)
        print(f"Data exported for Tableau to '{file_path}'.")
    except Exception as e:
        print(f"Error exporting data for Tableau: {e}")


def fetch_data_for_visualization(query):
    """
    Fetches data from the database for visualization purposes.
    Args:
        query (str): SQL query to fetch data.
    Returns:
        pd.DataFrame: Queried data as a DataFrame.
    """
    try:
        engine = create_db_engine()
        if engine:
            data = pd.read_sql(query, con=engine)
            print("Data fetched successfully for visualization.")
            return data
        else:
            print("Database engine creation failed.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data for visualization: {e}")
        return pd.DataFrame()


def main():
    """
    Main function to prepare data for visualization tools.
    """
    # Example SQL query to fetch key metrics
    query = """
    SELECT 
        transaction_date, 
        SUM(transaction_value) AS total_revenue, 
        AVG(profit_margin) AS avg_profit_margin
    FROM transactions
    GROUP BY transaction_date
    ORDER BY transaction_date;
    """

    # Fetch and export data for Tableau
    data = fetch_data_for_visualization(query)
    if not data.empty:
        export_for_tableau(data)


if __name__ == "__main__":
    main()
