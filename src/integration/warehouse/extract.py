
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

from src.utils.helper import etl_log
from src.utils.config import  staging,warehouse


def extract_staging(table_name: str): 
    
    try:
        # create connection to database
        conn = create_engine(f"postgresql+psycopg2://{staging['user']}:{staging['password']}@{staging['host']}:{staging['port']}/{staging['db']}")

        # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
        query = f"""
        SELECT * 
        FROM {table_name} 
        """

        #Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn)
        log_msg = {
                "step" : "warehouse",
                "component":"extraction database",
                "status": "success",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return df
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component":"extraction database",
            "status": "failed",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)

def extract_target(table_name: str):
    """
    this function is used to extract data from the data warehouse.
    """
    conn = create_engine(f"postgresql+psycopg2://{warehouse['user']}:{warehouse['password']}@{warehouse['host']}:{warehouse['port']}/{warehouse['db']}")

    # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
    query = f"SELECT * FROM {table_name}"

    # Execute the query with pd.read_sql
    df = pd.read_sql(sql=query, con=conn)
    
    return df

