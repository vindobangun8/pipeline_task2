
from sqlalchemy import create_engine
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread

from src.utils.helper import etl_log, read_etl_log
from src.utils.config import  source,sheets

from datetime import datetime

## Database
def extract_database(table_name: str): 
    
    try:
        # create connection to database
        conn = create_engine(f"postgresql+psycopg2://{source['user']}:{source['password']}@{source['host']}:{source['port']}/{source['db']}")

        # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
        query = f"""
        SELECT * 
        FROM {table_name} 
        """

        #Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn)
        log_msg = {
                "step" : "staging",
                "component":"extraction database",
                "status": "success",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return df
    except Exception as e:
        log_msg = {
            "step" : "staging",
            "component":"extraction database",
            "status": "failed",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)


## Google Sheet

def auth_gspread():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    #Define your credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(sheets['cred_path'], scope) # Your json file here

    gc = gspread.authorize(credentials)

    return gc

def init_key_file(key_file:str):
    #define credentials to open the file
    gc = auth_gspread()
    
    #open spreadsheet file by key
    sheet_result = gc.open_by_key(key_file)
    
    return sheet_result

def extract_sheet(key_file:str, worksheet_name: str) -> pd.DataFrame:
    # init sheet
    sheet_result = init_key_file(key_file)
    
    worksheet_result = sheet_result.worksheet(worksheet_name)
    
    df_result = pd.DataFrame(worksheet_result.get_all_values())
    
    # set first rows as columns
    df_result.columns = df_result.iloc[0]
    
    # get all the rest of the values
    df_result = df_result[1:].copy()
    
    return df_result

def extract_spreadsheet(worksheet_name: str, key_file: str):

    try:
        # extract data
        df_data = extract_sheet(worksheet_name = worksheet_name,
                                    key_file = key_file)
        df_data['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # success log message
        log_msg = {
                "step" : "staging",
                "component":"extraction spreadsheet",
                "status": "success",
                "table_name": worksheet_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
    except Exception as e:
        # fail log message
        log_msg = {
                "step" : "staging",
                "component":"extraction",
                "status": "failed",
                "table_name": worksheet_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
                "error_msg": str(e)
            }
        df_data = pd.DataFrame()
    finally:
        # load log to database
        etl_log(log_msg)
        
    return df_data
