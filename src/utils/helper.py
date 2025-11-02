
from src.utils.config import  log
from sqlalchemy import create_engine
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime

from src.utils.config import minio 

#list table 
def list_tables (db: dict):
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """
    conn = create_engine(f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['db']}")
    table_list = pd.read_sql(query, conn)
    return table_list



# Logging 
def etl_log(log_msg: dict):

    try:
        # create connection to database
        conn = create_engine(f"postgresql+psycopg2://{log['user']}:{log['password']}@{log['host']}:{log['port']}/{log['db']}")
        
        # convert dictionary to dataframe
        df_log = pd.DataFrame([log_msg])

        #extract data log
        df_log.to_sql(name = "etl_log",  # Your log table
                        con = conn,
                        if_exists = "append",
                        index = False,
                        )
    except Exception as e:
        print("Can't save your log message. Cause: ", str(e))


def read_etl_log(filter_params: dict):
    """
    function read_etl_log that reads log information from the etl_log table and extracts the maximum etl_date for a specific process, step, table name, and status.
    """
    try:
        # create connection to database
        conn = create_engine(f"postgresql+psycopg2://{log['user']}:{log['password']}@{log['host']}:{log['port']}/{log['db']}")
        
        # To help with the incremental process, get the etl_date from the relevant process
        query = """
            SELECT MAX(etl_date)
            FROM etl_log
            WHERE 
                step = %s AND
                table_name ILIKE %s AND
                status = %s AND
                component = %s    
            """
        
        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn, params=(filter_params,))

        #return extracted data
        return df
    except Exception as e:
        print("Can't execute your query. Cause: ", str(e))



def handle_error(data, bucket_name:str, table_name:str):

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Initialize MinIO client
    client = Minio('localhost:9000',
                access_key=minio['access_key'],
                secret_key=minio['secret_key'],
                secure=False)

    # Make a bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Convert DataFrame to CSV and then to bytes
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    # Upload the CSV file to the bucket
    client.put_object(
        bucket_name=bucket_name,
        object_name=f"{table_name}_{current_date}.csv", #name the fail source name and current etl date
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv'
    )

    # List objects in the bucket
    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        print(obj.object_name)

