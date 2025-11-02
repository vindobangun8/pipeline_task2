
from datetime import datetime
from sqlalchemy import create_engine
from pangres import upsert

from src.utils.helper import etl_log, handle_error
from src.utils.config import warehouse


def load_warehouse(data, table_name: str, source:str):
    try:
        # create connection to database
        conn = create_engine(f"postgresql+psycopg2://{warehouse['user']}:{warehouse['password']}@{warehouse['host']}:{warehouse['port']}/{warehouse['db']}")


        data.to_sql(table_name, conn, schema='public', if_exists='append', index=False)

        component = f"load from {source}"
        #create success log message
        log_msg = {
                "step" : "warehouse",
                "component": component,
                "status": "success",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        # return data
    except Exception as e:
        #create fail log message
        log_msg = {
            "step" : "warehouse",
            "component":source,
            "status": "failed",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") , # Current timestamp
            "error_msg": str(e)
        }

        # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='minio-container', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        etl_log(log_msg)

    