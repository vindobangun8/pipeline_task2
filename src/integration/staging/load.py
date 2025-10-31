
from datetime import datetime
from sqlalchemy import create_engine,inspect
from pangres import upsert

from src.utils.helper import etl_log, handle_error
from src.utils.config import staging


def load_staging(data, table_name: str, source:str):
    try:
        # create connection to database
        conn = create_engine(f"postgresql+psycopg2://{staging['user']}:{staging['password']}@{staging['host']}:{staging['port']}/{staging['db']}")
        
        # to get primary key of table, for this project all tables have only one primary key
        inspector = inspect(conn)
        pk = inspector.get_pk_constraint(table_name)["constrained_columns"][0]


        # set data index or primary key
        data = data.set_index(pk)
        # Do upsert (Update for existing data and Insert for new data)
        upsert(con = conn,
                df = data,
                table_name = table_name,
                if_row_exists = "update")
        
        component = f"load from {source}"
        #create success log message
        log_msg = {
                "step" : "staging",
                f"component": component,
                "status": "success",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        # return data
    except Exception as e:
        #create fail log message
        log_msg = {
            "step" : "staging",
            "component":source,
            "status": "failed",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") , # Current timestamp
            "error_msg": str(e)
        }

        # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='error-dellstore', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        etl_log(log_msg)

    