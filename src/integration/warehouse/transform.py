import pandas as pd
import re
from sqlalchemy import create_engine
from datetime import datetime

from src.integration.warehouse.extract import extract_target,extract_staging
from src.utils.helper import etl_log, handle_error

def transform_customer(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    try:
        process = "transformation"

        
        # rename column customer_id to nk_customer_id
        data = data.rename(columns={'customer_id':'nk_customer_id'})
        
        # remove duplicate nk_customer_id
        data = data.drop_duplicates(subset=['nk_customer_id'])

        # drop null values in phone column
        data = data.dropna(subset='phone')

        # remove negative values in loyaty_points
        data = data[data['loyalty_points'] >= 0]

        # change time created_at
        data['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        
        log_msg = {
                "step" : "warehouse",
                "component": process,
                "status": "success",
                "table_name": "customer",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component": process,
            "status": "failed",
            "table_name": "customer",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
        
         # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='minio-container', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        # Save the log message
        etl_log(log_msg)


def transform_employee(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
   

    try:
        process = "transformation"

        # rename column employee_id to nk_employee_id
        data = data.rename(columns={'employee_id':'nk_employee_id'})

        # remove duplicate nk_employee_id
        data = data.drop_duplicates(subset=['nk_employee_id'])

        # change hire_date format as integer

        data['hire_date'] = pd.to_datetime(data['hire_date'], errors='coerce')
        data['hire_date'] = data['hire_date'].dt.strftime('%Y%m%d').astype(int)

        # change time created_at
        data['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        
        log_msg = {
                "step" : "warehouse",
                "component": process,
                "status": "success",
                "table_name": "employee",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component": process,
            "status": "failed",
            "table_name": "employee",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
        
         # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='minio-container', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        # Save the log message
        etl_log(log_msg)

def transform_store_branch(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    try:
        process = "transformation"

        # rename column store_id to nk_store_id
        data = data.rename(columns={'store_id':'nk_store_id'})

        # remove duplicate nk_store_id
        data = data.drop_duplicates(subset=['nk_store_id'])

         # change time created_at
        data['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        
        log_msg = {
                "step" : "warehouse",
                "component": process,
                "status": "success",
                "table_name": "store_branch",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component": process,
            "status": "failed",
            "table_name": "store_branch",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
        
         # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='minio-container', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        # Save the log message
        etl_log(log_msg)


def transform_product(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    try:
        process = "transformation"

        # get store_branch data to merge with product data
        df_store = extract_target(table_name='dim_store_branch')
        data = data.merge(df_store[['store_name','sk_store_id']],left_on='store_branch',right_on='store_name', how='inner')

        # rename column product_id to nk_product_id and sk_store_id to sk_store_branch
        data = data.rename(columns={'product_id':'nk_product_id','sk_store_id':'sk_store_branch'})

        # remove duplicate nk_product_id
        data = data.drop_duplicates(subset=['nk_product_id'])

        # change numeric columns to appropriate data types
        data['unit_price'] = data['unit_price'].replace(r'[\$,\-]', '', regex=True).astype(float)
        data['cost_price'] = data['cost_price'].replace(r'[\$,\-]', '', regex=True).astype(float)


         # change time created_at
        data['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp

        # drop column store_name
        data = data.drop(columns=['store_name','store_branch'])
        
        log_msg = {
                "step" : "warehouse",
                "component": process,
                "status": "success",
                "table_name": "products",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component": process,
            "status": "failed",
            "table_name": "products",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
        
         # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='minio-container', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        # Save the log message
        etl_log(log_msg)

def transform_order(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    try:
        process = "transformation"

        # remove null values
        data = data.dropna(subset=['customer_id'])

        # get employee, customer, product data from warehouse to merge with order data
        df_emp = extract_target(table_name='dim_employees')
        df_emp = df_emp[['nk_employee_id','sk_employee_id']]

        df_cust = extract_target(table_name='dim_customers')
        df_cust = df_cust[['nk_customer_id','sk_customer_id']]

        df_prod = extract_target(table_name='dim_products')
        df_prod = df_prod[['nk_product_id','sk_product_id']]

        # get order_details data to merge with order data
        df_order_details = extract_staging(table_name='order_details')
        df_order_details = df_order_details.drop(columns=['created_at','order_detail_id'])

        #merging data
        data = data.merge(df_emp, left_on='employee_id', right_on='nk_employee_id', how='inner')
        data = data.merge(df_cust, left_on='customer_id', right_on='nk_customer_id', how='inner')
        data = data.merge(df_order_details, on='order_id', how='inner')
        data = data.merge(df_prod, left_on='product_id', right_on='nk_product_id', how='inner')

        # rename column order_id to nk_order_id 
        data = data.rename(columns={'order_id':'nk_order_id'})

        # remove duplicate nk_product_id
        data = data.drop_duplicates(subset=['nk_order_id'])

        # change order_date format as integer
        data['order_date'] = data['order_date'].dt.strftime('%Y%m%d').astype(int)

         # change time created_at
        data['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp

        #drop column
        data = data.drop(columns=['nk_employee_id','nk_customer_id','nk_product_id','employee_id','customer_id','product_id'])
        
        log_msg = {
                "step" : "warehouse",
                "component": process,
                "status": "success",
                "table_name": "order",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component": process,
            "status": "failed",
            "table_name": "order",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
        
         # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='minio-container', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        # Save the log message
        etl_log(log_msg)


def transform_inventory_tracking(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    try:
        process = "transformation"

        # get product data from warehouse to merge with order data
        df_prod = extract_target(table_name='dim_products')
        df_prod = df_prod[['nk_product_id','sk_product_id']]

        # merging data
        data = data.merge(df_prod, left_on='product_id', right_on='nk_product_id', how='inner')

        # rename column order_id to nk_tracking_id 
        data = data.rename(columns={'tracking_id':'nk_tracking_id'})

        # remove duplicate nk_tracking_id
        data = data.drop_duplicates(subset=['nk_tracking_id'])

        # change change_date format as integer
        data['change_date'] = pd.to_datetime(data['change_date'], errors='coerce')
        data['change_date'] = data['change_date'].dt.strftime('%Y%m%d').astype(int)

         # change time created_at
        data['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        
        # drop column 
        data = data.drop(columns=['product_id','nk_product_id'])
        
        log_msg = {
                "step" : "warehouse",
                "component": process,
                "status": "success",
                "table_name": "inventory_tracking",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component": process,
            "status": "failed",
            "table_name": "inventory_tracking",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
        
         # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='minio-container', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        # Save the log message
        etl_log(log_msg)





