
from src.utils.config import source,sheets
from src.integration.staging.load import load_staging
from src.utils.helper import list_tables

#Staging
from src.integration.staging.extract import extract_database,extract_spreadsheet

#Warehouse
from src.integration.warehouse.load import load_warehouse
from src.integration.warehouse.extract import extract_staging
from src.integration.warehouse.transform import transform_customer,transform_employee,transform_store_branch
from src.integration.warehouse.transform import transform_product,transform_order,transform_inventory_tracking


def data_pipeline():
    # EL from Source to Staging
    # Extract and Load from Database
    list_tables_db = list_tables(source)
    for index,row in list_tables_db.iterrows():
        data = extract_database(table_name=row['table_name'])
        load_staging(data=data, table_name=row['table_name'], source="database")

    # Extract and Load from Spreadsheet
    data = extract_spreadsheet(worksheet_name='store_branch', key_file=sheets['key_spreadsheet'])
    load_staging(data=data, table_name='store_branch', source="spreadsheet")

    
    #ETL to Data Warehouse
    # customers done
    df_customers = extract_staging(table_name='customers')
    df_customers_transformed = transform_customer(data = df_customers,table_name='customers')
    load_warehouse(data=df_customers_transformed, table_name='dim_customers', source='staging')

    # #employee done
    df_employee = extract_staging(table_name='employees')
    df_employee_transformed = transform_employee(data = df_employee,table_name='employees')
    load_warehouse(data=df_employee_transformed, table_name='dim_employees', source='staging')

    # # store branch done
    df_store_branch = extract_staging(table_name='store_branch')
    df_store_branch = transform_store_branch(data = df_store_branch,table_name='store_branch')
    load_warehouse(data=df_store_branch, table_name='dim_store_branch', source='staging')


    # # product done
    df_product = extract_staging(table_name='products')
    df_product_transformed = transform_product(data = df_product,table_name='products')
    load_warehouse(data=df_product_transformed, table_name='dim_products', source='staging')

    # #order
    df_order = extract_staging(table_name='orders')
    df_order_transformed = transform_order(data = df_order,table_name='orders')
    load_warehouse(data=df_order_transformed, table_name='fct_order', source='staging')

    # #inventory tracking
    df_inventory = extract_staging(table_name='inventory_tracking')
    df_inventory_transformed = transform_inventory_tracking(data = df_inventory,table_name='inventory')
    load_warehouse(data=df_inventory_transformed, table_name='fct_inventory', source='staging')

if __name__ == "__main__":
    data_pipeline()
