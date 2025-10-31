from src.integration.staging.extract import extract_database,extract_spreadsheet
from src.utils.config import source,sheets,list_tables_db,list_tables_sheet
from src.integration.staging.load import load_staging


def data_pipeline():
    # Extract and Load from Database
    for table in list_tables_db:
        data = extract_database(table_name=table)
        load_staging(data=data, table_name=table, source="database")
    
    # Extract and Load from Spreadsheet
    for table in list_tables_sheet:
        data = extract_spreadsheet(worksheet_name=table, key_file=sheets['key_spredsheet'])
        load_staging(data=data, table_name=table, source="spreadsheet")

if __name__ == "__main__":
    data_pipeline()
