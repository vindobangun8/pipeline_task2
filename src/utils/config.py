from dotenv import load_dotenv
import os


load_dotenv(".env")

source = {
"user" : os.getenv("SRC_POSTGRES_USER"),
"password":os.getenv("SRC_POSTGRES_PASSWORD"),
"port":os.getenv("SRC_POSTGRES_PORT"),
"db":os.getenv("SRC_POSTGRES_DB"),
"host":os.getenv("SRC_POSTGRES_HOST"),

}

warehouse = {
"user" : os.getenv("WH_POSTGRES_USER"),
"password":os.getenv("WH_POSTGRES_PASSWORD"),
"port":os.getenv("WH_POSTGRES_PORT"),
"db":os.getenv("WH_POSTGRES_DB"),
"host":os.getenv("WH_POSTGRES_HOST"),

}
staging = {
"user" : os.getenv("STG_POSTGRES_USER"),
"password":os.getenv("STG_POSTGRES_PASSWORD"),
"port":os.getenv("STG_POSTGRES_PORT"),
"db":os.getenv("STG_POSTGRES_DB"),
"host":os.getenv("STG_POSTGRES_HOST"),

}

log = {
"user" : os.getenv("LOG_POSTGRES_USER"),
"password":os.getenv("LOG_POSTGRES_PASSWORD"),
"port":os.getenv("LOG_POSTGRES_PORT"),
"db":os.getenv("LOG_POSTGRES_DB"),
"host":os.getenv("LOG_POSTGRES_HOST"),

}

minio ={
    "access_key" : os.getenv("MINIO_ACCESS_KEY"),
    "secret_key" : os.getenv("MINIO_SECRET_KEY")
}

sheets = {
"cred_path": os.getenv("CRED_PATH"),
"key_spredsheet": os.getenv("KEY_SPREADSHEET")
}

list_tables_db = os.getenv("LIST_TABLES_DB").split(",")
list_tables_sheet = os.getenv("LIST_TABLES_SHEET").split(",")