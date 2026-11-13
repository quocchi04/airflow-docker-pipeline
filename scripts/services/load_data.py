from email.mime import text
from operator import le
from time import strftime
import pandas as pd
import requests

from datetime import datetime, timedelta, timezone
from sqlalchemy import text, Table, MetaData
from sqlalchemy.engine import URL, truncate_table
from scripts.utils.db_utils import (
    check_if_table_exists, 
    establich_wh_connection, 
    table_is_empty, 
    add_metadata, 
    truncate_table,
    get_existing_columns,
)

MODIFIED_AT_COLUMN = "systemmodifiedat"

def get_last_modified(conn, table, schema, minutes_back: int = 0):
    query = f"SELECT COALESCE(MAX({MODIFIED_AT_COLUMN}), '2000-01-01') FROM {schema}.{table}"
    result = conn.execute(text(query))
    utc_timestamp_str = result.fetchone()[0]
    
    utc_timestamp_str_back = utc_timestamp_str - timedelta(minutes=minutes_back)
    return utc_timestamp_str_back.strftime("%Y-%m-%dT%H:%M:%SZ")

def transform_data(data) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df.columns = df.columns.str.lower()
    df["execution_time"] = datetime.now(timezone.utc)
    return df

# def fetch_inctremental_data(src_endpoint, last_modified = None):

def persist_data_to_dwh(df, dest_table, dest_schema):
    wh_conn = establich_wh_connection()
    with wh_conn.connect() as conn:
        with conn.begin() as transaction:
            try:
                if check_if_table_exists(conn, dest_schema, dest_table):
                    truncate_table(df, conn, dest_schema, dest_table)
            except Exception as e:
                print(f"Failed to persist to {dest_schema}.{dest_table}.Error: {str(e)}")
                raise
    print(f"Data persited to {dest_schema}.{dest_table} successfully.")
    
def full_load(source_endpoint, dest_table, dest_schema, isTablebased=False, metadata=[]):
    print(f"Start full loading...")
    print(f"Loading with pagination...")
    all_data = []
    next_url = source_endpoint
    while next_url:
        response = requests.get(next_url)
        new_data = response.get("values", [])
        all_data.extend(new_data)
        next_url = response.get("@odata.nextLink")
    
    if not all_data:
        print("No data found to load.")
        return
    print(f"Fetched {len(all_data)} rows from the source endpoint.")

    df = transform_data(all_data)

    if metadata:
        df = add_metadata(df, metadata)

    if not isTablebased:
        print("Performing data to DWH _WITHOUT_ table-based filtering...")
        

def insert_data(df: pd.DataFrame, conn, table: str, schema: str):
    print(f"Inserting {len(df)} rows into {schema}.{table}...")
    try:
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="append",
            index=False,
        )
        print(f"Successfully inserted {len(df)} rows into {schema}.{table}.")
    except Exception as e:
        print(f"Failed to insert data into {schema}.{table}: {e}")
        raise
def fetch_new_data(conn, source_endpoint, dest_table, dest_schema, mode):
    if mode == "full_load" or table_is_empty(conn, dest_table, dest_schema, minutes_back=20):
        print(f"Table is empty. Performing full load...")
        last_modified = None
    else:
        last_modified = get_last_modified(conn, dest_table, dest_schema)
        print(f"Last modified timestamp: {last_modified}")
        
    print("Loading with pagination...")
    all_data = []
    next_url = source_endpoint
    first_request = True
    
    while next_url:
        response = fetch_new_data
        new_data = response.json().get("values", [])
        all_data.extend(new_data)
        next_url = response.json().get("@odata.nextLink")
        
        if first_request and last_modified:
            all_data = [row for row in all_data if row.get(MODIFIED_AT_COLUMN) > last_modified]
            first_request = False
            
def upsert(conn, new_data, schema, table, keys):
    metadata = Metadata()

def incremental_load(source_endpoint, dest_table, dest_schema, key_column, mode, metadata=[]):
    print(f"Start incremental loading...")
    wh_conn = establich_wh_connection()
    keys = key_column.split(",")

    new_data, last_modified = fetch_new_data(wh_conn, source_endpoint, dest_schema, mode)
    table_columns = get_existing_columns(establich_wh_connection(), dest_schema, dest_table)
    
    if not new_data:
        print("No new data to process.")
        return
    
    new_df = transform_data(new_data)
    for col in new_data.columns:
        if col not in table_columns:
            print(f"Warning column: '{col}' does not exitst in destionation table and will be ignored.")
    new_df = new_df[[col for col in new_df.columns if col in table_columns]]
    
    if metadata:
        new_df = add_metadata(new_df, metadata, dest_table, dest_schema, table_columns=table_columns)
    wh_conn = establich_wh_connection()
    with wh_conn.connect() as conn:
        trans = conn.begin()
        try:
            if last_modified:
                upser
    