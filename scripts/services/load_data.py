import pandas as pd
import requests

from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from scripts.utils.db_utils import (
    check_if_table_exists, 
    establish_wh_connection, 
    table_is_empty, 
    add_metadata, 
    truncate_table,
    get_existing_columns,
)
VN_TZ = "Asia/Ho_Chi_Minh"
MODIFIED_AT_COLUMN = "time"

def get_last_modified(conn, schema, table):
    query = f"SELECT MAX({MODIFIED_AT_COLUMN}) FROM {schema}.{table}"
    return conn.execute(text(query)).scalar()

def transform_data(data, execution_time=None) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df.columns = df.columns.str.lower()
    df["execution_time"] = (
        pd.Timestamp(execution_time) if execution_time
        else pd.Timestamp.now(tz=VN_TZ)
    )
    return df

def insert_df(df: pd.DataFrame, conn, schema: str, table: str):
    print(f"Inserting {len(df)} row into {table}")
    try:
        df.to_sql(
            table,
            con = conn,
            schema=schema,
            if_exists="append",
            index=False,
        )
        print(f"Successfully inserted {len(df)} row into {table}")
    except Exception as e:
        print(f"Failed to insert into {table}. Error: {str(e)}")
        raise

# ----------Ghi df vào bảng dest_schema.dest_table trong dwh------------
def persist_data_to_dwh(df, dest_table, dest_schema):
    wh_conn = establish_wh_connection()
    with wh_conn.connect() as conn:
        with conn.begin() as transaction:
            try:
                if check_if_table_exists(conn, dest_schema, dest_table):
                    truncate_table(conn, dest_schema, dest_table)
                insert_df(df, conn, dest_schema, dest_table)
            except Exception as e:
                print(f"Failed to persist to {dest_schema}.{dest_table}.Error: {str(e)}")
                raise
    print(f"Data persited to {dest_schema}.{dest_table} successfully.")

def fetch_md_data(src_endpoint, params=None):
    response = requests.get(src_endpoint, params=params, timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        print(response.text)
        messages = f"{response.status_code}: {response}"
        raise Exception(messages)
        
def full_load(source_endpoint, dest_table, dest_schema, isTablebased=False, metadata=None, params=None, execution_time=None):
    print(f"Start full loading...")
    
    #--B1: Lấy dữ liệu từ API
    response = fetch_md_data(source_endpoint, params=params)
    current = response.get("current")
    all_data = [current] if current else []
    
    if not all_data:
        print("No data to process.")
        return
    print(f"{len(all_data)} source records")

    #--B2: Chuyển thành df
    df = transform_data(all_data, execution_time)
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(VN_TZ)

    #--B3: Gắn metadata
    if metadata:
        df = add_metadata(df, metadata, dest_schema, dest_table, auto_adjust = True)

    #--B4: Ghi vào dwh
    if not isTablebased:
        print("Performing data to DWH _WITHOUT_ table-based filtering...")
        persist_data_to_dwh(df, dest_schema=dest_schema, dest_table=dest_table)
    else:
        print("Persisting data to DWH _WITH_ table-based filtering...")
        table_columns = get_existing_columns(establish_wh_connection(), dest_schema, dest_table)
        df_filtered = df[[col for col in df.columns if col in table_columns]]
        if df_filtered.empty:
            print("No matching columns to insert after filtering. Exiting.")
            return
        if len(df_filtered) < len(df.columns):
            missing_cols = set(df.columns) - set(df_filtered.columns)
            print(f"Warning: The following columns are missing in the destination table and will be ignored: {missing_cols}")
        persist_data_to_dwh(df_filtered, dest_schema, dest_table)

def fetch_new_data(conn, source_endpoint, dest_schema, dest_table, params=None):
    if table_is_empty(conn, dest_schema, dest_table):
        print(f"Table is empty. Performing full load...")
        last_modified = None
    else:
        last_modified = get_last_modified(conn, dest_schema, dest_table)
        print(f"Last modified timestamp: {last_modified}")
    
    payload = fetch_md_data(source_endpoint, params=params) 
    current = payload.get("current")
    new_data = [current] if current else []

    if last_modified is not None:
        new_data = [row for row in new_data
                    if pd.to_datetime(row["time"], unit ="s", utc=True).tz_convert(VN_TZ) >= last_modified
                    ]
    return new_data, last_modified

def upsert(conn, new_data, schema, table, keys):
    metadata = MetaData()
    table = Table(table, metadata, schema=schema, autoload_with=conn)
    
    if new_data.empty:
        print("No new data to upsert.")
        return
    full_table_name = f"{schema}.{table}"
    
    stmt = insert(table).values(new_data.to_dict(orient="records"))
    update_dict = {col: stmt.excluded[col] for col in new_data.columns if col not in keys}
    upsert_stmt = stmt.on_conflict_do_update(index_elements=keys, set_=update_dict)
    conn.execute(upsert_stmt)
    print(f"Upserted {len(new_data)} rows into {full_table_name}.")
    
            
def incremental_load(source_endpoint, dest_table, dest_schema, key_column, metadata=None, params=None, execution_time=None):
    print(f"Start incremental loading...")
    keys = [key.strip() for key in key_column.split(",")]
    engine = establish_wh_connection()

    try:
        with engine.begin() as conn:
            if not check_if_table_exists(conn, dest_schema, dest_table):
                raise RuntimeError(f"Table {dest_schema}.{dest_table} does not exitst.")
            
            new_data, last_modified = fetch_new_data(conn, source_endpoint, dest_schema, dest_table, params)
            if not new_data:
                print("No new data to process.")
                return
    
            new_df = transform_data(new_data, execution_time)
            new_df["time"] = pd.to_datetime(new_df["time"], unit="s", utc=True).dt.tz_convert(VN_TZ)
            # Chỉ giữ các cột có trong bảng
            table_columns = get_existing_columns(conn, dest_schema, dest_table)
            for col in new_df.columns:
                if col not in table_columns:
                    print(f"Warning column: '{col}' does not exitst in destionation table and will be ignored.")
            new_df = new_df[[col for col in new_df.columns if col in table_columns]]
            # ALTER TABLE raw.weather_hcmc
            # ADD COLUMN IF NOT EXISTS execution_time timestamptz;

            if metadata:
                new_df = add_metadata(new_df, metadata, dest_schema, dest_table, table_columns=table_columns)
                
            if last_modified is not None:
                upsert(conn, new_df, dest_schema, dest_table, keys)
            else:
                insert_df(new_df, conn, dest_schema, dest_table)
        print("Incremental load completed successfully.")
    finally:
        engine.dispose()