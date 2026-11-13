from asyncio.log import logger
from email.mime import text
import json
import os
from pathlib import Path
from turtle import st
from venv import logger
import pandas as pd
import pendulum
import requests
from sqlalchemy import URL, create_engine
from utils.ingestion import get_ingestion
from datetime import date, datetime, timedelta
import numpy as np

def table_is_empty(conn, schema, table):
    query = f"SELECT COUNT(*) FROM {schema}.{table}"
    result = conn.execute(query)
    return result.scalar()[0] == 0

def check_if_table_exists(conn, schema: str, table: str) -> bool:
    query = text("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name = :table
        )
        """)
    result = conn.execute(query, {"schema": schema, "table": table}).scalar()
    return bool(result)

# -------Kết nối tới dwh---------------------    
def establish_wh_connection():
    wh_host = os.inviron("DWH_HOST")
    wh_port = os.environ("DWH_PORT")
    wh_user = os.environ("DWH_USER")
    wh_password = os.environ("DWH_PASSWORD")
    wh_db = os.environ("DWH_DB")
    try:
        engine_wh = create_engine(
            f"postgresql+psycopg2://{wh_user}:{wh_password}@{wh_host}:{wh_port}/{wh_db}"
        )
        logger.info("Established Data Warehouse connection - Database: {wh_db}")
        return engine_wh
    except Exception as e:
        error_msg = f"Failed to establish Data Warehouse connection - Database: {wh_db}. Error: {e}"
        logger.error(error_msg)
        raise Exception(error_msg)

def add_metadata(data: pd.DataFrame, metadata: list[dict],dest_schema:str,dest_table:str,table_columns: list[str] = [],auto_adjust: bool = False) -> pd.DataFrame:
    wh_engine = None
    table_exists = True
    if table_columns is None or len(table_columns) == 0:
        wh_engine = establish_wh_connection()
        wh_conn = wh_engine.connect()
        table_columns = get_existing_columns(wh_conn, dest_schema, dest_table)
    bc_conn = get_ingestion()
    for meta in metadata:
        if meta not in table_columns:
            if auto_adjust:
                if table_exists == False:
                    pass
                elif table_columns == [] and table_exists:
                    print(f"Table {dest_schema}.{dest_table} does not exists, create new table with new metadata column...")
                    table_exists = False
                elif table_columns != [] and table_exists: 
                    print(f"Metadata column {meta} not found in destination table {dest_schema}.{dest_table}, adding new column...")
                    if wh_engine is None:
                        wh_engine = establish_wh_connection()
                    with wh_engine.begin() as wh_conn:
                        query = ""
                        if meta == "last_refresh_at":    
                            query += f"""ALTER TABLE {dest_schema}.{dest_table}
                                        ADD COLUMN last_refresh_at timestamp with time zone;"""
                        if meta == "company_name":
                            query += f"""ALTER TABLE {dest_schema}.{dest_table}
                                        ADD COLUMN company_name varchar(256);"""
                        if meta == "company_id": 
                            query += f"""ALTER TABLE {dest_schema}.{dest_table}
                                        ADD COLUMN company_id varchar(256);"""
                        if query != "":
                            wh_conn.execute(text(query))
            else:  
                print(f"Metadata column {meta} not found in destination table {dest_schema}.{dest_table}, skipping...")
                continue
        if meta == "last_refresh_at":
            data[meta] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if meta == "company_name":
            data[meta] = bc_conn["company_name"]
        if meta == "company_id":
            data[meta] = bc_conn["company_id"]
        # Add other metadata handling as needed
    
    return data

def drop_table(conn, schema: str, table: str):
    conn.execute(text(f'DROP TABLE IF EXISTS {schema}."{table}" CASCADE'))
    logger.info(f"Dropped table {table}")

def truncate_table(conn, schema: str, table: str):
    logger.info(f'TRUNCATE TABLE {schema}.{table}')
    #Thực thi query
    conn.execute(text(f'TRUNCATE TABLE {schema}.{table}')) 
    logger.info(f"Truncated table {table}")
    
def get_existing_columns(conn, schema, table):
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
         AND table_name = '{table}'
        ORDER BY ordinal_position;
    """
    existing_cols_raw = conn.execute(text(query)).fetchall()
    existing_col = [col[0] for col in existing_cols_raw] 
    return existing_col