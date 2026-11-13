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
import numpy as np

def table_is_empty(conn, schema, table):
    query = f"SELECT COUNT(*) FROM {schema}.{table}"
    result = conn.execute(query)
    return result.scalar()[0] == 0

def check_if_table_exists(conn, schema: str, table: str) -> bool:
    query = text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name = :table
        )
        """)
    
def establich_wh_connection():
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
        erroer_msg = f"Failed to establish Data Warehouse connection - Database: {wh_db}. Error: {e}"
        logger.error(error_msg)
        raise Exception(error_msg)

def add_metadata(data: pd.DataFrame, metadata: list) -> pd.DataFrame:
    for meta in metadata:
        data[meta["name"]] = meta["value"]
    return data

def truncate_table(df: pd.DataFrame, conn, schema: str, table: str):
    try:
        df.to_sql(
            table,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
        )
        logger.info(f"Successfully insert a chunk of {len(df)} rows into {schema}.{table}.")
    except Exception as e:
        logger.error(f"Failed to insert data into {schema}.{table}: {e}")
        raise
    
def get_existing_columns(conn, schema, table):
    query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
         AND table_name = '{table}'
        ORDER BY ordinal_position;
    """)
    existing_cols_raw = conn.execute(text(query)).fetchall()
    existing_col = [col[0] for col in existing_cols_raw] 
    return existing_col