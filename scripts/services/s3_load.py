import re
from io import StringIO

import pandas as pd
from sqlalchemy import text
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from scripts.utils.db_utils import establish_wh_connection, get_existing_columns

VN_TZ = "Asia/Ho_Chi_Minh"
EXPECTED_FILES_PER_DAY = 24
SNAPSHOT_PATTERN = re.compile(r"snapshot-(\d{8}T\d{6}Z)\.csv$")

def build_prefix(base_prefix, collected_date):
    return f"{base_prefix}/{collected_date}={collected_date}/"

def parse_snapshot_ts(s3_key):
    match = SNAPSHOT_PATTERN.search(s3_key)
    if not match:
        raise ValueError(f"Unexpected file name: {s3_key}")
    return pd.to_datetime(match.group(1), format="%Y%m%dT%H%M%SZ", utc=True)

# Liệt kê file snapshot trong 1 partition
def list_partition_files(bucket, prefix, aws_conn_id="aws_default"):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    keys = hook.list_keys(bucket_name = bucket, prefix=prefix) or []
    files = sorted(k for k in keys if SNAPSHOT_PATTERN.search(k))
    print(f"Found {len(files)} snapshot files in s3://{bucket}/{prefix}")
    
    if not files:
        raise FileNotFoundError(f"No snapshot files in s3://{bucket}/{prefix}")
    if len(files) < EXPECTED_FILES_PER_DAY:
        print(f"Warning: expected {EXPECTED_FILES_PER_DAY} files, found {len(files)}")
    return files

# Đọc toàn bộ file của partition thành 1 df
def read_partition(bucket, keys, collected_date, execution_time, aws_conn_id="aws_default"):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    frames = []
    for key in keys:
        df = pd.read_csv(StringIO(hook.read_key(key=key, bucket_name=bucket)))
        df.columns = df.columns.str.lower()
        df["snapshot_ts"] = parse_snapshot_ts(key)
        df["source_file"] = key
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df["collected_date"] = pd.Timestamp(collected_date).date()
    df["execution_time"] = pd.Timestamp(execution_time)
    return df

# Xóa partition cũ và ghi lại trong 1 transaction
def load_partition_to_raw(bucket, keys, collected_date, dest_schema, dest_table, log_table, execution_time, aws_conn_id="aws_default"):
    df = read_partition(bucket, keys, collected_date, execution_time, aws_conn_id)
    engine = establish_wh_connection()
    try:
        with engine.begin() as conn:
            table_columns = get_existing_columns(conn, dest_schema, dest_table)
            if not table_columns:
                raise RuntimeError(f"Table {dest_schema}.{dest_table} does not exist.")
            ignored = set(df.columns) - set(table_columns)
            if ignored:
                print(f"Warning: columns {ignored} not in {dest_schema}.{dest_table} and will be ignored.")
            df = df[[col for col in df.columns if col in table_columns]]

            log_df = (
                df.groupby("source_file").size()
                .reset_index(name="row_count")
                .rename(columns={"source_file": "s3_key"})
            )
            log_df["collected_date"] = df["collected_date"].iloc[0]
            log_df["execution_time"] = df["execution_time"].iloc[0]

            for table in (dest_table, log_table):
                conn.execute(
                    text(f"DELETE FROM {dest_schema}.{table} WHERE collected_date = :d"),
                    {"d": collected_date},
                )
            df.to_sql(dest_table, con=conn, schema=dest_schema, if_exists="append",
                      index=False, method="multi", chunksize=1000)
            log_df.to_sql(log_table, con=conn, schema=dest_schema, if_exists="append", index=False)
        print(f"Loaded {len(df)} rows from {len(keys)} files into {dest_schema}.{dest_table} for {collected_date}")
        return len(df)
    finally:
        engine.dispose()

