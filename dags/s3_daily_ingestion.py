from datetime import timedelta
import json
from pathlib import Path

import pendulum
from airflow.sdk import Asset, Param, dag, task
from scripts.utils.default_args import dag_args, DAILY

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

config_path = (
    Path(__file__).resolve().parents[1]
    / "scripts/configs/s3_daily_ingestion.json"
)

with config_path.open(encoding="utf-8") as f:
    s3_ingestion_config = json.load(f)

for config in s3_ingestion_config:
    if not config["enabled"]:
        continue

    @dag(
        default_args={**dag_args, "retry_delay": timedelta(minutes=15)},
        dag_id=config["dag_id"],
        schedule=config["schedule"],
        start_date=pendulum.parse(config["start_date"], tz=VN_TZ),
        catchup=True,
        max_active_runs=1,
        params={"collected_date": Param(None, type=["null", "string"], format="date")},
        tags=[DAILY, "s3"],
    )
    def s3_daily_ingestion():
        @task
        def resolve_partition(cfg, **context):
            from scripts.services.s3_load import build_prefix

            manual_date = context["params"].get("collected_date")
            logical_date = context.get("logical_date")
            if manual_date:
                collected_date = pendulum.parse(manual_date).date()
            elif logical_date is not None:
                # 06:50 giờ VN = 23:50 UTC hôm trước -> đúng ngày UTC vừa đủ 24 file
                collected_date = logical_date.in_timezone("UTC").date()
            else:
                collected_date = pendulum.now("UTC").subtract(days=1).date()

            prefix = build_prefix(cfg["s3_prefix"], collected_date.isoformat())
            print(f"Collected date: {collected_date}, prefix: {prefix}")
            return {"collected_date": collected_date.isoformat(), "prefix": prefix}

        @task
        def list_files(partition, cfg):
            from scripts.services.s3_load import list_partition_files
            return list_partition_files(cfg["bucket"], partition["prefix"], cfg["aws_conn_id"])

        @task(outlets=[Asset(name=f"dwh.{config['dest_schema']}.{config['dest_table']}")])
        def load_partition(partition, keys, cfg):
            from scripts.services.s3_load import load_partition_to_raw
            load_partition_to_raw(
                bucket=cfg["bucket"],
                keys=keys,
                collected_date=partition["collected_date"],
                dest_schema=cfg["dest_schema"],
                dest_table=cfg["dest_table"],
                log_table=cfg["log_table"],
                aws_conn_id=cfg["aws_conn_id"],
            )

        partition = resolve_partition(config)
        keys = list_files(partition, config)
        load_partition(partition, keys, config)

    s3_daily_ingestion()
