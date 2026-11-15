from datetime import datetime
import json
import pandas as pd
import requests
from scripts.utils.default_args import dag_args, DAILY
from pathlib import Path

from airflow.decorators import dag, task

config_path = (
    Path(__file__).resolve().parents[1]
    / "scripts/configs/api_daily_ingestion.json"
)

with config_path.open(encoding="utf-8") as f:
    daly_ingestion_config = json.load(f)

for config in daly_ingestion_config:
    if not config["enabled"]:
        continue
    dag_id = config["dag_id"]

    @dag(
        dag_id=dag_id,
        schedule=config["schedule"],
        catchup=False,
        tags=[DAILY]
    )
    def daily_ingestion():
        @task
        def init_context():
            execution_date = datetime.utcnow()
            business_date = execution_date.date()
            return {
                "execution_date": execution_date,
                "business_date": business_date,
            }

        @task
        def load_data(ctx, cfg):
            from scripts.services.load_data import incremental_load, full_load
            if cfg["mode"] == "incremental":
                incremental_load(
                    cfg["source_endpoint"],
                    cfg["dest_schema"],
                    cfg["dest_table"],
                    cfg["key_column"],
                    cfg["mode"],
                    metadata = cfg.get("metadata", [])
            )
            elif cfg["mode"] == "full_load":
                full_load(
                    source_endpoint=cfg["api"],
                    dest_table=cfg["dest_table"],
                    dest_schema=cfg["dest_schema"],
                    isTablebased=cfg.get("isTablebased", False),
                    metadata=cfg.get("metadata", []),
                    params=cfg.get("params"),
                )
        context = init_context()
        load_data(context, config)
        
    daily_ingestion()