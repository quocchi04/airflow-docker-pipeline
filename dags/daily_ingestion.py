from datetime import datetime
import json
import os
from pathlib import Path

import pandas as pd
import pendulum
import requests
from sqlalchemy import URL, create_engine

from airflow.decorators import dag, task

with open('script/api_daily_ingestion.json', 'r') as f:
    daly_ingestion_config = json.load(f)

for config in daly_ingestion_config:
    if not config["enabled"]:
        continue
    dag_id = config["dest_table"]

    @dag(
        dag_id=dag_id,
        schedule=config["schedule"],
        catchup=False,
        tags=["job1", "personal"]
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
        def load_data(dt):
            from scripts.services.load_data import insert_data, incremental_load, full_load
            if dt["mode"] == "incremental":
                incremental_load(
                    dt["source_endpoint"],
                    dt["dest_schema"],
                    dt["dest_table"],
                    dt["key_column"],
                    dt["mode"],
                    metadata = dt.get("metadata", [])
            )
            engine = _get_dwh_engine()
            df.to_sql(
                config["dest_table"],
                engine,
                schema=config["dest_schema"],
                if_exists="append",
                index=False,
            )

        load_to_raw(fetch_weather())

    def _load_config() -> dict:
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        configs = json.load(config_file)

    if not isinstance(configs, list):
        raise ValueError(f"{CONFIG_PATH} must contain a JSON array")

    matching_configs = [
        config
        for config in configs
        if isinstance(config, dict) and config.get("model") == DAG_ID
    ]
    if len(matching_configs) != 1:
        raise ValueError(
            f"Expected exactly one config with model={DAG_ID!r} in {CONFIG_PATH}, "
            f"found {len(matching_configs)}"
        )

    config = matching_configs[0]
    required_keys = {"api", "dest_schema", "dest_table", "schedule", "params"}
    missing_keys = required_keys - config.keys()
    if missing_keys:
        raise ValueError(f"Missing config keys in {CONFIG_PATH}: {sorted(missing_keys)}")
    if not isinstance(config["params"], dict):
        raise ValueError(f"'params' in {CONFIG_PATH} must be a JSON object")
    return config


CONFIG = _load_config()


def _get_dwh_engine():
    user = os.environ["DWH_USER"]
    password = os.environ["DWH_PASSWORD"]
    host = os.environ["DWH_HOST"]
    port = os.environ["DWH_PORT"]
    db = os.environ["DWH_DB"]
    url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=db,
    )
    return create_engine(url)


def daily_ingestion():

    @task
    def fetch_weather() -> dict:
        """Lấy dữ liệu nguồn: gọi API Open-Meteo."""
        response = requests.get(
            CONFIG["api"],
            params=CONFIG["params"],
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    @task
    def load_to_raw(payload: dict) -> None:
        """Nạp vào raw: ghi thẳng vào schema raw của DWH bằng pandas to_sql()."""
        current = payload["current"]
        df = pd.DataFrame(
            [
                {
                    "fetched_at": current["time"],
                    "temperature_c": current["temperature_2m"],
                    "humidity_pct": current["relative_humidity_2m"],
                    "wind_speed_kmh": current["wind_speed_10m"],
                }
            ]
        )

        engine = _get_dwh_engine()
        df.to_sql(
            CONFIG["dest_table"],
            engine,
            schema=CONFIG["dest_schema"],
            if_exists="append",
            index=False,
        )

    load_to_raw(fetch_weather())


daily_ingestion()