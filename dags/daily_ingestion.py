from datetime import timedelta
import json
from scripts.utils.default_args import dag_args, DAILY
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

config_path = (
    Path(__file__).resolve().parents[1]
    / "scripts/configs/api_daily_ingestion.json"
)

with config_path.open(encoding="utf-8") as f:
    daily_ingestion_config = json.load(f)

for config in daily_ingestion_config:
    if not config["enabled"]:
        continue
    dag_id = config["dag_id"]

    @dag(
        default_args=dag_args,
        dag_id=dag_id,
        schedule=config["schedule"],
        catchup=False,
        max_active_runs=1,
        tags=[DAILY]
    )
    def daily_ingestion():
        @task
        def init_context(**context):
            execution_time = pendulum.now(VN_TZ)
            business_date = context["logical_date"].in_timezone(VN_TZ).date()
            print(f"Run started at {execution_time} | business_date={business_date}")

            return {
                "execution_time": execution_time.isoformat(),
                "business_date": business_date.isoformat(),
            }

        @task
        def load_data(ctx, cfg):
            from scripts.services.load_data import incremental_load, full_load
            
            execution_time = ctx["execution_time"]
            if cfg["mode"] == "incremental":
                incremental_load(
                    source_endpoint=cfg["api"],
                    dest_schema=cfg["dest_schema"],
                    dest_table=cfg["dest_table"],
                    key_column=cfg["key_column"],
                    metadata = cfg.get("metadata", []),
                    params=cfg.get("params"),
                    execution_time=execution_time,
                )
            elif cfg["mode"] == "full_load":
                full_load(
                    source_endpoint=cfg["api"],
                    dest_table=cfg["dest_table"],
                    dest_schema=cfg["dest_schema"],
                    isTablebased=cfg.get("isTablebased", False),
                    metadata=cfg.get("metadata", []),
                    params=cfg.get("params"),
                    execution_time=execution_time,
                )
            else:
                raise ValueError(f"Unknown mode '{cfg['mode']}' for {cfg['dag_id']}")
        context = init_context()
        load_data(context, config)
        
    daily_ingestion()