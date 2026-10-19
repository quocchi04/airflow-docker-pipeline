"""
Công việc 1 - Daily ingestion: lấy dữ liệu nguồn và nạp vào schema raw.

Nguồn: Open-Meteo (API thời tiết công khai, không cần API key) cho TP.HCM.
Đích: bảng raw.weather_hcmc trong database "dwh".
"""
from __future__ import annotations

import os

import pandas as pd
import pendulum
import requests
from sqlalchemy import create_engine

from airflow.sdk import dag, task

HCMC_LATITUDE = 10.8231
HCMC_LONGITUDE = 106.6297


def _get_dwh_engine():
    user = os.environ["DWH_USER"]
    password = os.environ["DWH_PASSWORD"]
    host = os.environ["DWH_HOST"]
    port = os.environ["DWH_PORT"]
    db = os.environ["DWH_DB"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


@dag(
    dag_id="daily_ingestion",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["job1", "personal"],
)
def daily_ingestion():

    @task
    def fetch_weather() -> dict:
        """Lấy dữ liệu nguồn: gọi API Open-Meteo."""
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": HCMC_LATITUDE,
                "longitude": HCMC_LONGITUDE,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
                "timezone": "Asia/Ho_Chi_Minh",
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    @task
    def load_to_raw(payload: dict) -> None:
        """Nạp vào raw: ghi thẳng vào schema raw của DWH bằng pandas to_sql()."""
        current = payload["current"]
        df = pd.DataFrame([{
            "fetched_at": current["time"],
            "temperature_c": current["temperature_2m"],
            "humidity_pct": current["relative_humidity_2m"],
            "wind_speed_kmh": current["wind_speed_10m"],
        }])

        engine = _get_dwh_engine()
        df.to_sql(
            "weather_hcmc",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
        )

    load_to_raw(fetch_weather())


daily_ingestion()