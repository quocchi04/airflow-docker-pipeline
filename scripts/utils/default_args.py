from pendulum import datatime
from datetime import datetime, timedelta

dag_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["quocchi2708ax@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2023, 1, 1, tz="UTC"),
    "retries": 0,
    "max_active_runs": 1,
    "catchup": False,
    "is_paused_upon_creation": True,
    # "on_failure_callback": notify_google_chat_api_on_failure,
    "on_success_callback": None,
    "execution_timeout": timedelta(hours=2),
}

# Airflow Tags
# ── Frequency ─────────────────────────────────────────────────────────────────
HOURLY              = "hourly"
DAILY               = "daily"
WEEKLY              = "weekly"
MONTHLY             = "monthly"
ON_DEMAND           = "on_demand"