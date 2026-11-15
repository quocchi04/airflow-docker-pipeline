from datetime import timedelta

dag_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}

# Airflow Tags
# ── Frequency ─────────────────────────────────────────────────────────────────
HOURLY              = "hourly"
DAILY               = "daily"
WEEKLY              = "weekly"
MONTHLY             = "monthly"
ON_DEMAND           = "on_demand"