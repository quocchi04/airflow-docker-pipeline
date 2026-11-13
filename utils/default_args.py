from utils.notifier_callbacks import notify_slack_on_failure, notify_team_on_failure,notify_google_chat_api_on_failure
from pendulum import datetime
from datetime import timedelta

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
    "on_failure_callback": notify_google_chat_api_on_failure,
    "on_success_callback": None,
    "execution_timeout": timedelta(hours=2),
}

# Airflow Tags
# ── Sources ──────────────────────────────────────────────────────────────────
AMAZON_SP           = "amazon_sp"
AMAZON_ADS          = "amazon_ads"
META_ADS            = "meta_ads"
GOOGLE_ADS          = "google_ads"
GOOGLE_PROFILE      = "google_profile"
BING_ADS            = "bing_ads"
PINTEREST           = "pinterest"
WALMART_SELLER      = "walmart_seller"
WALMART_ADS         = "walmart_ads"
HOMEDEPOT           = "homedepot"
LOWES               = "lowes"
WAYFAIR             = "wayfair"
MAGENTO             = "magento"
BC                  = "business_central"
WMS                 = "wms"
ERP                 = "erp"
LN                  = "ln"
SHIPWORKS           = "shipworks"
ZENDESK             = "zendesk"
AFTERSHIP           = "aftership"
SELLERCLOUD         = "sellercloud"
ADOBE               = "adobe"
QUICKBOOK           = "quickbook"
GOOGLE_DRIVE        = "google_drive"
GOOGLE_SHEET        = "google_sheet"
MS_DRIVE            = "ms_drive"
RAKUTEN             = "rakuten"

# ── Pipeline Stage ────────────────────────────────────────────────────────────
INGEST_FULL         = "ingest_full"
INGEST_DELTA        = "ingest_delta"
INGEST_CRAWL        = "ingest_crawl"
TRANSFORM           = "transform"
EXPORT              = "export"
ML                  = "ml"
MONITOR             = "monitor"
BACKUP              = "backup"

# ── Frequency ─────────────────────────────────────────────────────────────────
HOURLY              = "hourly"
DAILY               = "daily"
WEEKLY              = "weekly"
MONTHLY             = "monthly"
ON_DEMAND           = "on_demand"

# ── Department ────────────────────────────────────────────────────────────────
DEPT_MARKETING      = "dept:marketing"
DEPT_COMMERCE       = "dept:commerce"
DEPT_OPERATIONS     = "dept:operations"
DEPT_FINANCE        = "dept:finance"
DEPT_ANALYTICS      = "dept:analytics"