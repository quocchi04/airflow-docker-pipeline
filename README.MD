cấu trúc repo
my-data-project/
├── docker-compose.yml       ← orchestrate toàn bộ service
├── Dockerfile                ← build image Airflow có thêm requests/pandas/psycopg2
├── requirements.txt
├── .env                       ← biến môi trường (KHÔNG commit lên Git thật)
├── postgres-init/init-dwh.sql ← chạy 1 lần lúc Postgres khởi tạo, tạo database dwh
└── dags/daily_ingestion.py    ← chính là "công việc 1"

Chạy thử
bash
cd my-data-project
mkdir -p logs plugins
docker compose build
docker compose up airflow-init
docker compose up -d