# Data project

Project này chạy Apache Airflow và PostgreSQL bằng Docker Compose. Airflow lấy dữ
liệu thời tiết TP.HCM từ Open-Meteo mỗi ngày và ghi vào `dwh.raw.weather_hcmc`.

## Cấu hình WSL2 cho máy 8 GB RAM

Trên Windows, tạo file `%UserProfile%\.wslconfig` (ví dụ
`C:\Users\<tên-user>\.wslconfig`) với nội dung:

```ini
[wsl2]
memory=5GB
swap=2GB
```

Giới hạn WSL ở 5 GB để Windows còn khoảng 3 GB; swap 2 GB là vùng dự phòng,
không thay thế RAM. Cấu hình này áp dụng cho các distro WSL2. Sau khi lưu file,
mở PowerShell và chạy:

```powershell
wsl --shutdown
```

Mở lại WSL. Có thể kiểm tra giới hạn bên trong WSL bằng `free -h`.

## Chạy lần đầu

Cần cài Docker Desktop trên Windows và bật WSL Integration cho distro đang dùng.
Trong terminal WSL, xác nhận các lệnh sau chạy được:

```bash
docker --version
docker compose version
```

## Cấu hình kết nối DWH

Compose tự đọc các giá trị trong file `.env` ở thư mục dự án và truyền chúng
cho các dịch vụ Airflow; DAG lấy chúng bằng biến môi trường `DWH_HOST`,
`DWH_PORT`, `DWH_USER`, `DWH_PASSWORD` và `DWH_DB`. File `.env` đã được
`.gitignore` loại trừ.

Với PostgreSQL chạy trong Compose, dùng `DWH_HOST=postgres` và `DWH_PORT=5432`.
Nếu kết nối tới DWH bên ngoài, thay các giá trị này bằng địa chỉ mà container
Airflow truy cập được; không dùng `localhost` trừ khi DWH thực sự chạy bên
trong chính container Airflow.

Sau khi sửa `.env`, xác nhận cấu hình. Nếu stack đã chạy, tạo lại các dịch vụ
Airflow để nhận giá trị mới:

```bash
docker compose config -q
docker compose up -d --force-recreate airflow-api-server airflow-scheduler \
  airflow-dag-processor airflow-triggerer
```

Nếu đang cấu hình lần đầu, tiếp tục các bước khởi chạy bên dưới; không cần chạy
lệnh tạo lại dịch vụ trước.

Sau đó tại thư mục dự án:

```bash
mkdir -p logs plugins
docker compose config
docker compose build
docker compose up airflow-init
docker compose up
```

`docker compose up` chạy foreground và giữ terminal hiện log. Để tắt toàn bộ
stack, nhấn **Ctrl+C** tại terminal đó; Compose sẽ dừng các container nhưng giữ
dữ liệu trong volume. Lần sau, chạy lại `docker compose up`. Nếu trước đó đã
khởi chạy bằng `docker compose up -d`, chạy `docker compose down` một lần để
dừng stack cũ rồi dùng `docker compose up` từ những lần sau.
Trong khi stack đang chạy, có thể kiểm tra trạng thái từ một terminal khác bằng
`docker compose ps`.

Compose giới hạn PostgreSQL ở 768 MB, Airflow API server và scheduler ở 768 MB
mỗi dịch vụ, DAG processor và triggerer ở 512 MB mỗi dịch vụ. Giới hạn RAM tối
đa cộng lại là khoảng 3.25 GiB (không tính daemon Docker/WSL); mỗi dịch vụ cũng
có giới hạn CPU. Airflow chỉ chạy tối đa hai task đồng thời, một task mỗi DAG,
và chỉ xử lý một DAG run tại một thời điểm.

## Kết nối DataGrip

PostgreSQL chạy trong container, còn dữ liệu được lưu bền vững trong Docker
volume `postgres-db-volume`. DataGrip chỉ là client để kết nối, xem và chạy SQL;
nó không phải nơi lưu database. Compose chỉ publish PostgreSQL trên máy local
(`127.0.0.1:5433`), không mở cổng này ra mạng ngoài.

Khi `docker compose up` đang chạy, trong DataGrip tạo data source **PostgreSQL**
và nhập:

| Trường | Giá trị |
| --- | --- |
| Host | `localhost` |
| Port | `5433` |
| User | `airflow` |
| Password | `airflow` |
| Database | `dwh` |

Chọn **Test Connection**; nếu DataGrip đề nghị tải PostgreSQL driver, chấp
nhận tải driver. Sau khi kết nối, mở database `dwh`, rồi schema `raw` và bảng
`weather_hcmc`. Nếu chưa thấy bảng, hãy trigger DAG `daily_ingestion` trên
Airflow trước; task nạp dữ liệu sẽ tạo bảng khi ghi dữ liệu lần đầu.

Không xóa volume nếu muốn giữ dữ liệu. `docker compose down` giữ volume;
`docker compose down -v` sẽ xóa database.

Mở <http://localhost:8080> và đăng nhập bằng `airflow` / `airflow` (mật khẩu
mặc định dành cho chạy thử trên máy cá nhân). Bật DAG `daily_ingestion` rồi
chọn **Trigger** để chạy thử ngay.

## Cấu hình DAG bằng JSON

DAG đọc cấu hình từ `scripts/api_daily_ingestion.json`. Docker Compose mount
thư mục này vào `/opt/airflow/scripts` ở chế độ chỉ đọc; sau khi sửa JSON,
Airflow sẽ nhận cấu hình mới khi parse lại DAG. `schedule: null` nghĩa là DAG
chỉ chạy khi trigger thủ công; đặt `"@daily"` nếu muốn lập lịch hàng ngày.
Các giá trị `dest_schema` và `dest_table` xác định schema và bảng đích trong
DWH.

Sau khi chỉnh sửa cấu hình, kiểm tra Compose và khởi động lại các dịch vụ Airflow:

```bash
docker compose config -q
docker compose up -d --force-recreate airflow-api-server airflow-scheduler \
  airflow-dag-processor airflow-triggerer
```

Xem log nếu cần:

```bash
docker compose logs -f airflow-scheduler airflow-dag-processor
```

Kiểm tra dữ liệu đã nạp:

```bash
docker compose exec postgres psql -U airflow -d dwh \
  -c "SELECT * FROM raw.weather_hcmc ORDER BY fetched_at DESC LIMIT 5;"
```

`docker compose down` dừng các dịch vụ nhưng giữ database. Không dùng
`docker compose down -v` nếu muốn giữ dữ liệu. PostgreSQL chỉ chạy script
`postgres-init/init-dwh.sql` khi volume dữ liệu được tạo lần đầu.
