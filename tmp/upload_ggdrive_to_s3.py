import os
import shutil
import gdown
import boto3
from dotenv import load_dotenv

# Load các biến từ file .env trong project
load_dotenv()

FOLDER_ID = "1DfQtRJ9IWW05TegnXf6o4xKes190DbUu"
TEMP_DIR = "./temp_drive_data"
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# 1. Khởi tạo kết nối AWS S3
s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

def migrate():
    # 2. Tải toàn bộ folder từ Google Drive về thư mục tạm
    print("Đang tải dữ liệu từ Google Drive...")
    os.makedirs(TEMP_DIR, exist_ok=True)
    url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
    gdown.download_folder(url=url, output=TEMP_DIR, quiet=False, use_cookies=False)

    # 3. Đẩy toàn bộ file vừa tải lên AWS S3
    print("\nĐang đẩy dữ liệu lên AWS S3...")
    count = 0
    for root, dirs, files in os.walk(TEMP_DIR):
        for file in files:
            local_path = os.path.join(root, file)
            rel_path = os.path.relpath(local_path, TEMP_DIR).replace("\\", "/")
            s3_key = f"raw/coingecko/{rel_path}"

            s3.upload_file(local_path, BUCKET_NAME, s3_key)
            count += 1
            print(f"[{count}] Uploaded: s3://{BUCKET_NAME}/{s3_key}")

    # 4. Dọn dẹp thư mục tạm trên máy
    shutil.rmtree(TEMP_DIR)
    print(f"\nHoàn tất! Đã chuyển thành công {count} file lên S3 và dọn sạch file tạm.")

if __name__ == "__main__":
    migrate()