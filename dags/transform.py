import os
import re
import ast
import pandas as pd
from html import unescape
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_data(execution_date):
    """Chuyển đổi dữ liệu việc làm từ file CSV thô thành dạng chuẩn hóa."""
    logger.info("Bắt đầu chuyển đổi dữ liệu cho ngày %s", execution_date)

    input_file = f"/opt/airflow/data/vietnamwork/{execution_date}/vietnamworks_jobs.csv"
    output_file = f"/opt/airflow/data/vietnamwork/{execution_date}/vietnamworks_jobs_transformed.csv"

    # 1️⃣ Kiểm tra file đầu vào
    if not os.path.exists(input_file):
        logger.error("❌ File đầu vào %s không tồn tại.", input_file)
        raise FileNotFoundError(f"Input file {input_file} does not exist")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df = pd.read_csv(input_file, encoding="utf-8-sig")
    logger.info("Đã đọc %d bản ghi từ %s", len(df), input_file)

    # 2️⃣ Xử lý ngày tháng
    def normalize_date(date_str):
        """Chuẩn hóa định dạng ngày tháng."""
        if pd.isna(date_str):
            return pd.NaT
        try:
            dt = pd.to_datetime(date_str)
            dt = dt.tz_localize(None)
            return dt
        except Exception as e:
            logger.warning("⚠ Lỗi khi xử lý ngày %s: %s", date_str, e)
            return pd.NaT

    for col in ['created_on', 'approved_on', 'expired_on']:
        if col in df.columns:
            df[col] = df[col].apply(normalize_date)

    # 3️⃣ Xử lý ký tự HTML trong mô tả công việc
    def clean_html(text):
        """Loại bỏ HTML tags và chuẩn hóa khoảng trắng."""
        if pd.isna(text):
            return text
        text = unescape(str(text))
        text = re.sub(r'<[^>]+>', '', text)
        return ' '.join(text.split())

    for col in ['job_description', 'job_requirement']:
        if col in df.columns:
            df[col] = df[col].apply(clean_html)

    # 4️⃣ Chuẩn hóa city_name và address
    city_mapping = {
        'hanoi': 'Ha Noi',
        'ha noi': 'Ha Noi',
        'ho chi minh': 'Ho Chi Minh',
        'hcm': 'Ho Chi Minh',
        'binh duong': 'Binh Duong',
        'dong nai': 'Dong Nai',
        'other': 'Other'
    }

    def fill_and_normalize(row):
        """Chuẩn hóa tên thành phố và địa chỉ."""
        city = row.get('city_name')
        addr = row.get('address')

        if pd.isna(city):
            if pd.notna(addr) and addr.lower() in city_mapping:
                city = city_mapping[addr.lower()]
            else:
                city = 'Unknown'
        else:
            city = city_mapping.get(city.lower(), city)

        if pd.isna(addr):
            addr = city

        return pd.Series([city, addr], index=['city_name', 'address'])

    if 'city_name' in df.columns and 'address' in df.columns:
        df[['city_name', 'address']] = df.apply(fill_and_normalize, axis=1)

    # 5️⃣ Chuẩn hóa chữ thường
    def normalize_case(text):
        """Chuyển đổi văn bản thành chữ thường và chuẩn hóa khoảng trắng."""
        if pd.isna(text):
            return text
        return ' '.join(str(text).lower().split())

    for col in ['job_title', 'company_name', 'city_name', 'job_description', 'job_requirement', 'address']:
        if col in df.columns:
            df[col] = df[col].apply(normalize_case)

    # 6️⃣ Xử lý cột skill_name (list)
    if 'skill_name' in df.columns:
        df['skill_name'] = df['skill_name'].apply(
            lambda x: ast.literal_eval(x) if pd.notna(x) else []
        )

    # 7️⃣ Ghi ra file CSV
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info("🎉 Hoàn tất transform! Đã lưu %d bản ghi vào %s", len(df), output_file)
