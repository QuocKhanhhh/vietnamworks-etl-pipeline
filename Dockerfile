FROM apache/airflow:2.8.1-python3.10

USER root

# 1️⃣ Cài đặt các gói hệ thống (root)
RUN apt-get update && \
    apt-get install -y wget curl unzip gnupg && \
    wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 2️⃣ Tạo thư mục dữ liệu và gán quyền cho user airflow
RUN mkdir -p /opt/airflow/data/vietnamwork && \
    chown -R airflow:root /opt/airflow/data && \
    chmod -R 775 /opt/airflow/data

# 3️⃣ Copy requirements
COPY requirements.txt /

# 4️⃣ Chuyển sang user airflow để cài Python package
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
