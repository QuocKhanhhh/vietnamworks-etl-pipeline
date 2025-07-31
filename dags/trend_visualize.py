import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Đọc file CSV
df = pd.read_csv("E:/docker-projects/airflow-project/data/vietnamwork/vietnamworks_jobs_transformed.csv", encoding="utf-8-sig")

# Chuyển đổi cột created_on và expired_on thành datetime
df['created_on'] = pd.to_datetime(df['created_on'], errors='coerce')
df['expired_on'] = pd.to_datetime(df['expired_on'], errors='coerce')

# Nhóm dữ liệu theo tháng
df['created_month'] = df['created_on'].dt.to_period('M')
df['expired_month'] = df['expired_on'].dt.to_period('M')

# Đếm số lượng công việc theo tháng
created_trend = df['created_month'].value_counts().sort_index().reset_index()
created_trend.columns = ['month', 'created_count']

expired_trend = df['expired_month'].value_counts().sort_index().reset_index()
expired_trend.columns = ['month', 'expired_count']

# Gộp dữ liệu để vẽ
trend_df = pd.merge(created_trend, expired_trend, on='month', how='outer').fillna(0)
trend_df['month'] = trend_df['month'].astype(str)  # Chuyển period thành chuỗi để vẽ

# Thiết lập style cho Seaborn
sns.set_style("whitegrid")

# Vẽ biểu đồ đường
plt.figure(figsize=(12, 6))
sns.lineplot(data=trend_df, x='month', y='created_count', label='Công việc mới (Created)', color='blue', marker='o')
sns.lineplot(data=trend_df, x='month', y='expired_count', label='Công việc hết hạn (Expired)', color='red', marker='o')

# Tùy chỉnh biểu đồ
plt.title('Xu hướng việc làm theo thời gian (Created vs Expired)', fontsize=14, pad=20)
plt.xlabel('Tháng/Năm', fontsize=12)
plt.ylabel('Số lượng công việc', fontsize=12)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()

# Lưu biểu đồ
plt.savefig('job_trend.png', dpi=300, bbox_inches='tight')

# Hiển thị biểu đồ
plt.show()