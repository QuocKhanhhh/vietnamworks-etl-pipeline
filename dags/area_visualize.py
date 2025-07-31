import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import re

df = pd.read_csv("E:/docker-projects/airflow-project/data/vietnamwork/vietnamworks_jobs_transformed.csv", encoding="utf-8-sig")


# Danh sách quận/huyện và thành phố
locations = {
    'Ha Noi': [
        'Quận Ba Đình', 'Quận Hoàn Kiếm', 'Quận Đống Đa', 'Quận Hai Bà Trưng',
        'Quận Hoàng Mai', 'Quận Thanh Xuân', 'Quận Long Biên', 'Quận Nam Từ Liêm',
        'Quận Bắc Từ Liêm', 'Quận Tây Hồ', 'Quận Cầu Giấy', 'Quận Hà Đông',
        'Huyện Đông Anh', 'Huyện Gia Lâm', 'Huyện Thanh Trì',
        'District Ba Đình', 'District Hoàn Kiếm', 'District Đống Đa'
    ],
    'Ho Chi Minh': [
        'Quận 1', 'Quận 3', 'Quận 4', 'Quận 5', 'Quận 6', 'Quận 7', 'Quận 8',
        'Quận 10', 'Quận 11', 'Quận 12', 'Quận Bình Thạnh', 'Quận Gò Vấp',
        'Quận Phú Nhuận', 'Quận Tân Bình', 'Quận Tân Phú', 'Quận Thủ Đức',
        'Huyện Bình Chánh', 'Huyện Cần Giờ', 'Huyện Củ Chi',
        'District 1', 'District 3', 'District 7'
    ],
    'Binh Duong': [
        'Thành phố Thủ Dầu Một', 'Thành phố Dĩ An', 'Thành phố Thuận An',
        'Huyện Bắc Tân Uyên', 'Huyện Bàu Bàng', 'Huyện Dầu Tiếng'
    ],
    'Dong Nai': [
        'Thành phố Biên Hòa', 'Thành phố Long Khánh', 'Huyện Cẩm Mỹ',
        'Huyện Định Quán', 'Huyện Long Thành'
    ]
}

# Danh sách ánh xạ thành phố
city_mapping = {
    'hanoi': 'Ha Noi',
    'ha noi': 'Ha Noi',
    'ho chi minh': 'Ho Chi Minh',
    'hcm': 'Ho Chi Minh',
    'binh duong': 'Binh Duong',
    'dong nai': 'Dong Nai',
    'other': 'Other'
}

# Hàm trích xuất khu vực
def extract_area(row):
    address = row['address'] if pd.notna(row['address']) else ''
    city_name = row['city_name'] if pd.notna(row['city_name']) else ''

    address_lower = address.lower()

    # Tìm quận/huyện trong address
    for city, districts in locations.items():
        for district in districts:
            if district.lower() in address_lower:
                return district

    # Nếu không tìm thấy quận/huyện, dùng city_name hoặc tìm thành phố trong address
    if city_name.lower() in city_mapping:
        return city_mapping[city_name.lower()]
    for city_key, city_value in city_mapping.items():
        if city_key in address_lower:
            return city_value

    return 'Unknown'

# Tạo cột area
df['area'] = df.apply(extract_area, axis=1)

# Phân tích số lượng công việc theo khu vực
area_counts = df['area'].value_counts().head(10)  # Lấy top 10 khu vực

# Phân tích số lượng công việc theo thành phố
city_counts = df['city_name'].value_counts()

# In kết quả phân tích
print("Số lượng công việc theo khu vực (Top 10):")
print(area_counts)
print("\nSố lượng công việc theo thành phố:")
print(city_counts)

# Thiết lập style cho Seaborn
sns.set_style("whitegrid")

# Vẽ biểu đồ cột: Số lượng công việc theo khu vực
plt.figure(figsize=(12, 6))
sns.barplot(x=area_counts.values, y=area_counts.index, palette='viridis')
plt.title('Top 10 khu vực có nhiều công việc nhất', fontsize=14, pad=20)
plt.xlabel('Số lượng công việc', fontsize=12)
plt.ylabel('Khu vực', fontsize=12)
plt.tight_layout()
plt.savefig('area_job_counts.png', dpi=300, bbox_inches='tight')
plt.show()

# Vẽ biểu đồ tròn: Tỷ lệ công việc theo thành phố
plt.figure(figsize=(8, 8))
plt.pie(city_counts, labels=city_counts.index, autopct='%1.1f%%', colors=sns.color_palette('viridis', len(city_counts)))
plt.title('Tỷ lệ công việc theo thành phố', fontsize=14)
plt.tight_layout()
plt.savefig('city_job_pie.png', dpi=300, bbox_inches='tight')
plt.show()