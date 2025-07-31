import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import ast
import os
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_top_skills(execution_date):
    """Tạo biểu đồ top 10 kỹ năng phổ biến cho ngành Data Engineer."""
    keyword = "Data Engineer"  # Chỉ sử dụng từ khóa thử nghiệm
    input_file = f"/opt/airflow/data/vietnamwork/{execution_date}/vietnamworks_jobs_transformed.csv"
    output_dir = f"/opt/airflow/data/vietnamwork/{execution_date}"

    # Kiểm tra file đầu vào
    if not os.path.exists(input_file):
        logger.error("File đầu vào %s không tồn tại.", input_file)
        raise FileNotFoundError(f"Input file {input_file} does not exist")

    # Tạo thư mục đầu ra
    os.makedirs(output_dir, exist_ok=True)
    
    # Đọc dữ liệu
    df = pd.read_csv(input_file, encoding="utf-8-sig")
    logger.info("Đã đọc %d bản ghi từ %s", len(df), input_file)

    # Lọc dữ liệu theo từ khóa
    df_keyword = df[df['keywords'] == keyword]

    # Chuyển skill_name từ chuỗi JSON thành danh sách
    df_keyword['skill_name'] = df_keyword['skill_name'].apply(
        lambda x: ast.literal_eval(x) if pd.notna(x) else []
    )

    # Tạo dictionary tần suất kỹ năng
    skills = df_keyword['skill_name'].explode().value_counts().head(10).to_dict()

    # Chuyển dictionary thành DataFrame để vẽ biểu đồ
    skills_df = pd.DataFrame(list(skills.items()), columns=['Skill', 'Count'])
    logger.info("Top 10 kỹ năng: %s", skills_df.to_dict())

    # Thiết lập style cho Seaborn
    sns.set_style("dark")

    # Vẽ biểu đồ cột
    plt.figure(figsize=(10, 6))
    sns.barplot(data=skills_df, x='Count', y='Skill', palette='viridis')

    # Tùy chỉnh biểu đồ
    plt.title(f"Top 10 Kỹ năng phổ biến ngành {keyword}", fontsize=14, pad=20)
    plt.xlabel('Số lần xuất hiện', fontsize=12)
    plt.ylabel('Kỹ năng', fontsize=12)
    plt.tight_layout()

    # Lưu biểu đồ
    output_path = f"{output_dir}/{keyword}_skills_barplot.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("Đã lưu biểu đồ vào %s", output_path)