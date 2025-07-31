from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import WebScraper
from transform import transform_data
from skill_visualize import get_top_skills

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],  # Thay bằng email của bạn
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=3),
}

def run_extract(**context):
    """Task extract dữ liệu từ VietnamWorks"""
    execution_date = context['ds']  # Ngày chạy dạng YYYY-MM-DD
    scraper = WebScraper(headless=True)
    scraper.extract_jobs(
        output_dir='/opt/airflow/data/vietnamwork',
        execution_date=execution_date
    )

def run_transform(**context):
    """Task transform dữ liệu"""
    execution_date = context['ds']
    transform_data(execution_date)

def run_visualize(**context):
    """Task visualize kỹ năng"""
    execution_date = context['ds']
    get_top_skills(execution_date)

# Định nghĩa DAG
with DAG(
    'vietnamworks_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract, transform, and visualize job data for Data Engineer roles from VietnamWorks',
    schedule_interval=None,  # Chạy thủ công để thử nghiệm
    start_date=datetime(2025, 7, 1),
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=run_extract,
        provide_context=True,  # để hàm nhận được **context
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=run_transform,
        provide_context=True,
    )

    visualize_task = PythonOperator(
        task_id='visualize_skills',
        python_callable=run_visualize,
        provide_context=True,
    )

    # Thứ tự chạy
    extract_task >> transform_task >> visualize_task
