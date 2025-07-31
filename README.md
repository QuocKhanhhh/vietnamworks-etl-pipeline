# VietnamWorks ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to scrape, process, and visualize job data for Data Engineer roles from VietnamWorks. The pipeline is containerized with Docker for easy deployment and reproducibility.

## Project Overview

The pipeline performs the following tasks:
1. **Extract**: Scrapes job listings from VietnamWorks using Selenium and API calls.
2. **Transform**: Cleans and standardizes the extracted data using Pandas.
3. **Visualize**: Generates visualizations of the top 10 skills for Data Engineer roles.

The project is orchestrated using Apache Airflow, with services running in Docker containers, including a PostgreSQL database for Airflow metadata.

## Prerequisites

- **Docker** and **Docker Compose** installed on your system.
- Python 3.10 or later (for local development outside Docker).
- Google Chrome (for Selenium WebDriver compatibility).
- Basic familiarity with Airflow, Docker, and Python.

## Project Structure

```plaintext
├── dags/
│   └── etl_dag.py              # Airflow DAG definition
├── data/
│   └── vietnamwork/            # Output directory for extracted and transformed data
├── logs/                       # Airflow logs
├── plugins/                    # Airflow plugins (if any)
├── Dockerfile                  # Docker image configuration for Airflow
├── docker-compose.yml          # Docker Compose configuration
├── requirements.txt            # Python dependencies
├── extract.py                 # Script for data extraction
├── transform.py               # Script for data transformation
├── skill_visualize.py         # Script for data visualization
└── README.md                  # This file
```

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Create a `.env` File**:
   Create a `.env` file in the project root with the following content:
   ```plaintext
   AIRFLOW_UID=50000
   ```

3. **Build and Run Docker Containers**:
   ```bash
   docker-compose up --build
   ```
   This command builds the Airflow image and starts the services (PostgreSQL, Airflow Webserver, and Scheduler).

4. **Access Airflow Web Interface**:
   Open your browser and navigate to `http://localhost:8080`. The default credentials are:
   - Username: `airflow`
   - Password: `airflow`

5. **Run the ETL Pipeline**:
   - In the Airflow web interface, locate the `vietnamworks_etl_pipeline` DAG.
   - Trigger the DAG manually by clicking the "Run" button.

6. **View Output**:
   - Extracted and transformed data will be saved in `data/vietnamwork/<execution_date>/`.
   - Visualizations (e.g., bar plots of top skills) will be saved as PNG files in the same directory.

## Configuration Details

- **Docker Compose** (`docker-compose.yml`):
  - Defines services for PostgreSQL, Airflow Webserver, and Scheduler.
  - Configures volumes for persistent data storage and mounts local directories for DAGs, logs, plugins, and data.
  - Sets the timezone to `Asia/Ho_Chi_Minh` for Vietnam.

- **Dockerfile**:
  - Extends the `apache/airflow:2.8.1-python3.10` image.
  - Installs Google Chrome and ChromeDriver for Selenium.
  - Installs Python dependencies from `requirements.txt`.
  - Creates and sets permissions for the data directory.

- **DAG** (`etl_dag.py`):
  - Defines an ETL pipeline with three tasks: `extract_data`, `transform_data`, and `visualize_skills`.
  - Uses PythonOperator to execute functions from `extract.py`, `transform.py`, and `skill_visualize.py`.
  - Configured to run manually (`schedule_interval=None`).

- **Extract** (`extract.py`):
  - Uses Selenium WebDriver and VietnamWorks API to scrape job listings for specified keywords (e.g., "Data Engineer").
  - Saves raw data as JSON and CSV files.

- **Transform** (`transform.py`):
  - Cleans and standardizes data (e.g., date normalization, HTML tag removal, city name standardization).
  - Outputs transformed data as a CSV file.

- **Visualize** (`skill_visualize.py`):
  - Generates a bar plot of the top 10 skills for Data Engineer roles using Seaborn and Matplotlib.
  - Saves the plot as a PNG file.

## Dependencies

Key Python packages (listed in `requirements.txt`):
- `apache-airflow==2.8.1`
- `selenium==4.8.0`
- `selenium-wire==5.1.0`
- `webdriver-manager==3.8.5`
- `pandas==2.0.3`
- `requests==2.28.2`
- `seaborn==0.12.2`
- `matplotlib==3.7.1`

## Notes

- The pipeline is set to run manually for testing purposes. To schedule it, modify the `schedule_interval` in `etl_dag.py`.
- Ensure sufficient disk space for the `data/vietnamwork` directory, as it stores raw and processed data.
- The visualization is currently configured for "Data Engineer" roles but can be modified for other keywords in `skill_visualize.py`.
- Logs are stored in the `logs/` directory and can be accessed via the Airflow web interface or directly from the file system.

## Troubleshooting

- **WebDriver Issues**: Ensure Chrome and ChromeDriver versions are compatible. The Dockerfile handles this automatically.
- **API Rate Limits**: If VietnamWorks API calls fail, check for rate-limiting issues or verify cookie handling in `extract.py`.
- **File Permissions**: Ensure the `airflow` user has write permissions for the `data/vietnamwork` directory.
- **Missing Data**: Verify that the input CSV file exists in the expected directory before transformation or visualization.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue on GitHub for suggestions or bug reports.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.