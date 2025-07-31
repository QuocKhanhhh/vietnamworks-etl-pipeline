import os
import time
import json
import logging
import pandas as pd
import requests
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from datetime import datetime

# Cấu hình log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self, headless=True):
        self.keywords = ["Data Engineer", "Data Engineer Intern", "Data Engineer Fresher", "Data Analyst", "Data Analyst Intern", "Data Analyst Fresher", 
                         "Data Scientist", "Data Scientist Intern", "Data Scientist Fresher", "Machine Learning Engineer", "Machine Learning Intern", "Machine Learning Fresher", "AI Engineer", "AI Intern", "AI Fresher"]
        logger.info("Khởi tạo WebScraper...")

        chrome_options = Options()
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        seleniumwire_options = {
            "request_storage_base_dir": "/tmp/selenium-wire",
            "verify_ssl": True
        }

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options,
            seleniumwire_options=seleniumwire_options
        )
        self.all_jobs = []
        logger.info("✅ WebDriver đã khởi tạo thành công (headless=%s)", headless)

    def handle_cookie_popup(self):
        """Đóng popup cookie nếu có"""
        try:
            cookie_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Đồng ý') or contains(text(), 'Accept')]"))
            )
            cookie_button.click()
            logger.info("Đã đóng cookie banner.")
            time.sleep(1)
        except (TimeoutException, NoSuchElementException):
            logger.info("Không tìm thấy cookie banner, tiếp tục...")

    def search_jobs(self, keyword):
        """Truy cập trang VietnamWorks và tìm kiếm keyword"""
        logger.info("🔍 Đang tìm kiếm công việc với từ khóa: '%s'", keyword)
        try:
            self.driver.get("https://www.vietnamworks.com")
            self.handle_cookie_popup()

            search_box = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[placeholder="Tìm kiếm việc làm, công ty, kỹ năng"]'))
            )
            search_box.clear()
            search_box.send_keys(keyword)
            search_box.send_keys(Keys.ENTER)
            logger.info("Đã gửi từ khóa tìm kiếm thành công.")
            time.sleep(5)  # Chờ trang tải kết quả
            return True
        except Exception as e:
            logger.error("❌ Lỗi khi tìm kiếm: %s", str(e))
            return False

    def fetch_jobs_api(self, keyword):
        """Gọi API để lấy dữ liệu việc làm"""
        logger.info("📡 Đang gọi API để lấy dữ liệu cho từ khóa '%s'...", keyword)
        job_data = []
        payload = {
            "userId": 0,
            "query": keyword,
            "filter": [],
            "ranges": [],
            "order": [{"field": "relevant", "value": "asc"}],
            "hitsPerPage": 50,
            "page": 0,
            "retrieveFields": [
                "jobTitle", "jobUrl", "createdOn", "approvedOn", "expiredOn",
                "companyName", "jobDescription", "jobRequirement", "salary",
                "salaryMax", "salaryMin", "skills", "address", "workingLocations",
                "jobLevel", "salaryCurrency"
            ]
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        try:
            cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}
            response = requests.post(
                "https://ms.vietnamworks.com/job-search/v1.0/search",
                json=payload,
                headers=headers,
                cookies=cookies,
                timeout=20
            )
            response.raise_for_status()
            jobs = response.json().get("data", [])

            for job in jobs:
                skill_names = [s.get("skillName") for s in job.get("skills", [])] if job.get("skills") else []
                job_entry = {
                    "job_title": job.get("jobTitle"),
                    "job_url": job.get("jobUrl"),
                    "created_on": job.get("createdOn"),
                    "approved_on": job.get("approvedOn"),
                    "expired_on": job.get("expiredOn"),
                    "company_name": job.get("companyName"),
                    "job_description": job.get("jobDescription"),
                    "job_requirement": job.get("jobRequirement"),
                    "salary": job.get("salary"),
                    "salary_max": job.get("salaryMax"),
                    "salary_min": job.get("salaryMin"),
                    "skill_name": skill_names,
                    "address": job.get("address"),
                    "city_name": job.get("workingLocations", [{}])[0].get("cityName") if job.get("workingLocations") else None,
                    "job_level": job.get("jobLevel"),
                    "salary_currency": job.get("salaryCurrency"),
                    "keywords": keyword
                }
                job_data.append(job_entry)

            logger.info("✅ Đã lấy được %d job từ API cho keyword '%s'", len(job_data), keyword)

        except Exception as e:
            logger.error("❌ Lỗi khi gọi API: %s", str(e))

        return job_data

    def extract_jobs(self, output_dir, execution_date=None):
        """Hàm chính để extract job"""
        if execution_date is None:
            execution_date = datetime.now().strftime("%Y-%m-%d")

        output_path = os.path.join(output_dir, execution_date)
        os.makedirs(output_path, exist_ok=True)
        logger.info("Bắt đầu extract dữ liệu, output: %s", output_path)

        try:
            for keyword in self.keywords:
                if self.search_jobs(keyword):
                    job_data = self.fetch_jobs_api(keyword)
                    self.all_jobs.extend(job_data)
                else:
                    logger.warning("⚠ Không thể tìm kiếm với từ khóa '%s'", keyword)

            if self.all_jobs:
                json_path = f"{output_path}/vietnamworks_jobs.json"
                csv_path = f"{output_path}/vietnamworks_jobs.csv"

                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(self.all_jobs, f, ensure_ascii=False, indent=4)
                pd.DataFrame(self.all_jobs).to_csv(csv_path, index=False, encoding="utf-8-sig")

                logger.info("🎉 Hoàn tất! Đã lưu %d job vào:", len(self.all_jobs))
                logger.info("   - JSON: %s", json_path)
                logger.info("   - CSV : %s", csv_path)
            else:
                logger.error("❌ Không thu được dữ liệu job nào.")
                raise ValueError("No jobs extracted")

        finally:
            self.driver.quit()
            logger.info("Đã đóng WebDriver.")

if __name__ == "__main__":
    scraper = WebScraper(headless=True)
    scraper.extract_jobs(output_dir="/opt/airflow/data/vietnamwork")
