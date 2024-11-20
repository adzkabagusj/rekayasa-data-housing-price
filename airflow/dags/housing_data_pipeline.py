from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'housing_data_pipeline',
    default_args=default_args,
    description='A pipeline to scrape and process housing data',
    schedule_interval=timedelta(minutes=30),  # Runs every 30 minutes
    start_date=days_ago(1),
    catchup=False
)

# Task 1: Scrape Data from Rumah123
def scrape_rumah123():
    """
    This task will scrape data from rumah123.com and store it in MongoDB.
    """
    import subprocess
    subprocess.run(["python", "/scripts/scraper/rumah123_scraper.py"])

scrape_task = PythonOperator(
    task_id='scrape_rumah123_data',
    python_callable=scrape_rumah123,
    dag=dag
)

# Task 2: Scrape OpenStreetMap API Data
def scrape_osm_data():
    """
    This task will call the OpenStreetMap API based on 'kecamatan'.
    """
    import subprocess
    subprocess.run(["python", "/scripts/api/fetch_facilities.py"])

osm_task = PythonOperator(
    task_id='scrape_osm_data',
    python_callable=scrape_osm_data,
    dag=dag
)

# Task 3: Clean all data using Python (no Spark)
def clean_all_data():
    """
    This task will clean and process all data using Python.
    """
    import subprocess
    subprocess.run(["python", "/processing/jobs/data_cleaning.py"])

clean_data_task = PythonOperator(
    task_id='clean_all_data',
    python_callable=clean_all_data,
    dag=dag
)

# Task Dependencies
scrape_task >> osm_task >> clean_data_task