from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from fetch_data import fetch_youtube_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 27),
    'email': ['swatikrishnavlv@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'fetch_youtube_data_dag',
    default_args=default_args,
    description='Fetch trending data from youtube data api and save to csv.',
    schedule_interval=timedelta(days=1)
)

execute_process = PythonOperator(
    task_id='fetch_data_from_youtube_data_api',
    python_callable=fetch_youtube_data,
    op_kwargs={
        'country_codes': ['AE', 'AR', 'AU', 'BR', 'CA', 'CH', 'CL', 'CO', 'DE', 'ES', 'GR', 'HK', 'ID', 'IL', 'IQ',
                          'IS', 'IT', 'JM', 'JP', 'KR', 'MX', 'MY', 'NL', 'NZ', 'PK', 'RU', 'SA', 'SG', 'ZA', 'GB',
                          'US', 'IN',
                          'FR']
    },
    dag=dag,
)

execute_process
