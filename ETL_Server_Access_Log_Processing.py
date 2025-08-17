#ETL_Server_Access_Log_Processing
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests

# Define the path for the input and output files
url ='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt'
in_path='/home/project/airflow/web-server-access-log.txt'
extracted_file = 'extracted-data.txt'
transformed_file = 'transformed.txt'
load_file='capitalized.txt'
output_file = 'data_for_analytics.csv'

def download():
    url
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad status
    with open(in_path, 'wb') as f:
        f.write(response.content)
    print(f"Downloaded file to {in_path}")
def read(in_path): 
    with open(in_path, 'r') as f:
        for line in f:
            fields = line.strip().split('#')
            timestamp = fields[0]
            latitude = float(fields[1])
            longitude = float(fields[2])
            visitorid = fields[3]
            accessed_from_mobile = fields[4] == 'True'
            browser_code = int(fields[5])
            print(timestamp, latitude, longitude, visitorid, accessed_from_mobile, browser_code)
def extract(out_path,ti):
    records=[]#Liste pour stocker les ...
    # Read the contents of the file into a string
    with open(out_path, 'r') as infile:
        for line in infile:
            fields = line.strip().split('#')
            if len(fields)  < 6:
                continue
            timestamp = fields[0]
            visitorid = fields[3] 
             # On écrit les deux champs séparés par une virgule (par exemple)
            records.append((timestamp, visitorid))
    ti.xcom_push(key='extracted_records', value=records)
    
def transform(ti):
    records = ti.xcom_pull(key='extracted_records', task_ids='extract')
    capitalized_records = []
    for timestamp, visitorid in records:
        capitalized_records.append((timestamp, visitorid.upper()))
    ti.xcom_push(key='capitalized_records', value=capitalized_records)
def load(ti):
    capitalized_records = ti.xcom_pull(key='capitalized_records', task_ids='transform')
    output_file = 'capitalized.txt'
    with open(output_file, 'w') as f:
        for timestamp, visitorid in capitalized_records:
            # Écrit chaque ligne avec les champs séparés par une virgule
            f.write(f"{timestamp},{visitorid}\n")
    print(f"Saved {len(capitalized_records)} records to {output_file}")
def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Your name',
    'start_date': days_ago(0),
    'email': ['mymail@.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Define the DAG
dag = DAG(
    'Acces-server-log',
    default_args=default_args,
    description='My second DAG',
    schedule_interval=timedelta(days=1),
)
# Define the task named execute_extract to call the `extract` function
execute_download = PythonOperator(
    task_id='download',
    python_callable=download,
    dag=dag,
)
# Define the task named execute_extract to call the `extract` function
execute_read = PythonOperator(
    task_id='read',
    python_callable=read,
    dag=dag,
)
# Define the task named execute_extract to call the `extract` function
execute_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    op_kwargs={'out_path': '/home/project/airflow/web-server-access-log.txt'},
    dag=dag,
)
# Define the task named execute_transform to call the `transform` function
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)
# Define the task named execute_load to call the `load` function
execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)
# Define the task named execute_load to call the `load` function
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)
# Task pipeline
execute_download >> execute_read >>   execute_extract >> execute_transform >> execute_load >> execute_check