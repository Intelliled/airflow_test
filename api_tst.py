from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from datetime import datetime

#test merge 6


def extract_data():
    """Извлечение данных из API"""
    print("Извлечение данных...")
    # Пример: получение данных из JSONPlaceholder API
    response = requests.get('https://api.genderize.io/?name=denisa')
    data = response.json()
    print(f"Получено {len(data)} записей")
    return data

def transform_data(**kwargs):
    """Трансформация данных"""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    
    print("Трансформация данных...")
    # Простая трансформация
    transformed_data = []
    transformed_data.append({
            
            'name': data['name'],
            'gender': data['gender']
            
        })
    
    print(f"Трансформировано {len(transformed_data)} записей")
    return transformed_data

def load_data(**kwargs):
    """Загрузка данных (сохранение в файл)"""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    
    print("Загрузка данных...")
    df = pd.DataFrame(data)
    filename = f"/opt/airflow/outputs/users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"Данные сохранены в {filename}")
    return filename

 
with DAG(
    'etl_pipeline',
    description='Пример ETL пайплайна',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    notify = BashOperator(
        task_id='notify',
        bash_command='echo "ETL процесс завершен успешно!"',
    )

    extract >> transform >> load >> notify   
