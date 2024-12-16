import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

GITHUB_FILE_URL = "https://github.com/niduran/airflow/raw/main/SpaceNK_2.0.xlsx"

'''
database = os.getenv("POSTGRES_DB")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
'''

database='<your_database>',
user= '<your_user>',
password= '<your_password>',
host= '<your_host>'
postgres_url = f"postgresql://{user}:{password}@{host}:5432/{database}"

def connect_postgres():
    conn = psycopg2.connect(
        database=database,
        user= user,
        password= password,
        host= host,
        port=5432
    )
    conn.autocommit = True
    return conn
    
# Define the DAG
with DAG(
    'excel_to_postgres',
    default_args=default_args,
    description='ETL pipeline to extract, transform, and load Excel data into Postgres',
    schedule_interval=None,
    start_date=datetime(2024, 12, 16),
    catchup=False,
) as dag:

    def create_table(**kwargs):
            conn = connect_postgres()
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS store_report (
                    store_no varchar(10) primary key,
                    store_name varchar(50),
                    ty_units INT,
                    ly_units INT,
                    tw_sales FLOAT,
                    lw_sales FLOAT,
                    lw_var_percent FLOAT,
                    ly_sales FLOAT,
                    ly_var_percent FLOAT,
                    ytd_sales FLOAT,
                    lytd_sales FLOAT,
                    lytd_var_percent FLOAT
                );
                """)
            conn.commit()
            cur.close()

    def extract_data(**kwargs):
        df = pd.read_excel(GITHUB_FILE_URL, header=5, sheet_name="Last Week Report by Store", engine="openpyxl", skipfooter=1)
        serialized_data = df.to_dict(orient="records")
        kwargs['ti'].xcom_push(key="raw_data", value=serialized_data)

    def transform_data(**kwargs):
        raw_data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='raw_data')

        df = pd.DataFrame(raw_data)

        # Rename columns
        df.rename(columns={
            'Store No': 'store_no',
            'Store': 'store_name',
            'TY Units': 'ty_units',
            'LY Units': 'ly_units',
            'TW Sales': 'tw_sales',
            'LW Sales': 'lw_sales',
            'LW Var %': 'lw_var_percent',
            'LY Sales': 'ly_sales',
            'LY Var %': 'ly_var_percent',
            'YTD Sales': 'ytd_sales',
            'LYTD Sales': 'lytd_sales',
            'LYTD Var %': 'lytd_var_percent'
        }, inplace=True)

        transformed_data = df.to_dict(orient="records")

        kwargs['ti'].xcom_push(key="transformed_data", value=transformed_data)
    
    def load_to_postgres(**kwargs):
        data = kwargs['ti'].xcom_pull(task_ids='transform_data')
        df = pd.DataFrame(data)
        engine = create_engine(postgres_url)
        conn = engine.connect()
        df.to_sql('store_report', conn, if_exists='append', index=False)
    
    # Define the tasks
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )
    
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_postgres,
    )

    # Set the task dependencies
    create_table_task >> extract_data_task >> transform_data_task >> load_data_task