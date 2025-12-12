from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def merge_data():
    conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        dbname="finance_database",
        user="postgres",
        password="kuzey1234"
    )
    cursor = conn.cursor()

    merge_sql = """
    CREATE TABLE IF NOT EXISTS public.stock_price_merged (
        date DATE,
        symbol TEXT,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume BIGINT,
        source TEXT
    );

    DELETE FROM public.stock_price_merged;

    INSERT INTO public.stock_price_merged (date, symbol, open, high, low, close, volume, source)
    SELECT date, symbol, open, high, low, close, volume, 'twelve' AS source
    FROM public.raw_price_twelve

    UNION ALL

    SELECT date, symbol, open, high, low, close, volume, 'marketstack' AS source
    FROM public.raw_price_marketstack
    WHERE (symbol, date) NOT IN (
        SELECT symbol, date FROM public.raw_price_twelve
    );
    """

    cursor.execute(merge_sql)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='merge_data_dag',
    default_args=default_args,
    schedule_interval='30 9 * * *',
    catchup=False,
    description='Merge twelve and marketstack data into one table',
) as dag:

    merge_task = PythonOperator(
        task_id='merge_stock_data',
        python_callable=merge_data
    )

    merge_task
