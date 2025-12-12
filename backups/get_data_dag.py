from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

def get_data_api():    
    symbol_list = []
    with open("/opt/airflow/dags/stock_list.txt", "r") as f:
        for row in f:
            code = row.strip()
            if code:
                symbol_list.append(code)

    # PostgreSQL Connection 
    
    connection = connection = "postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database"
    engine = create_engine(connection)

    start_date = '2020-01-01'
    end_date = None

    for code in symbol_list:
        data = yf.download(tickers=code, start=start_date, end=end_date, interval='1d')

        if not data.empty:
            data.reset_index(inplace=True)

            # MultiIndex fix
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [col[0] for col in data.columns]

            data["symbol"] = code

            if "Adj Close" in data.columns:
                data.drop(columns=["Adj Close"], inplace=True)

            # Date + Symbol repeat check
            data.drop_duplicates(subset=["Date", "symbol"], inplace=True)

            # Write PostgreSQL
            data.to_sql("stock_price_dag", con=engine, if_exists="append", index=False)


default_args = {
    'start_date': datetime(2024, 4, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='get_finance_dag',
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['finance', 'pipeline'],
) as dag:

    get_data_api_task = PythonOperator(
        task_id='get_data_api',
        python_callable=get_data_api,
    )

    get_data_api_task
