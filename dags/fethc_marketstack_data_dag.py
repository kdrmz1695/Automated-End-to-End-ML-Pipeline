from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import time

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def fetch_marketstack_data():
    api_key = "7ee36bb30962a55e16d9f970a70cd249"
    engine = create_engine("postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database")

    with open("/opt/airflow/dags/stock_list.txt", "r") as f:
        symbols = [line.strip() for line in f if line.strip()]

    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=365)

    for symbol in symbols:
        url = f"http://api.marketstack.com/v1/eod"
        params = {
            "access_key": api_key,
            "symbols": symbol,
            "date_from": start_date.strftime("%Y-%m-%d"),
            "date_to": end_date.strftime("%Y-%m-%d"),
            "limit": 1000
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if "data" not in data or not data["data"]:
                continue

            df = pd.DataFrame(data["data"])
            df["symbol"] = symbol
            df["source"] = "marketstack"
            df["date"] = pd.to_datetime(df["date"]).dt.date

            df = df.rename(columns={
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume"
            })

            df = df[["date", "symbol", "open", "high", "low", "close", "volume", "source"]]
            df.to_sql("raw_price_marketstack", con=engine, if_exists="append", index=False)

        except Exception as e:
            print(f"{symbol} ERROR: {e}")

        time.sleep(2.5)

with DAG(
    dag_id='fetch_marketstack_data_dag',
    default_args=default_args,
    description='Daily Marketstack Data',
    start_date=datetime(2024, 1, 1),
    schedule_interval='15 9 * * *', 
    catchup=False,
    tags=['marketstack', 'fetch', 'daily']
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_marketstack_data',
        python_callable=fetch_marketstack_data
    )
