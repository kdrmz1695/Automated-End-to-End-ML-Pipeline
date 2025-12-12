from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import time
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_store_finnhub_data():
    api_key = "d1gful9r01qmqatv6pkgd1gful9r01qmqatv6pl0"
    start_date = int(datetime(2024, 4, 1).timestamp())
    end_date = int(datetime.today().timestamp())

    with open("/opt/airflow/dags/stock_list.txt", "r") as f:
        symbols = [line.strip() for line in f if line.strip()]

    engine = create_engine("postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database")

    for i, symbol in enumerate(symbols):
        print(f"\n📥 Veri çekiliyor: {symbol}")
        url = f"https://finnhub.io/api/v1/stock/candle"
        params = {
            "symbol": symbol,
            "resolution": "D",
            "from": start_date,
            "to": end_date,
            "token": api_key
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if data.get("s") != "ok":
                print(f"⚠️ {symbol} için veri alınamadı: {data}")
                continue

            df = pd.DataFrame({
                "date": pd.to_datetime(data["t"], unit="s"),
                "symbol": symbol,
                "open": data["o"],
                "high": data["h"],
                "low": data["l"],
                "close": data["c"],
                "volume": data["v"],
                "source": "finnhub"
            })

            df.to_sql("raw_price_finnhub", con=engine, if_exists="append", index=False)
            print(f"✅ {symbol} → {len(df)} kayıt yazıldı.")
        except Exception as e:
            print(f"❌ {symbol} → HATA: {e}")

        # 🔒 Güvenli olması için her 20 sembolde bir sleep ekleyelim
        if (i + 1) % 20 == 0:
            print("⏳ 65 saniye bekleniyor (API limiti koruması)...")
            time.sleep(65)

    print("\n🎉 Finnhub → Tüm semboller tamamlandı.")

with DAG(
    dag_id="fetch_finnhub_data_dag",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval=None,
    catchup=False,
    tags=["finnhub", "raw"]
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_finnhub_data",
        python_callable=fetch_and_store_finnhub_data
    )

    fetch_data
