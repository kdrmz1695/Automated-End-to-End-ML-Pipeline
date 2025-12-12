from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
import time

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_store_alpha_data():
    API_KEY = "ZW7CA1S4K6ZJ55QE"
    BASE_URL = "https://www.alphavantage.co/query"

    # Sembol listesini oku
    with open("/opt/airflow/dags/stock_list.txt", "r") as f:
        symbols = [line.strip() for line in f if line.strip()]

    engine = create_engine("postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database")
    metadata = MetaData()
    stock_price = Table("stock_price_alpha", metadata, autoload_with=engine)

    batch_size = 5
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        for symbol in batch:
            print(f"\n📥 Veri çekiliyor: {symbol}")
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "outputsize": "full",
                "apikey": API_KEY
            }

            response = requests.get(BASE_URL, params=params)
            if response.status_code != 200:
                print(f"❌ {symbol} → HTTP Hata: {response.status_code}")
                continue

            data = response.json()
            daily_data = data.get("Time Series (Daily)", {})
            if not daily_data:
                print(f"⚠️ {symbol} → Veri boş veya hatalı API dönüşü")
                continue

            rows = []
            for date_str, values in daily_data.items():
                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                    rows.append({
                        "symbol": symbol,
                        "date": date_obj,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"])
                    })
                except Exception as e:
                    print(f"⛔ {symbol} için satır hatası: {e}")
                    continue

            if not rows:
                print(f"⚠️ {symbol} → Satır bulunamadı, SQL'e yazılmayacak")
                continue

            stmt = insert(stock_price).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "symbol"],
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume
                }
            )

            try:
                with Session(engine) as session:
                    session.execute(stmt)
                    session.commit()
                print(f"✅ {symbol} → SQL'e yazıldı: {len(rows)} satır")
            except Exception as e:
                print(f"❌ {symbol} → SQL yazma hatası: {e}")

        print("⏳ 60 saniye bekleniyor (API limiti)...")
        time.sleep(60)

    print("\n🎉 Tüm semboller tamamlandı.")

with DAG(
    dag_id="get_finance_alpha_dag",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval="0 9 * * *",
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_alpha_data",
        python_callable=fetch_and_store_alpha_data
    )

    fetch_data
