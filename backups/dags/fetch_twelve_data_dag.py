from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import time
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_store_twelve_data():
    api_key = "03d20d58848a45f0aeac330ddfa7a6e1"
    start_date = "2024-04-01"
    end_date = datetime.today().strftime("%Y-%m-%d")

    # ✅ Sembol listesi dosyadan okunuyor
    with open("/opt/airflow/dags/stock_list_twelve.txt", "r") as f:
        symbols = [line.strip() for line in f if line.strip()]

    engine = create_engine("postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database")

    for i, symbol in enumerate(symbols):
        print(f"\n📥 Veri çekiliyor: {symbol}")
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": symbol,
            "interval": "1day",
            "start_date": start_date,
            "end_date": end_date,
            "apikey": api_key,
            "outputsize": 5000,
            "format": "JSON"
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if "values" not in data:
                print(f"⚠️ {symbol} için veri alınamadı: {data}")
                continue

            df = pd.DataFrame(data["values"])
            df["symbol"] = symbol
            df["source"] = "twelve"
            df["date"] = pd.to_datetime(df["datetime"])
            df = df.rename(columns=str.lower)
            df = df[["date", "symbol", "open", "high", "low", "close", "volume", "source"]]

            df.to_sql("raw_price_twelve", con=engine, if_exists="append", index=False)
            print(f"✅ {symbol} → {len(df)} kayıt yazıldı.")
        except Exception as e:
            print(f"❌ {symbol} → HATA: {e}")

        # ✅ API limitine takılmamak için her 7 sembolde bir bekle
        if (i + 1) % 7 == 0:
            print("⏳ 65 saniye bekleniyor (API limiti koruması)...")
            time.sleep(65)

    print("\n🎉 Twelve Data → Tüm semboller tamamlandı.")

with DAG(
    dag_id="get_finance_twelve_dag",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval=None,
    catchup=False,
    tags=["twelve", "raw"]
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_twelve_data",
        python_callable=fetch_and_store_twelve_data
    )
