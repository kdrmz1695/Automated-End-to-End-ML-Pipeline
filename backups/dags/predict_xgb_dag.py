from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import joblib
from sqlalchemy import create_engine
import os

def predict_model():
    # DB bağlantısı
    connection = "postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database"
    engine = create_engine(connection)

    # Feature tablosunu oku
    df = pd.read_sql("SELECT * FROM feature_eng_big_ready_dag ORDER BY date", con=engine)

    # Model inputları
    feature_cols = [
        "open_close_n", "high_low_n", "av_3d_n",
        "return_1d_n", "volume_change_n", "volatility_3d_n"
    ]

    # Modeli yükle
    model_path = "/opt/airflow/models/xgboost_model.pkl"
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model dosyası bulunamadı: {model_path}")
    model = joblib.load(model_path)

    # Her symbol için en güncel satırları al
    df_latest = df.sort_values("date").groupby("symbol").tail(1)

    for _, row in df_latest.iterrows():
        try:
            symbol = row["symbol"]
            latest_date = row["date"]
            features = row[feature_cols].values.reshape(1, -1)
            prediction = float(model.predict(features)[0])
            target_date = pd.to_datetime(latest_date) + pd.Timedelta(days=1)

            # SQL INSERT
            insert_query = """
                INSERT INTO prediction_results (model_name, prediction_date, target_date, symbol, predicted_close)
                VALUES (%s, %s, %s, %s, %s)
            """
            with engine.connect() as conn:
                conn.execute(insert_query, (
                    "xgboost",
                    datetime.today().date(),
                    target_date.date(),
                    symbol,
                    prediction
                ))

            print(f"✅ {symbol}: {target_date.date()} için tahmin = {prediction:.2f}")
        
        except Exception as e:
            print(f"❌ {symbol} için tahmin hatası: {e}")

# Airflow DAG ayarları
default_args = {
    "start_date": datetime(2024, 5, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="predict_xgb_model",
    schedule_interval="0 18 * * *",  # Her gün saat 18:00
    default_args=default_args,
    catchup=False,
    tags=["prediction"]
) as dag:

    predict_task = PythonOperator(
        task_id="predict_and_store",
        python_callable=predict_model,
        retries=0,
        retry_delay=timedelta(minutes=5)
    )
