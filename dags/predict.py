from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from tensorflow.keras.models import load_model


FEATURE_COLS = [
    "open_close_n", "high_low_n", "av_3d_n",
    "return_1d_n", "volume_change_n", "volatility_3d_n"
]
SEQ_LEN = 5

MODEL_PATH = "/opt/airflow/models/model_lstm.h5"

DB_URL = "postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database"


PRED_TABLE = "prediction_results_lstm"


def predict_next_day():
    # 1) Model var mı?
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f" Couldn't find model: {MODEL_PATH}. Firstly, DAG must execute.")

    model = load_model(MODEL_PATH)

    engine = create_engine(DB_URL)

    # 2) If prediction table does not exist
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {PRED_TABLE} (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        last_observed_date DATE NOT NULL,
        prediction_date DATE NOT NULL,
        y_pred DOUBLE PRECISION NOT NULL,
        model_name TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 3) Fetc data from feature table
    sql = """
      SELECT symbol, date,
             open_close_n, high_low_n, av_3d_n,
             return_1d_n, volume_change_n, volatility_3d_n
      FROM feature_eng_big_ready_dag
    """
    df = pd.read_sql(sql, con=engine)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["symbol", "date"] + FEATURE_COLS)
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    
    rows_to_insert = []

    for sym, g in df.groupby("symbol", sort=False):
        g = g.sort_values("date")

        if len(g) < SEQ_LEN:
            continue

        last_block = g.tail(SEQ_LEN)
        X_last = last_block[FEATURE_COLS].values.astype(np.float32)

        
        X_last = X_last.reshape(1, SEQ_LEN, len(FEATURE_COLS))

        y_pred = float(model.predict(X_last, verbose=0).ravel()[0])

        last_date = last_block["date"].iloc[-1].date()
        pred_date = last_date + timedelta(days=1)  

        rows_to_insert.append({
            "symbol": sym,
            "last_observed_date": last_date,
            "prediction_date": pred_date,
            "y_pred": y_pred,
            "model_name": "LSTM_v2.1"
        })

    if not rows_to_insert:
        raise ValueError("Hiçbir symbol için tahmin üretilemedi. Veri/feature tablosunu kontrol et.")

    
    out_df = pd.DataFrame(rows_to_insert)
    out_df.to_sql(PRED_TABLE, con=engine, if_exists="append", index=False)

    print(f"Inserted {len(out_df)} predictions into {PRED_TABLE}.")
    print(out_df.head(10).to_string(index=False))


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="LSTM_predict_v1",
    default_args=default_args,
    schedule_interval="0 18 * * *",  
    catchup=False,
    tags=["predict", "lstm"],
) as dag:
    predict_task = PythonOperator(
        task_id="predict_lstm_next_day",
        python_callable=predict_next_day
    )
