from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau

from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error


FEATURE_COLS = [
    "open_close_n", "high_low_n", "av_3d_n",
    "return_1d_n", "volume_change_n", "volatility_3d_n"
]
TARGET_COL = "Target"
SEQ_LEN = 5  # for short momentum. 
VAL_RATIO = 0.20 # 

def build_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(64, activation="tanh", input_shape=input_shape))
    model.add(Dense(1))
    model.compile(optimizer="adam", loss="mse")
    return model

def make_sequences_by_symbol(df, seq_len=SEQ_LEN):
    
    out = {}
    for sym, g in df.groupby("symbol", sort=False):
        g = g.sort_values("date")
        data = g[FEATURE_COLS].values
        targets = g[TARGET_COL].values
        if len(data) <= seq_len:
            continue
        X, y = [], []
        for i in range(len(data) - seq_len):
            X.append(data[i:i+seq_len])
            y.append(targets[i+seq_len])
        out[sym] = (np.asarray(X, dtype=np.float32), np.asarray(y, dtype=np.float32))
    return out

def chrono_split_sequences(seqs_dict, val_ratio=VAL_RATIO):
    X_tr, y_tr, X_val, y_val = [], [], [], []
    for sym, (X, y) in seqs_dict.items():
        n = len(X)
        if n == 0:
            continue
        cut = int(n * (1 - val_ratio))
        cut = min(max(cut, 1), n-1)  
        X_tr.append(X[:cut]); y_tr.append(y[:cut])
        X_val.append(X[cut:]); y_val.append(y[cut:])
    if not X_tr:
        return None, None, None, None
    X_tr = np.concatenate(X_tr, axis=0)
    y_tr = np.concatenate(y_tr, axis=0)
    X_val = np.concatenate(X_val, axis=0)
    y_val = np.concatenate(y_val, axis=0)
    return X_tr, y_tr, X_val, y_val

def train_lstm_model():
    np.random.seed(42)
    tf.random.set_seed(42)

    # DB connection 
    engine = create_engine("postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database")

    # Pull only what we need
    sql = """
      SELECT symbol, date,
             open_close_n, high_low_n, av_3d_n, return_1d_n, volume_change_n, volatility_3d_n,
             "Target"
      FROM feature_eng_big_ready_dag
    """
    df = pd.read_sql(sql, con=engine)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"]).dropna(subset=FEATURE_COLS + [TARGET_COL]).reset_index(drop=True)

    # Build sequences per symbol, then chronological split per symbol
    seqs = make_sequences_by_symbol(df, seq_len=SEQ_LEN)
    X_train, y_train, X_val, y_val = chrono_split_sequences(seqs, val_ratio=VAL_RATIO)

    # Note: if validation ended up empty, then go a global chronological 80/20 split
    if X_train is None:
        data = df[FEATURE_COLS].values
        tgt  = df[TARGET_COL].values
        X = np.asarray([data[i:i+SEQ_LEN] for i in range(len(data)-SEQ_LEN)], dtype=np.float32)
        y = np.asarray([tgt[i+SEQ_LEN] for i in range(len(tgt)-SEQ_LEN)], dtype=np.float32)
        cut = int(len(X) * 0.8)
        X_train, y_train = X[:cut], y[:cut]
        X_val,   y_val   = X[cut:], y[cut:]

    # Build & train
    model = build_lstm_model(input_shape=X_train.shape[1:])

    callbacks = [
        EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True),
        ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=5, verbose=1)
    ]

    history = model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=10,
        batch_size=128,
        shuffle=False,               
        callbacks=callbacks,
        verbose=1
    )

    
    y_pred = model.predict(X_val, verbose=0).ravel()
    y_true = y_val.ravel()

    r2   = float(r2_score(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae  = float(mean_absolute_error(y_true, y_pred))
    # safe MAPE (%)
    mask = np.abs(y_true) > 1e-8
    mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100.0) if mask.sum() > 0 else float("nan")

    print(f"LSTM R²: {r2:.6f}")
    print(f"LSTM RMSE: {rmse:.6f}")
    print(f"LSTM MAE: {mae:.6f}")
    print(f"LSTM MAPE: {mape:.2f}%")

    # Persist model
    os.makedirs("/opt/airflow/models", exist_ok=True)
    model.save("/opt/airflow/models/model_lstm.h5")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="LSTM_v2.1",
    default_args=default_args,
    schedule_interval="0 17 * * 0",   # Sundays 17:00
    catchup=False,
    tags=["model", "lstm_v2.1"]
) as dag:
    train_model_task = PythonOperator(
        task_id="lstm_v2.1",
        python_callable=train_lstm_model
    )
