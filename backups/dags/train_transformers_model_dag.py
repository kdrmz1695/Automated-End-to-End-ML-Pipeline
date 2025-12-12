from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, LayerNormalization, Dropout
from tensorflow.keras.layers import MultiHeadAttention, GlobalAveragePooling1D

# SQL bağlantısı
DB_URI = 'postgresql+psycopg2://postgres:kuzey1234@host.docker.internal:5432/finance_database'

# Feature ve target bilgileri
FEATURES = ["open_close_n", "high_low_n", "av_3d_n", "return_1d_n", "volume_change_n", "volatility_3d_n"]
TARGET = "Target"
SEQ_LEN = 5
TABLE_NAME = 'feature_eng_big_ready_dag'

def load_data():
    engine = create_engine(DB_URI)
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)
    df.sort_values(by=["symbol", "date"], inplace=True)

    X, y = [], []
    for symbol in df["symbol"].unique():
        df_sym = df[df["symbol"] == symbol]
        feature_data = df_sym[FEATURES].values
        target_data = df_sym[TARGET].values

        for i in range(len(feature_data) - SEQ_LEN):
            X.append(feature_data[i:i+SEQ_LEN])
            y.append(target_data[i + SEQ_LEN])

    return np.array(X), np.array(y)

def build_transformer_model(input_shape):
    inputs = Input(shape=input_shape)
    x = LayerNormalization(epsilon=1e-6)(inputs)
    x = MultiHeadAttention(num_heads=4, key_dim=input_shape[-1])(x, x)
    x = Dropout(0.1)(x)
    x = LayerNormalization(epsilon=1e-6)(x)
    x = GlobalAveragePooling1D()(x)
    x = Dense(64, activation='relu')(x)
    outputs = Dense(1)(x)

    model = Model(inputs, outputs)
    model.compile(optimizer='adam', loss='mse')
    return model

def train_transformer():
    X, y = load_data()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = build_transformer_model(X_train.shape[1:])
    model.fit(X_train, y_train, epochs=20, batch_size=32, validation_split=0.1, verbose=1)

    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    mae = mean_absolute_error(y_test, y_pred)

    print(f"\u2705 Transformer R² Score: {r2:.4f}")
    print(f"\u2705 Transformer RMSE: {rmse:.4f}")
    print(f"\u2705 Transformer MAE : {mae:.4f}")

    model.save("/opt/airflow/models/transformer_model.h5")

with DAG(
    dag_id="train_transformer_model_dag",
    start_date=datetime(2025, 6, 1),
    schedule_interval="0 15 * * 0",
    catchup=False,
) as dag:
    train_task = PythonOperator(
        task_id="train_transformer_model",
        python_callable=train_transformer
    )

    train_task
