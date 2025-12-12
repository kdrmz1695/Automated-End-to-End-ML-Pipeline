from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import SimpleRNN, Dense
from tensorflow.keras.models import load_model

# Veri hazırlık fonksiyonu
def prepare_data_for_rnn(df, sequence_length=5):
    features = ["open_close_n", "high_low_n", "av_3d_n", "return_1d_n", "volume_change_n", "volatility_3d_n"]
    X, y = [], []

    for symbol in df["symbol"].unique():
        df_sym = df[df["symbol"] == symbol].sort_values("date")
        data = df_sym[features].values
        targets = df_sym["Target"].values

        for i in range(len(data) - sequence_length):
            X.append(data[i:i+sequence_length])
            y.append(targets[i + sequence_length])

    return np.stack(X), np.array(y)

# RNN modeli kurma fonksiyonu

def build_rnn_model(input_shape):
    model = Sequential()
    model.add(SimpleRNN(64, activation='tanh', input_shape=input_shape))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')
    return model


# Ana eğitim fonksiyonu
def train_rnn_model():
    connection = "postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database"
    engine = create_engine(connection)

    df = pd.read_sql("SELECT * FROM feature_eng_big_ready_dag", con=engine)
    X, y = prepare_data_for_rnn(df)

    split_index = int(len(X) * 0.8)
    X_train, X_test = X[:split_index], X[split_index:]
    y_train, y_test = y[:split_index], y[split_index:]


    model = build_rnn_model(input_shape=X_train.shape[1:])
    model.fit(
    X_train,
    y_train,
    validation_data=(X_test, y_test),
    shuffle=False,
    epochs=20,
    batch_size=16,
    verbose=1
)

    # Modeli kaydet
    os.makedirs("/opt/airflow/models", exist_ok=True)
    model.save("/opt/airflow/models/rnn_model.keras")

    # Performans ölçümü
    y_pred = model.predict(X_test).flatten()
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    
    from sklearn.metrics import r2_score

    r2 = r2_score(y_test, y_pred)
    print(f"✅ R² Score: {r2:.4f}")

    print(f"✅ RNN RMSE: {rmse:.4f}")
    print(f"✅ RNN MAE : {mae:.4f}")

# Airflow DAG tanımı
default_args = {
    "start_date": datetime(2024, 5, 1),
    "retries": 1
}

with DAG(
    dag_id="train_rnn_model_dag",
    schedule_interval="0 19 * * 0",
    default_args=default_args,
    catchup=False
) as dag:
    rnn_train_task = PythonOperator(
        task_id="train_rnn_model",
        python_callable=train_rnn_model
    )
