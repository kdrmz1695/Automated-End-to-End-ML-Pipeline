from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

# --- 1. VERİ HAZIRLAMA ---
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

# --- 2. MODEL KUR ---
def build_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(64, activation='tanh', input_shape=input_shape))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')
    return model

# --- 3. EĞİTİM FUNCTİON ---
def train_lstm_model():
    engine = create_engine("postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database")
    df = pd.read_sql("SELECT * FROM feature_eng_big_ready_dag", con=engine)

    X, y = prepare_data_for_rnn(df)
    split_index = int(len(X) * 0.8)
    X_train, X_test = X[:split_index], X[split_index:]
    y_train, y_test = y[:split_index], y[split_index:]

    model = build_lstm_model(input_shape=X_train.shape[1:])
    model.fit(
        X_train,
        y_train,
        validation_data=(X_test, y_test),
        shuffle=False,
        epochs=20,
        batch_size=16,
        verbose=1
    )

    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    mae = mean_absolute_error(y_test, y_pred)

    print("✅ LSTM R² Score:", round(r2, 4))
    print("✅ LSTM RMSE:", round(rmse, 4))
    print("✅ LSTM MAE :", round(mae, 4))

    model.save("/opt/airflow/models/model_lstm.h5")

# --- 4. DAG TANIMI ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='train_lstm_model_dag',
    default_args=default_args,
    schedule_interval="0 17 * * 0",
    catchup=False,
    tags=['model', 'lstm']
) as dag:

    train_model_task = PythonOperator(
        task_id='train_lstm_model',
        python_callable=train_lstm_model
    )

    train_model_task
