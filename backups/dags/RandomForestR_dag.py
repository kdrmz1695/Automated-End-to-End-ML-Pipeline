from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
import joblib

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

def train_rf_tuned():
    # Veritabanı bağlantısı
    connection = "postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database"
    engine = create_engine(connection)

    # Feature tablosunu oku
    df = pd.read_sql("SELECT * FROM feature_eng_big_ready_dag", con=engine)

    # Feature ve target
    features = ["open_close_n", "high_low_n", "av_3d_n", "return_1d_n", "volume_change_n", "volatility_3d_n"]
    X = df[features]
    y = df["Target"]

    # Eğitim ve test verisi
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    param_dist = {
    'n_estimators': [50, 75, 100],          # Daha az estimator
    'max_depth': [5, 7, 9],                 # Daha sığ ağaçlar
    'min_samples_split': [15, 20, 25],      # Bölünme için daha fazla örnek gerekir
    'min_samples_leaf': [10, 12, 15],       # Yapraklar için daha büyük örnek gereksinimi
    'bootstrap': [True]                     # Bootstrap açık kalsın
     }



    base_model = RandomForestRegressor(random_state=42)

    search = RandomizedSearchCV(
    estimator=RandomForestRegressor(random_state=42, n_jobs=-1),
    param_distributions=param_dist,
    n_iter=10,
    scoring='r2',
    cv=3,
    verbose=2,
    random_state=42,
    )


    search.fit(X_train, y_train)
    best_model = search.best_estimator_

    y_pred = best_model.predict(X_test)

    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"✅ Tuned RF R²: {r2:.4f}")
    print(f"✅ RMSE: {rmse:.4f}")
    print(f"✅ MAE : {mae:.4f}")
    print(f"🎯 En iyi parametreler: {search.best_params_}")

    # Modeli kaydet
    os.makedirs("/opt/airflow/models", exist_ok=True)
    joblib.dump(best_model, "/opt/airflow/models/rf_tuned_model.pkl")
    print("✅ Tuned RF modeli kaydedildi.")

# Airflow DAG
default_args = {
    "start_date": datetime(2025, 6, 16),
    "retries": 1
}

with DAG(
    dag_id="train_rf_tuned_model_dag",
    schedule_interval="0 13 * * 0",
    default_args=default_args,
    catchup=False
) as dag:
    tune_task = PythonOperator(
        task_id="train_rf_tuned_model",
        python_callable=train_rf_tuned
    )
