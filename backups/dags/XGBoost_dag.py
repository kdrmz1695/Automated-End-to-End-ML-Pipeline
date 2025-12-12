from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error
from xgboost import XGBRegressor
import joblib
import os
import numpy as np
import matplotlib.pyplot as plt

def train_model():
    connection = "postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database"
    engine = create_engine(connection)

    df = pd.read_sql("SELECT * FROM feature_eng_big_ready_dag", con=engine)

    X = df[["open_close_n", "high_low_n", "av_3d_n", "return_1d_n", "volume_change_n", "volatility_3d_n"]]
    y = df["Target"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = XGBRegressor(
        n_estimators=60,
        learning_rate=0.03,
        max_depth=2,
        reg_lambda=2.0,
        reg_alpha=1.0,
        early_stopping_rounds=10,
        random_state=42
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=True
    )

    y_pred = model.predict(X_test)

    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = model.score(X_test, y_test)

    print(f"✅ R² Score: {r2}")
    print(f"✅ RMSE: {rmse}")
    print(f"✅ MAE : {mae}")

    os.makedirs("/opt/airflow/models", exist_ok=True)
    joblib.dump(model, "/opt/airflow/models/xgboost_model.pkl")

    importances = model.feature_importances_
    features = X.columns

    plt.figure(figsize=(8, 5))
    plt.barh(features, importances)
    plt.title("Feature Importance (XGBoost)")
    plt.xlabel("Importance Score")
    plt.ylabel("Feature")
    plt.tight_layout()
    plt.savefig("/opt/airflow/models/feature_importance.png")
    print("✅ Feature importance grafiği kaydedildi.")

default_args = {
    "start_date": datetime(2024, 5, 1),
    "retries": 1
}

with DAG(
    dag_id="XGboost_model",
    schedule_interval="0 11 * * 0",
    default_args=default_args,
    catchup=False
) as dag:
    train_task = PythonOperator(
        task_id="train_xgboost_model",
        python_callable=train_model
    )
