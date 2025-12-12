from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def run_feature_engineering():
    connection = "postgresql://postgres:kuzey1234@host.docker.internal:5432/finance_database"
    engine = create_engine(connection)
    
    df = pd.read_sql("SELECT * FROM stock_price_merged", con=engine)
    # Order by Date
    df.sort_values(by=["symbol", "date"], inplace=True)

    # Feature calculation
    df["Open_Close"] = df["open"] - df["close"]
    df["High_Low"] = df["high"] - df["low"]
    df["Av_3d"] = df.groupby("symbol")["close"].transform(lambda x: x.rolling(window=3).mean())
    df["Return_1d"] = df.groupby("symbol")["close"].transform(lambda x: x.diff())
    df["Volume_Change"] = df.groupby("symbol")["volume"].transform(lambda x: x.diff())
    df["Volatility_3d"] = df.groupby("symbol")["close"].transform(lambda x: x.rolling(window=3).std())

    #Normalization
    
    df["open_close_n"] = (df["Open_Close"] - df["Open_Close"].min())/(df["Open_Close"].max() - df["Open_Close"].min())
    df["high_low_n"] = (df["High_Low"] - df["High_Low"].min())/(df["High_Low"].max() - df["High_Low"].min())
    df["av_3d_n"] = (df["Av_3d"] - df["Av_3d"].min())/(df["Av_3d"].max() - df["Av_3d"].min())
    df["return_1d_n"] = (df["Return_1d"] - df["Return_1d"].min())/(df["Return_1d"].max() - df["Return_1d"].min())
    df["volume_change_n"] = (df["Volume_Change"] - df["Volume_Change"].min())/(df["Volume_Change"].max() - df["Volume_Change"].min())
    df["volatility_3d_n"] = (df["Volatility_3d"] - df["Volatility_3d"].min())/(df["Volatility_3d"].max() - df["Volatility_3d"].min())
    
    # Target
    df["Target"] = df.groupby("symbol")["close"].shift(-1)


    # drop null
    df.dropna(inplace=True)

    # write sql
    df_ready = df[["symbol", "date", "open_close_n", "high_low_n", "av_3d_n", "return_1d_n", "volume_change_n", "volatility_3d_n", "Target"]]
    df_ready.to_sql("feature_eng_big_ready_dag", con=engine, if_exists="replace", index=False)

default_args = {
    "start_date": datetime(2024, 5, 1),
    "retries": 1
}

with DAG(
    dag_id="feature_engineering_dag",
    schedule_interval="0 10 * * *",
    default_args=default_args,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="run_feature_engineering",
        python_callable=run_feature_engineering
    )
