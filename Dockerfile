FROM apache/airflow:2.7.2-python3.10

USER root
RUN apt-get update && apt-get install -y tzdata

USER airflow

RUN pip install --upgrade pip && pip install --no-cache-dir \
    numpy==1.24.3 \
    pandas==1.5.3 \
    scikit-learn==1.2.2 \
    matplotlib==3.7.1 \
    yfinance==0.2.33 \
    sqlalchemy==1.4.49 \
    psycopg2-binary==2.9.9 \
    joblib==1.2.0 \
    xgboost==1.7.6 \
    requests-cache


# 2. En sona TensorFlow (tek başına kurulsun ki çatışmasın)
RUN pip install --no-cache-dir tensorflow==2.12.0