FROM apache/airflow:2.7.2-python3.10

USER root
RUN apt-get update && apt-get install -y tzdata

USER airflow

RUN pip install --upgrade pip && pip install --no-cache-dir \
    numpy==1.24.3 \
    pandas==1.5.3 \
    scikit-learn==1.2.2 \
    matplotlib==3.7.1 \
    sqlalchemy==1.4.49 \
    psycopg2-binary==2.9.9 \
    joblib==1.2.0 \
    requests-cache



RUN pip install --no-cache-dir tensorflow==2.12.0