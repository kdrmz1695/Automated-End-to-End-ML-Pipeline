# Automated End-to-End Data Pipeline for Machine Learning (Finance)

This repository contains an automated, end-to-end data pipeline designed for time-series machine learning applications in finance.  
The project focuses on daily stock price forecasting using a fully automated workflow orchestrated with Apache Airflow and Docker.

The main objective is to automate the entire lifecycle from data ingestion to model training and prediction with minimal manual intervention.

---

## Project Overview

The pipeline is built around the following ideas:

- Automated data ingestion from external APIs
- Multi-source data collection with fallback logic
- Centralized storage using PostgreSQL
- Automated feature engineering for time-series data
- Scheduled model training and prediction
- Production-aware and lightweight machine learning design

The system runs continuously like an automated clock, where each component operates on a predefined schedule.

---

## Architecture

Pipeline flow:

1. Data ingestion from external market APIs (TwelveData, Marketstack)
2. Raw data storage in PostgreSQL
3. Data merging and cleaning
4. Feature engineering for time-series modeling
5. Weekly model training
6. Daily next-day prediction

---

## Technologies Used

- Python
- Apache Airflow
- Docker
- PostgreSQL
- Pandas / NumPy
- Scikit-learn
- TensorFlow / Keras

---

## Machine Learning Model

### LSTM (Long Short-Term Memory)

- Used as a sequential baseline model
- Captures short-term temporal dependencies using sliding windows
- Intentionally kept lightweight to ensure stable execution inside an automated pipeline

---

## Scheduling Strategy

### Daily Tasks
- Data ingestion
- Feature engineering
- Prediction

### Weekly Tasks
- Model training (LSTM)

This separation ensures that computationally heavy training jobs do not interfere with daily inference tasks.

---

## Repository Structure

.
├── dags/
│   ├── fetch_twelvedata_data.py
│   ├── fetch_marketstack_data.py
│   ├── merge_data.py
│   ├── feature_engineering.py
│   ├── train_lstm.py
│   └── predict_lstm.py
│
├── docker/
│   └── docker-compose.yml
│
├── models/
│   └── (generated automatically by Airflow)
│
└── README.md

---

## Key Design Decisions

- Airflow is used strictly for orchestration, not experimentation
- Models are trained with limited epochs for predictable runtime
- Trained models are treated as runtime artifacts and are not committed to GitHub
- Automation and reproducibility are prioritized over raw prediction accuracy

---

## Outputs

- Trained model artifacts (.h5, .pkl)
- Validation metrics (R², RMSE, MAE, MAPE)
- Daily next-day stock price predictions stored in PostgreSQL

---

## Future Improvements

- Automated model selection (AutoML)
- Model performance monitoring and drift detection
- CI/CD integration for DAG validation and deployment
- Advanced MLOps practices such as model versioning and rollback

  
---

## Disclaimer

This project is for educational and research purposes only.  
It is not intended for real-world trading or financial decision-making.
