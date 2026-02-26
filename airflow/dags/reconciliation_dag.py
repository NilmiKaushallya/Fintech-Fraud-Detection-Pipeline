from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

def reconciliation():
    valid_path = "/data/valid"
    fraud_path = "/data/fraud"

    valid_amount = 0
    fraud_amount = 0

    for root, _, files in os.walk(valid_path):
        for f in files:
            if f.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(root, f))
                valid_amount += df["amount"].sum()

    for root, _, files in os.walk(fraud_path):
        for f in files:
            if f.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(root, f))
                fraud_amount += df["amount"].sum()

    report = pd.DataFrame([{
        "Total Ingress": valid_amount + fraud_amount,
        "Validated Amount": valid_amount,
        "Fraud Amount": fraud_amount
    }])

    report.to_csv("/data/reports/reconciliation.csv", index=False)

with DAG(
    "reconciliation_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="reconcile",
        python_callable=reconciliation
    )
