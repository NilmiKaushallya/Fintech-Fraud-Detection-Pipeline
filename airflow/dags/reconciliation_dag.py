from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os


def reconciliation():
    valid_path = "/data/valid"
    fraud_path = "/data/fraud"
    report_path = "/data/reports"

    # Ensure report directory exists
    os.makedirs(report_path, exist_ok=True)

    valid_amount = 0.0
    fraud_amount = 0.0

    # ----------------------------
    # Process VALID transactions
    # ----------------------------
    if os.path.exists(valid_path):
        for root, _, files in os.walk(valid_path):
            for f in files:
                if f.endswith(".parquet"):
                    file_path = os.path.join(root, f)
                    try:
                        df = pd.read_parquet(file_path)
                        if "amount" in df.columns:
                            valid_amount += df["amount"].sum()
                    except Exception as e:
                        print(f"Error reading {file_path}: {e}")

    # ----------------------------
    # Process FRAUD transactions
    # ----------------------------
    if os.path.exists(fraud_path):
        for root, _, files in os.walk(fraud_path):
            for f in files:
                if f.endswith(".parquet"):
                    file_path = os.path.join(root, f)
                    try:
                        df = pd.read_parquet(file_path)

                        # Some fraud files may use 'total_amount'
                        if "total_amount" in df.columns:
                            fraud_amount += df["total_amount"].sum()
                        elif "amount" in df.columns:
                            fraud_amount += df["amount"].sum()

                    except Exception as e:
                        print(f"Error reading {file_path}: {e}")

    # ----------------------------
    # Generate reconciliation report
    # ----------------------------
    total_ingress = valid_amount + fraud_amount

    report = pd.DataFrame([{
        "Total Ingress": float(total_ingress),
        "Validated Amount": float(valid_amount),
        "Fraud Amount": float(fraud_amount)
    }])

    report_file = os.path.join(report_path, "reconciliation.csv")
    report.to_csv(report_file, index=False)

    print("Reconciliation report generated successfully.")
    print(report)


# ----------------------------
# Airflow DAG
# ----------------------------
with DAG(
    dag_id="reconciliation_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",   # Every 6 hours
    catchup=False,
    tags=["fraud", "reconciliation"]
) as dag:

    task = PythonOperator(
        task_id="reconcile",
        python_callable=reconciliation
    )