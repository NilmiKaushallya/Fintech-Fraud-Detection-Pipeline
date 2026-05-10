from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import shutil

# Define Paths
VALID_PATH = "/data/valid"
FRAUD_PATH = "/data/fraud"
REPORT_PATH = "/data/reports"
WAREHOUSE_PATH = "/data/warehouse"

def reconciliation():
    valid_amount = 0
    fraud_amount = 0

    # 1. Calculate Valid Amount and "Move" to Warehouse
    os.makedirs(WAREHOUSE_PATH, exist_ok=True)
    
    if os.path.exists(VALID_PATH):
        for root, _, files in os.walk(VALID_PATH):
            for f in files:
                if f.endswith(".parquet"):
                    file_path = os.path.join(root, f)
                    try:
                        df = pd.read_parquet(file_path)
                        valid_amount += df["amount"].sum()
                        # Simulate movement to warehouse
                        shutil.copy(file_path, os.path.join(WAREHOUSE_PATH, f))
                    except Exception as e:
                        print(f"Error reading valid file {f}: {e}")

    # 2. Calculate Fraud Amount
    if os.path.exists(FRAUD_PATH):
        for root, _, files in os.walk(FRAUD_PATH):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        file_path = os.path.join(root, f)
                        df = pd.read_parquet(file_path)
                        # Summing 'total_amount' from Spark aggregation
                        fraud_amount += df["total_amount"].sum()
                    except Exception as e:
                        print(f"Error reading fraud file {f}: {e}")

    # 3. Generate Reconciliation Report DataFrame
    report = pd.DataFrame([{
        "Run_Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Total Ingress": valid_amount + fraud_amount,
        "Validated Amount": valid_amount,
        "Fraud Amount": fraud_amount
    }])

    # 4. Save with APPEND logic so logs update instead of overwrite
    os.makedirs(REPORT_PATH, exist_ok=True)
    csv_file = os.path.join(REPORT_PATH, "reconciliation.csv")
    file_exists = os.path.isfile(csv_file)

    report.to_csv(csv_file, mode='a', index=False, header=not file_exists)
    print(f"Reconciliation successful: Total Ingress {valid_amount + fraud_amount}")

def fraud_by_category():
    all_data = []

    if not os.path.exists(FRAUD_PATH):
        print("No fraud data directory found.")
        return

    # Read all fraud parquet files
    for root, _, files in os.walk(FRAUD_PATH):
        for f in files:
            if f.endswith(".parquet"):
                try:
                    df = pd.read_parquet(os.path.join(root, f))
                    all_data.append(df)
                except Exception as e:
                    print(f"Error reading {f}: {e}")

    os.makedirs(REPORT_PATH, exist_ok=True)
    report_file = os.path.join(REPORT_PATH, "fraud_by_category.csv")

    if len(all_data) == 0:
        # Create empty template if no fraud exists
        fraud_report = pd.DataFrame(columns=["merchant_category", "fraud_count"])
    else:
        full_df = pd.concat(all_data)
        # Count occurrences per category
        fraud_report = (
            full_df.groupby("merchant_category")
            .size()
            .reset_index(name="fraud_count")
        )

    fraud_report.to_csv(report_file, index=False)
    print("Fraud by category report updated.")

with DAG(
    "reconciliation_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 */6 * * *", # Every 6 hours
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=1)}
) as dag:

    reconcile_task = PythonOperator(
        task_id="reconcile_and_warehouse_move",
        python_callable=reconciliation
    )

    fraud_category_task = PythonOperator(
        task_id="fraud_by_category_report",
        python_callable=fraud_by_category
    )

    reconcile_task >> fraud_category_task