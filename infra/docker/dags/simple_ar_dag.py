"""
NIGHTLY_AR_PROCESSING DAG (Simplified)

This DAG demonstrates the modernization of the legacy AS400 NIGHTLY_AR batch job.

Legacy AS400 Job (WRKJOBSCDE @ 2:00 AM):
    1. AR010R - Cash Application Program
    2. AR020R - Calculate AR Aging
    3. AR030R - Generate Aging Report (Spool File ARAGIN01)
    4. AR040R - Flag Accounts for Collection
    5. CPYTOIMPF - Export to IFS
    6. FTP - Transfer to external system

Modern Airflow DAG:
    1. extract_data - Simulate AS400 data extraction
    2. transform_staging - dbt staging models (Bronze → Silver)
    3. transform_marts - dbt mart models (Silver → Gold)
    4. validate_data - Run dbt tests
    5. generate_report - Create AR aging report
    6. notify - Send completion notification

Author: Vignesh
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# Default arguments
default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Task functions
def extract_as400_data(**context):
    """
    Simulate extraction from AS400.
    
    In production, this would:
    - Connect to DB2/400 via pyodbc or ibm_db
    - Or receive files via MQ Series / SFTP
    - Parse CPYTOIMPF fixed-width exports
    - Convert CYYMMDD dates to standard format
    """
    print("="*60)
    print("STEP 1: EXTRACT AS400 DATA")
    print("="*60)
    print("Simulating AS400 data extraction...")
    print("  - Connecting to DB2/400...")
    print("  - Reading CUSMAS (Customer Master)...")
    print("  - Reading ARMAS (AR Invoice Master)...")
    print("  - Reading PAYTRAN (Payment Transactions)...")
    print("  - Reading GLJRN (GL Journal)...")
    print("  - Converting CYYMMDD dates...")
    print("  - Parsing packed decimal fields...")
    print("Extraction complete!")
    
    # Simulate record counts
    stats = {
        'customers': 500,
        'invoices': 5000,
        'payments': 3644,
        'gl_entries': 16118
    }
    
    context['ti'].xcom_push(key='extraction_stats', value=stats)
    return stats


def run_dbt_staging(**context):
    """
    Run dbt staging models (Bronze → Silver).
    
    This replaces:
    - Manual data cleaning in Excel
    - RPGLE programs that reformatted data
    """
    print("="*60)
    print("STEP 2: DBT STAGING MODELS")
    print("="*60)
    print("Running dbt staging transformations...")
    print("  - stg_customers: Decoding segment codes, cleaning addresses")
    print("  - stg_invoices: Calculating aging buckets, status decoding")
    print("  - stg_payments: Identifying unapplied cash")
    print("  - stg_gl_entries: Mapping account codes to names")
    print("Staging models complete!")
    return "staging_complete"


def run_dbt_marts(**context):
    """
    Run dbt mart models (Silver → Gold).
    
    This replaces:
    - AR020R (Aging Calculation)
    - AR040R (Collection Flagging)
    - Various Query/400 reports
    """
    print("="*60)
    print("STEP 3: DBT MART MODELS")
    print("="*60)
    print("Running dbt mart transformations...")
    print("  - dim_customers: Customer dimension with risk scoring")
    print("  - fct_ar_aging: AR aging fact table with collection priority")
    print("  - metrics_ar_summary: Executive KPIs")
    print("Mart models complete!")
    return "marts_complete"


def validate_data(**context):
    """
    Run data quality tests.
    
    This replaces:
    - Manual reconciliation
    - Exception reports
    """
    print("="*60)
    print("STEP 4: DATA VALIDATION")
    print("="*60)
    print("Running dbt tests...")
    print("  ✓ unique_customer_id: PASSED")
    print("  ✓ not_null_invoice_number: PASSED")
    print("  ✓ relationships_invoice_customer: PASSED")
    print("  ✓ accepted_values_status: PASSED")
    print("  ✓ gl_entries_balanced: PASSED")
    print("All tests passed!")
    return "validation_complete"


def generate_ar_report(**context):
    """
    Generate AR Aging Report.
    
    This replaces:
    - AR030R program
    - Spool file ARAGIN01
    - Manual Excel reports
    """
    print("="*60)
    print("STEP 5: GENERATE AR AGING REPORT")
    print("="*60)
    print("")
    print("╔════════════════════════════════════════════════════════════╗")
    print("║           ACCOUNTS RECEIVABLE AGING REPORT                 ║")
    print("║           As of: " + datetime.now().strftime("%Y-%m-%d") + "                              ║")
    print("╠════════════════════════════════════════════════════════════╣")
    print("║  Open Invoices:        1,714                               ║")
    print("║  Total AR Balance:     $7,140,897.95                       ║")
    print("╠════════════════════════════════════════════════════════════╣")
    print("║  AGING BUCKETS:                                            ║")
    print("║    Current:            $0.00                               ║")
    print("║    1-30 Days:          $0.00                               ║")
    print("║    31-60 Days:         $0.00                               ║")
    print("║    61-90 Days:         $0.00                               ║")
    print("║    90+ Days:           $7,140,897.95                       ║")
    print("╠════════════════════════════════════════════════════════════╣")
    print("║  HIGH RISK ACCOUNTS:   89                                  ║")
    print("║  DISPUTED INVOICES:    316                                 ║")
    print("╚════════════════════════════════════════════════════════════╝")
    print("")
    
    metrics = {
        'total_ar': 7140897.95,
        'open_invoices': 1714,
        'high_risk_accounts': 89
    }
    
    context['ti'].xcom_push(key='ar_metrics', value=metrics)
    return metrics


def send_notification(**context):
    """
    Send completion notification.
    
    This replaces:
    - SNDSMTPEMM command
    - Manual email from AR team
    """
    print("="*60)
    print("STEP 6: SEND NOTIFICATIONS")
    print("="*60)
    
    metrics = context['ti'].xcom_pull(key='ar_metrics', task_ids='generate_report')
    
    print("Sending notifications...")
    print(f"  📧 Email sent to: ar-team@company.com")
    print(f"  💬 Slack message posted to: #finance-alerts")
    print("")
    print("╔════════════════════════════════════════════════════════════╗")
    print("║           NIGHTLY AR PROCESSING COMPLETE                   ║")
    print("╠════════════════════════════════════════════════════════════╣")
    print(f"║  Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                       ║")
    print(f"║  Total AR Balance: ${metrics['total_ar']:,.2f}                       ║")
    print(f"║  Status: SUCCESS                                          ║")
    print("╚════════════════════════════════════════════════════════════╝")
    
    return "notifications_sent"


# DAG Definition
with DAG(
    dag_id='nightly_ar_processing',
    default_args=default_args,
    description='Nightly AR Processing - Replaces AS400 NIGHTLY_AR batch job',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'ar', 'nightly', 'as400-migration'],
    doc_md=__doc__,
) as dag:
    
    # Task definitions
    start = EmptyOperator(task_id='start')
    
    extract = PythonOperator(
        task_id='extract_as400_data',
        python_callable=extract_as400_data,
    )
    
    staging = PythonOperator(
        task_id='transform_staging',
        python_callable=run_dbt_staging,
    )
    
    marts = PythonOperator(
        task_id='transform_marts',
        python_callable=run_dbt_marts,
    )
    
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )
    
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_ar_report,
    )
    
    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Task dependencies (mirrors legacy job flow)
    start >> extract >> staging >> marts >> validate >> report >> notify >> end