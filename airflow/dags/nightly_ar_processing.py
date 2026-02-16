"""
NIGHTLY_AR_PROCESSING DAG

This DAG replaces the legacy AS400 NIGHTLY_AR CL job that ran via WRKJOBSCDE.

Legacy Job Flow (AS400):
    1. AR010R - Cash Application
    2. AR020R - AR Aging Calculation  
    3. AR030R - Generate Aging Report (Spool File)
    4. AR040R - Flag Collection Accounts
    5. CPYTOIMPF - Extract to IFS
    6. FTP - Transfer to Analytics Server

Modern Flow (Airflow):
    1. extract_as400_data - Parse fixed-width files
    2. load_to_warehouse - Load to DuckDB via dbt seed
    3. run_dbt_staging - Transform: Bronze → Silver
    4. run_dbt_marts - Transform: Silver → Gold
    5. run_dbt_tests - Data quality checks
    6. generate_reports - Create AR aging reports
    7. notify_completion - Send notifications

Schedule: Daily at 2:00 AM (same as legacy)

Author: Vignesh
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


# =============================================================================
# DAG Configuration
# =============================================================================

default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'email': ['ar-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Project paths
PROJECT_ROOT = Path('/opt/airflow/project')
DBT_PROJECT_DIR = PROJECT_ROOT / 'dbt_project'
DATA_DIR = PROJECT_ROOT / 'data' / 'mock_as400'


# =============================================================================
# Task Functions
# =============================================================================

def extract_as400_data(**context):
    """
    Extract data from AS400 fixed-width files.
    
    In production, this would:
    - Connect to AS400 via ODBC or receive files via MQ/SFTP
    - Parse CPYTOIMPF-style fixed-width exports
    - Handle CYYMMDD date conversion
    
    For this demo, we use pre-generated mock data.
    """
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    
    from src.ingestion.as400_parser import parse_all_files
    from src.utils.config import PHYSICAL_FILES_DIR, EXTRACTS_DIR
    
    print(f"Extracting AS400 data from: {PHYSICAL_FILES_DIR}")
    
    # Parse all AS400 files
    dataframes = parse_all_files(
        input_dir=PHYSICAL_FILES_DIR,
        output_dir=EXTRACTS_DIR
    )
    
    # Log results
    for name, df in dataframes.items():
        print(f"  {name}: {len(df):,} records extracted")
    
    # Push record counts to XCom for downstream tasks
    context['ti'].xcom_push(
        key='extraction_stats',
        value={name: len(df) for name, df in dataframes.items()}
    )
    
    return "Extraction complete"


def validate_extracted_data(**context):
    """
    Validate extracted data before loading.
    
    Checks:
    - File existence
    - Record counts within expected range
    - No completely empty files
    """
    from src.utils.config import EXTRACTS_DIR
    
    print("Validating extracted data...")
    
    required_files = ['cusmas.parquet', 'armas.parquet', 'paytran.parquet', 'gljrn.parquet']
    
    for filename in required_files:
        filepath = EXTRACTS_DIR / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Required file missing: {filepath}")
        print(f"  ✓ {filename} exists")
    
    # Get extraction stats from previous task
    stats = context['ti'].xcom_pull(key='extraction_stats', task_ids='extract.extract_as400_data')
    
    if stats:
        for table, count in stats.items():
            if count == 0:
                raise ValueError(f"Table {table} has 0 records!")
            print(f"  ✓ {table}: {count:,} records")
    
    return "Validation passed"


def generate_ar_report(**context):
    """
    Generate AR Aging report.
    
    This replaces the legacy AR030R program that created
    spool file ARAGIN01.
    """
    import duckdb
    
    db_path = DBT_PROJECT_DIR / 'data' / 'finance.duckdb'
    
    print(f"Connecting to DuckDB: {db_path}")
    conn = duckdb.connect(str(db_path))
    
    # Get AR summary
    summary = conn.execute("""
        SELECT * FROM main_marts.metrics_ar_summary
    """).fetchdf()
    
    print("\n" + "="*60)
    print("AR AGING SUMMARY REPORT")
    print("="*60)
    print(f"Report Date: {summary['report_date'].iloc[0]}")
    print(f"Open Invoices: {summary['open_invoice_count'].iloc[0]:,}")
    print(f"Total AR Balance: ${summary['total_ar_balance'].iloc[0]:,.2f}")
    print("="*60)
    
    # Get top 10 customers by AR balance
    top_customers = conn.execute("""
        SELECT 
            customer_name,
            total_ar_balance,
            risk_category
        FROM main_marts.dim_customers
        WHERE total_ar_balance > 0
        ORDER BY total_ar_balance DESC
        LIMIT 10
    """).fetchdf()
    
    print("\nTop 10 Customers by AR Balance:")
    print(top_customers.to_string(index=False))
    
    conn.close()
    
    # Push metrics to XCom
    context['ti'].xcom_push(
        key='ar_metrics',
        value={
            'total_ar': float(summary['total_ar_balance'].iloc[0]) if summary['total_ar_balance'].iloc[0] else 0,
            'open_invoices': int(summary['open_invoice_count'].iloc[0])
        }
    )
    
    return "Report generated"


def notify_completion(**context):
    """
    Send completion notification.
    
    In production, this would:
    - Send email to AR team
    - Post to Slack channel
    - Update monitoring dashboard
    """
    metrics = context['ti'].xcom_pull(key='ar_metrics', task_ids='report.generate_ar_report')
    
    print("\n" + "="*60)
    print("NIGHTLY AR PROCESSING COMPLETE")
    print("="*60)
    
    if metrics:
        print(f"Total AR Balance: ${metrics['total_ar']:,.2f}")
        print(f"Open Invoices: {metrics['open_invoices']:,}")
    
    print(f"Completion Time: {datetime.now()}")
    print("="*60)
    
    # In production: send_email(), post_to_slack(), etc.
    
    return "Notifications sent"


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id='nightly_ar_processing',
    default_args=default_args,
    description='Nightly AR processing - replaces legacy AS400 NIGHTLY_AR job',
    schedule_interval='0 2 * * *',  # 2:00 AM daily (same as legacy)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'ar', 'nightly', 'as400-migration'],
    doc_md=__doc__
) as dag:
    
    # Start
    start = EmptyOperator(task_id='start')
    
    # ---------------------------------------------------------------------
    # Extract Task Group (replaces CPYTOIMPF)
    # ---------------------------------------------------------------------
    with TaskGroup(group_id='extract') as extract_group:
        
        extract_task = PythonOperator(
            task_id='extract_as400_data',
            python_callable=extract_as400_data,
        )
        
        validate_task = PythonOperator(
            task_id='validate_extracted_data',
            python_callable=validate_extracted_data,
        )
        
        extract_task >> validate_task
    
    # ---------------------------------------------------------------------
    # Transform Task Group (replaces AR010R, AR020R via dbt)
    # ---------------------------------------------------------------------
    with TaskGroup(group_id='transform') as transform_group:
        
        # Load seeds (in case source data changed)
        dbt_seed = BashOperator(
            task_id='dbt_seed',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir ~/.dbt',
        )
        
        # Run staging models (Bronze → Silver)
        dbt_staging = BashOperator(
            task_id='dbt_run_staging',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir ~/.dbt',
        )
        
        # Run mart models (Silver → Gold)
        dbt_marts = BashOperator(
            task_id='dbt_run_marts',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir ~/.dbt',
        )
        
        # Run tests
        dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir ~/.dbt',
        )
        
        dbt_seed >> dbt_staging >> dbt_marts >> dbt_test
    
    # ---------------------------------------------------------------------
    # Report Task Group (replaces AR030R, AR040R)
    # ---------------------------------------------------------------------
    with TaskGroup(group_id='report') as report_group:
        
        generate_report = PythonOperator(
            task_id='generate_ar_report',
            python_callable=generate_ar_report,
        )
    
    # ---------------------------------------------------------------------
    # Notify
    # ---------------------------------------------------------------------
    notify = PythonOperator(
        task_id='notify_completion',
        python_callable=notify_completion,
    )
    
    # End
    end = EmptyOperator(task_id='end')
    
    # ---------------------------------------------------------------------
    # Task Dependencies
    # ---------------------------------------------------------------------
    start >> extract_group >> transform_group >> report_group >> notify >> end