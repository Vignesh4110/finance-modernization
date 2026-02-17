"""
Finance Modernization Platform - Streamlit Dashboard
Transforming Legacy AS400 Systems into Modern Cloud-Native Architecture
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import os
from datetime import datetime
from dotenv import load_dotenv
import duckdb
import subprocess

# ============================================================================
# SETUP & CONFIGURATION
# ============================================================================
load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

DB_PATH = PROJECT_ROOT / "data" / "finance.duckdb"
DBT_PROJECT = PROJECT_ROOT / "dbt_project"

# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================
@st.cache_resource
def initialize_database():
    """Initialize database if it doesn't exist"""
    if not DB_PATH.exists():
        st.info("🔄 Building data warehouse for the first time... This may take a minute.")
        
        # Create data directory
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Create dbt profiles directory and file
        dbt_profiles_dir = Path.home() / ".dbt"
        dbt_profiles_dir.mkdir(parents=True, exist_ok=True)
        
        # Use absolute path for the database
        profiles_yml = dbt_profiles_dir / "profiles.yml"
        profiles_yml.write_text(f"""
finance_dw:
  outputs:
    dev:
      type: duckdb
      path: {DB_PATH.absolute()}
      threads: 4
  target: dev
""")
        
        st.write(f"✅ Created dbt profiles at {profiles_yml}")
        st.write(f"✅ Database will be created at: {DB_PATH.absolute()}")
        
        # Run dbt to build the database
        try:
            # Change to dbt project directory
            os.chdir(DBT_PROJECT)
            
            # Run dbt seed
            st.write("Running dbt seed...")
            result = subprocess.run(
                ["dbt", "seed", "--profiles-dir", str(dbt_profiles_dir)], 
                capture_output=True, 
                text=True
            )
            
            if result.returncode != 0:
                st.error(f"❌ dbt seed failed:")
                st.code(result.stdout)
                st.code(result.stderr)
                return False
            
            st.write("✅ Seeds loaded")
            
            # Run dbt run
            st.write("Running dbt models...")
            result = subprocess.run(
                ["dbt", "run", "--profiles-dir", str(dbt_profiles_dir)], 
                capture_output=True, 
                text=True
            )
            
            if result.returncode != 0:
                st.error(f"❌ dbt run failed:")
                st.code(result.stdout)
                st.code(result.stderr)
                return False
            
            st.success("✅ Data warehouse built successfully!")
            
        except FileNotFoundError:
            st.error("❌ dbt command not found. Make sure dbt-core and dbt-duckdb are in requirements.txt")
            return False
        except Exception as e:
            st.error(f"❌ Unexpected error: {e}")
            import traceback
            st.code(traceback.format_exc())
            return False
        finally:
            # Change back to original directory
            os.chdir(PROJECT_ROOT)
    
    return True

# ============================================================================
# PAGE CONFIG
# ============================================================================
st.set_page_config(
    page_title="Finance Modernization Platform",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database
if not initialize_database():
    st.stop()

# ============================================================================
# DATABASE CONNECTION
# ============================================================================
@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)

def run_query(query):
    conn = get_connection()
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

# ============================================================================
# CUSTOM CSS
# ============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        text-align: center;
        color: #666;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# HEADER
# ============================================================================
st.markdown('<div class="main-header">💰 Finance Modernization Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Transforming Legacy AS400 Systems into Modern Cloud-Native Architecture</div>', unsafe_allow_html=True)

# ============================================================================
# SIDEBAR
# ============================================================================
with st.sidebar:
    st.title("Finance Platform")
    st.markdown("*AS400 Modernization*")
    st.markdown("---")
    
    page = st.radio("Navigation", 
                   ["📊 AR Dashboard", "👥 Customer Analysis", 
                    "📋 Collection Worklist", "🤖 AI Assistant"])
    
    st.markdown("---")
    st.markdown("### 📅 Last Updated")
    st.info(datetime.now().strftime("%Y-%m-%d %H:%M"))

# ============================================================================
# MAIN CONTENT
# ============================================================================

if page == "📊 AR Dashboard":
    st.header("📊 Accounts Receivable Dashboard")
    
    # KPI Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    metrics = run_query("""
        SELECT 
            COUNT(*) as total_invoices,
            SUM(current_balance) as total_ar,
            AVG(days_outstanding) as avg_days,
            SUM(CASE WHEN days_outstanding > 90 THEN current_balance ELSE 0 END) as past_due_90
        FROM main_marts.fct_invoices
        WHERE status = 'OP'
    """)
    
    if not metrics.empty:
        with col1:
            st.metric("Total Open Invoices", f"{int(metrics['total_invoices'].iloc[0]):,}")
        with col2:
            st.metric("Total AR", f"${metrics['total_ar'].iloc[0]:,.2f}")
        with col3:
            st.metric("Avg Days Outstanding", f"{metrics['avg_days'].iloc[0]:.0f}")
        with col4:
            st.metric("Past Due 90+", f"${metrics['past_due_90'].iloc[0]:,.2f}")
    
    st.markdown("---")
    
    # AR Aging
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("AR Aging Analysis")
        aging = run_query("""
            SELECT 
                aging_bucket,
                SUM(current_balance) as balance,
                COUNT(*) as invoice_count
            FROM main_marts.fct_ar_aging
            GROUP BY aging_bucket
            ORDER BY 
                CASE aging_bucket
                    WHEN 'Current' THEN 1
                    WHEN '1-30 Days' THEN 2
                    WHEN '31-60 Days' THEN 3
                    WHEN '61-90 Days' THEN 4
                    WHEN '90+ Days' THEN 5
                END
        """)
        
        if not aging.empty:
            fig = px.bar(aging, x='aging_bucket', y='balance', 
                        title='AR Balance by Aging Bucket',
                        labels={'balance': 'Balance ($)', 'aging_bucket': 'Aging Bucket'})
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Top 10 Customers by AR Balance")
        top_customers = run_query("""
            SELECT 
                customer_name,
                SUM(current_balance) as balance
            FROM main_marts.fct_invoices f
            JOIN main_marts.dim_customers c ON f.customer_id = c.customer_id
            WHERE status = 'OP'
            GROUP BY customer_name
            ORDER BY balance DESC
            LIMIT 10
        """)
        
        if not top_customers.empty:
            fig = px.bar(top_customers, x='balance', y='customer_name', 
                        orientation='h',
                        title='Top 10 Customers',
                        labels={'balance': 'Balance ($)', 'customer_name': 'Customer'})
            st.plotly_chart(fig, use_container_width=True)

elif page == "👥 Customer Analysis":
    st.header("👥 Customer Analysis")
    
    segments = run_query("""
        SELECT 
            segment,
            COUNT(*) as customer_count,
            SUM(credit_limit) as total_credit,
            SUM(credit_used) as total_used
        FROM main_marts.dim_customers
        GROUP BY segment
    """)
    
    if not segments.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(segments, values='customer_count', names='segment',
                        title='Customer Distribution by Segment')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(segments, x='segment', y=['total_credit', 'total_used'],
                        title='Credit Limit vs Used by Segment',
                        barmode='group')
            st.plotly_chart(fig, use_container_width=True)

elif page == "📋 Collection Worklist":
    st.header("📋 Collection Worklist")
    
    collections = run_query("""
        SELECT 
            customer_name,
            SUM(current_balance) as balance,
            MAX(days_outstanding) as max_days,
            COUNT(*) as invoice_count
        FROM main_marts.fct_invoices f
        JOIN main_marts.dim_customers c ON f.customer_id = c.customer_id
        WHERE status = 'OP' AND days_outstanding > 60
        GROUP BY customer_name
        ORDER BY balance DESC
        LIMIT 20
    """)
    
    if not collections.empty:
        st.dataframe(collections, use_container_width=True)
    else:
        st.info("No customers requiring collection action.")

elif page == "🤖 AI Assistant":
    st.header("🤖 AI Assistant")
    st.info("💡 AI features require Groq API key")

# ============================================================================
# FOOTER
# ============================================================================
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p><strong>Finance Modernization Platform</strong> | Built using Streamlit, dbt, Airflow & Groq AI from<br>
    Existing AS400, DB2 | © 2026 Vignesh</p>
</div>
""", unsafe_allow_html=True)
