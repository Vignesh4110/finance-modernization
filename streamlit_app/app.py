"""
Finance Modernization Platform - Streamlit Dashboard
Complete AS400 Modernization Showcase
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import os
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import duckdb

# Database path
DB_PATH = PROJECT_ROOT / "data" / "finance.duckdb"

# ============================================================================
# PAGE CONFIG
# ============================================================================
st.set_page_config(
    page_title="Finance Modernization Platform",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CUSTOM CSS
# ============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1rem;
        color: #666;
        margin-top: 0;
    }
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .risk-critical { background-color: #ff4444; color: white; padding: 3px 8px; border-radius: 4px; }
    .risk-high { background-color: #ff8800; color: white; padding: 3px 8px; border-radius: 4px; }
    .risk-medium { background-color: #ffcc00; color: black; padding: 3px 8px; border-radius: 4px; }
    .risk-low { background-color: #00cc66; color: white; padding: 3px 8px; border-radius: 4px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        background-color: #f0f2f6;
        border-radius: 5px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1f77b4;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)

def run_query(query):
    conn = get_connection()
    try:
        return conn.execute(query).fetchdf()
    finally:
        conn.close()

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================
@st.cache_data(ttl=300)
def load_ar_summary():
    return run_query("""
        SELECT * FROM main_marts.metrics_ar_summary
    """)

@st.cache_data(ttl=300)
def load_aging_breakdown():
    return run_query("""
        SELECT 
            aging_bucket,
            COUNT(*) as invoice_count,
            SUM(current_balance) as total_balance,
            AVG(days_past_due) as avg_days_past_due
        FROM main_staging.stg_invoices
        WHERE current_balance > 0
        GROUP BY aging_bucket
        ORDER BY 
            CASE aging_bucket
                WHEN 'Current' THEN 1
                WHEN '1-30 Days' THEN 2
                WHEN '31-60 Days' THEN 3
                WHEN '61-90 Days' THEN 4
                WHEN '90+ Days' THEN 5
                ELSE 6
            END
    """)

@st.cache_data(ttl=300)
def load_top_customers(limit=20):
    return run_query(f"""
        SELECT 
            customer_name,
            segment_name,
            region_name,
            total_ar_balance,
            open_invoice_count,
            over_90_balance,
            payment_rate_pct,
            risk_category,
            credit_limit,
            is_over_credit_limit
        FROM main_marts.dim_customers
        WHERE total_ar_balance > 0
        ORDER BY total_ar_balance DESC
        LIMIT {limit}
    """)

@st.cache_data(ttl=300)
def load_risk_distribution():
    return run_query("""
        SELECT 
            risk_category,
            COUNT(*) as customer_count,
            SUM(total_ar_balance) as total_balance,
            AVG(total_ar_balance) as avg_balance
        FROM main_marts.dim_customers
        WHERE total_ar_balance > 0
        GROUP BY risk_category
        ORDER BY 
            CASE risk_category
                WHEN 'Critical' THEN 1
                WHEN 'High' THEN 2
                WHEN 'Medium' THEN 3
                WHEN 'Low' THEN 4
                ELSE 5
            END
    """)

@st.cache_data(ttl=300)
def load_segment_breakdown():
    return run_query("""
        SELECT 
            segment_name,
            COUNT(*) as customer_count,
            SUM(total_ar_balance) as total_balance,
            AVG(payment_rate_pct) as avg_payment_rate
        FROM main_marts.dim_customers
        WHERE total_ar_balance > 0
        GROUP BY segment_name
        ORDER BY total_balance DESC
    """)

@st.cache_data(ttl=300)
def load_payment_methods():
    return run_query("""
        SELECT 
            payment_method,
            COUNT(*) as payment_count,
            SUM(payment_amount) as total_amount,
            AVG(payment_amount) as avg_amount
        FROM main_staging.stg_payments
        GROUP BY payment_method
        ORDER BY total_amount DESC
    """)

@st.cache_data(ttl=300)
def load_collection_worklist():
    return run_query("""
        SELECT 
            customer_id,
            customer_name,
            segment_name,
            total_ar_balance,
            open_invoice_count,
            over_90_balance,
            payment_rate_pct,
            risk_category,
            credit_limit,
            email
        FROM main_marts.dim_customers
        WHERE total_ar_balance > 0
        ORDER BY 
            CASE risk_category 
                WHEN 'Critical' THEN 1 
                WHEN 'High' THEN 2 
                WHEN 'Medium' THEN 3 
                ELSE 4 
            END,
            total_ar_balance DESC
        LIMIT 100
    """)

@st.cache_data(ttl=300)
def load_region_breakdown():
    return run_query("""
        SELECT 
            region_name,
            COUNT(*) as customer_count,
            SUM(total_ar_balance) as total_balance
        FROM main_marts.dim_customers
        WHERE total_ar_balance > 0
        GROUP BY region_name
        ORDER BY total_balance DESC
    """)

# ============================================================================
# SIDEBAR
# ============================================================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/money-bag.png", width=80)
    st.markdown("## Finance Platform")
    st.markdown("*AS400 Modernization*")
    st.markdown("---")
    
    # Quick Stats
    try:
        summary = load_ar_summary()
        if not summary.empty:
            st.metric("💰 Total AR", f"${summary['total_ar_balance'].iloc[0]:,.0f}")
            st.metric("📄 Open Invoices", f"{int(summary['open_invoice_count'].iloc[0]):,}")
            st.metric("⚠️ Over 90 Days", f"${summary['bucket_over_90'].iloc[0]:,.0f}")
            st.metric("📊 DSO", f"{summary['days_sales_outstanding'].iloc[0]:.1f} days")
    except Exception as e:
        st.error(f"Error loading summary")
    
    st.markdown("---")
    st.markdown("### 🔧 Settings")
    auto_refresh = st.checkbox("Auto-refresh (5 min)", value=False)
    
    st.markdown("---")
    st.markdown("### 📅 Last Updated")
    st.markdown(f"*{datetime.now().strftime('%Y-%m-%d %H:%M')}*")

# ============================================================================
# MAIN CONTENT
# ============================================================================
st.markdown('<p class="main-header">💰 Finance Modernization Platform</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Transforming Legacy AS400 Systems into Modern Cloud-Native Architecture</p>', unsafe_allow_html=True)
st.markdown("")

# Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 AR Dashboard", 
    "👥 Customer Analysis", 
    "📋 Collection Worklist",
    "🤖 AI Assistant",
    "ℹ️ About"
])

# ============================================================================
# TAB 1: AR DASHBOARD
# ============================================================================
with tab1:
    st.header("📊 Accounts Receivable Dashboard")
    
    try:
        summary = load_ar_summary()
        
        # Top Metrics Row
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total AR Balance",
                f"${summary['total_ar_balance'].iloc[0]:,.2f}",
                help="Total outstanding accounts receivable"
            )
        with col2:
            st.metric(
                "Open Invoices",
                f"{int(summary['open_invoice_count'].iloc[0]):,}",
                help="Number of unpaid invoices"
            )
        with col3:
            st.metric(
                "DSO",
                f"{summary['days_sales_outstanding'].iloc[0]:.1f} days",
                help="Days Sales Outstanding"
            )
        with col4:
            st.metric(
                "Current %",
                f"{summary['pct_current'].iloc[0]:.1f}%",
                help="Percentage of AR that is current"
            )
        with col5:
            st.metric(
                "Over 90 Days %",
                f"{summary['pct_over_90'].iloc[0]:.1f}%",
                delta=f"-{summary['pct_over_90'].iloc[0]:.1f}%" if summary['pct_over_90'].iloc[0] > 10 else None,
                delta_color="inverse",
                help="Percentage of AR over 90 days"
            )
        
        st.markdown("---")
        
        # Aging Analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 AR Aging by Amount")
            aging = load_aging_breakdown()
            
            colors = ['#28a745', '#7cb342', '#ffc107', '#fd7e14', '#dc3545']
            fig = px.bar(
                aging,
                x='aging_bucket',
                y='total_balance',
                color='aging_bucket',
                color_discrete_sequence=colors,
                labels={'total_balance': 'Balance ($)', 'aging_bucket': 'Aging Bucket'},
                text=aging['total_balance'].apply(lambda x: f'${x:,.0f}')
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🥧 AR Aging Distribution")
            fig = px.pie(
                aging,
                values='total_balance',
                names='aging_bucket',
                color='aging_bucket',
                color_discrete_sequence=colors,
                hole=0.4
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Second Row
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("💳 Payment Methods")
            payments = load_payment_methods()
            fig = px.bar(
                payments,
                x='payment_method',
                y='total_amount',
                color='payment_method',
                labels={'total_amount': 'Total Amount ($)', 'payment_method': 'Method'},
                text=payments['total_amount'].apply(lambda x: f'${x:,.0f}')
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🌎 AR by Region")
            regions = load_region_breakdown()
            fig = px.pie(
                regions,
                values='total_balance',
                names='region_name',
                hole=0.3
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        # Aging Table
        st.subheader("📋 Aging Summary Table")
        aging_display = aging.copy()
        aging_display['total_balance'] = aging_display['total_balance'].apply(lambda x: f"${x:,.2f}")
        aging_display['avg_days_past_due'] = aging_display['avg_days_past_due'].apply(lambda x: f"{x:.0f} days")
        aging_display.columns = ['Aging Bucket', 'Invoice Count', 'Total Balance', 'Avg Days Past Due']
        st.dataframe(aging_display, use_container_width=True, hide_index=True)
        
    except Exception as e:
        st.error(f"Error loading AR Dashboard: {e}")

# ============================================================================
# TAB 2: CUSTOMER ANALYSIS
# ============================================================================
with tab2:
    st.header("👥 Customer Analysis")
    
    try:
        # Risk Distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🎯 Risk Distribution by Balance")
            risk_data = load_risk_distribution()
            
            risk_colors = {
                'Critical': '#dc3545',
                'High': '#fd7e14',
                'Medium': '#ffc107',
                'Low': '#28a745'
            }
            
            fig = px.pie(
                risk_data,
                values='total_balance',
                names='risk_category',
                color='risk_category',
                color_discrete_map=risk_colors,
                hole=0.4
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📊 Customers by Risk Category")
            fig = px.bar(
                risk_data,
                x='risk_category',
                y='customer_count',
                color='risk_category',
                color_discrete_map=risk_colors,
                text='customer_count'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Segment Analysis
        st.subheader("🏢 AR by Customer Segment")
        segments = load_segment_breakdown()
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                segments,
                x='segment_name',
                y='total_balance',
                color='segment_name',
                text=segments['total_balance'].apply(lambda x: f'${x:,.0f}')
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                segments,
                x='segment_name',
                y='avg_payment_rate',
                color='segment_name',
                text=segments['avg_payment_rate'].apply(lambda x: f'{x:.1f}%'),
                title='Average Payment Rate by Segment'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        # Top Customers Table
        st.subheader("🏆 Top 20 Customers by AR Balance")
        
        top_customers = load_top_customers(20)
        
        # Add risk badge column
        def risk_badge(risk):
            colors = {
                'Critical': '🔴',
                'High': '🟠',
                'Medium': '🟡',
                'Low': '🟢'
            }
            return f"{colors.get(risk, '⚪')} {risk}"
        
        display_df = top_customers.copy()
        display_df['risk_category'] = display_df['risk_category'].apply(risk_badge)
        display_df['total_ar_balance'] = display_df['total_ar_balance'].apply(lambda x: f"${x:,.2f}")
        display_df['over_90_balance'] = display_df['over_90_balance'].apply(lambda x: f"${x:,.2f}")
        display_df['credit_limit'] = display_df['credit_limit'].apply(lambda x: f"${x:,.0f}")
        display_df['payment_rate_pct'] = display_df['payment_rate_pct'].apply(lambda x: f"{x:.1f}%")
        
        display_df = display_df[['customer_name', 'segment_name', 'total_ar_balance', 
                                  'open_invoice_count', 'over_90_balance', 'payment_rate_pct', 
                                  'risk_category', 'credit_limit']]
        display_df.columns = ['Customer', 'Segment', 'AR Balance', 'Open Invoices', 
                              'Over 90 Days', 'Payment Rate', 'Risk', 'Credit Limit']
        
        st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)
        
    except Exception as e:
        st.error(f"Error loading Customer Analysis: {e}")

# ============================================================================
# TAB 3: COLLECTION WORKLIST
# ============================================================================
with tab3:
    st.header("📋 Collection Worklist")
    st.markdown("*Prioritized by ML-based risk scoring*")
    
    try:
        worklist = load_collection_worklist()
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        critical = worklist[worklist['risk_category'] == 'Critical Risk']
        high = worklist[worklist['risk_category'] == 'High Risk']
        
        with col1:
            st.metric("🔴 Critical Accounts", len(critical))
        with col2:
            st.metric("🟠 High Risk Accounts", len(high))
        with col3:
            total_critical = critical['total_ar_balance'].sum() if len(critical) > 0 else 0
            st.metric("💰 Critical AR", f"${total_critical:,.0f}")
        with col4:
            total_high = high['total_ar_balance'].sum() if len(high) > 0 else 0
            st.metric("💰 High Risk AR", f"${total_high:,.0f}")
        
        st.markdown("---")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            risk_filter = st.multiselect(
            "Filter by Risk",
            options=['High Risk', 'Medium Risk', 'Low Risk'],
            default=['High Risk']
)
        with col2:
            min_balance = st.number_input("Min Balance ($)", value=0, step=1000)
        with col3:
            segment_filter = st.multiselect(
                "Filter by Segment",
                options=worklist['segment_name'].unique().tolist(),
                default=[]
            )
        
        # Apply filters
        filtered = worklist.copy()
        if risk_filter:
            filtered = filtered[filtered['risk_category'].isin(risk_filter)]
        if min_balance > 0:
            filtered = filtered[filtered['total_ar_balance'] >= min_balance]
        if segment_filter:
            filtered = filtered[filtered['segment_name'].isin(segment_filter)]
        
        st.subheader(f"Showing {len(filtered)} accounts")
        
        # Collection cards
        for idx, row in filtered.head(20).iterrows():
            risk_colors = {
                'Critical': '🔴',
                'High': '🟠', 
                'Medium': '🟡',
                'Low': '🟢'
            }
            
            with st.expander(f"{risk_colors.get(row['risk_category'], '⚪')} **{row['customer_name']}** - ${row['total_ar_balance']:,.2f}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"**Segment:** {row['segment_name']}")
                    st.markdown(f"**Open Invoices:** {int(row['open_invoice_count'])}")
                    st.markdown(f"**Over 90 Days:** ${row['over_90_balance']:,.2f}")
                
                with col2:
                    st.markdown(f"**Risk Category:** {row['risk_category']}")
                    st.markdown(f"**Payment Rate:** {row['payment_rate_pct']:.1f}%")
                    st.markdown(f"**Credit Limit:** ${row['credit_limit']:,.0f}")
                
                with col3:
                    if st.button("✉️ Generate Collection Email", key=f"email_{idx}"):
                        with st.spinner("Generating email with AI..."):
                            try:
                                from src.llm_agents.agents.collections_agent import CollectionsAgent, CustomerAccount
                                
                                agent = CollectionsAgent()
                                account = CustomerAccount(
                                    customer_id=str(row['customer_id']),
                                    customer_name=row['customer_name'],
                                    contact_name="Accounts Payable",
                                    email=row['email'] if row['email'] else "ap@company.com",
                                    total_balance=float(row['total_ar_balance']),
                                    days_past_due=int(row['over_90_balance'] / max(row['total_ar_balance'], 1) * 90),
                                    invoice_count=int(row['open_invoice_count']),
                                    payment_history_score=int(row['payment_rate_pct']),
                                    segment=row['segment_name']
                                )
                                
                                email = agent.generate_email(account)
                                
                                st.markdown("---")
                                st.markdown(f"**To:** {email.to}")
                                st.markdown(f"**Subject:** {email.subject}")
                                st.markdown(f"**Tone:** {email.tone.name}")
                                st.text_area("Email Body", email.body, height=250, key=f"body_{idx}")
                                
                            except Exception as e:
                                st.error(f"Error: {e}")
                
    except Exception as e:
        st.error(f"Error loading Collection Worklist: {e}")

# ============================================================================
# TAB 4: AI ASSISTANT
# ============================================================================
with tab4:
    st.header("🤖 AI Query Assistant")
    st.markdown("*Ask questions about your AR data in natural language - Powered by Groq (Llama 3.3)*")
    
    # Example questions
    st.markdown("**Try these examples:**")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("💰 Total AR Balance"):
            st.session_state['ai_query'] = "What is our total accounts receivable balance?"
    with col2:
        if st.button("🏆 Top Customers"):
            st.session_state['ai_query'] = "Show me the top 10 customers by AR balance"
    with col3:
        if st.button("📊 Aging Breakdown"):
            st.session_state['ai_query'] = "What is the aging breakdown of our receivables?"
    with col4:
        if st.button("⚠️ High Risk"):
            st.session_state['ai_query'] = "Which customers are high risk?"
    
    st.markdown("---")
    
    # Query input
    query = st.text_input(
        "Ask a question about your AR data:",
        value=st.session_state.get('ai_query', ''),
        placeholder="e.g., What is our total AR balance by customer segment?"
    )
    
    col1, col2 = st.columns([1, 5])
    with col1:
        ask_button = st.button("🔍 Ask AI", type="primary")
    
    if ask_button and query:
        with st.spinner("🤔 Thinking..."):
            try:
                from src.llm_agents.agents.ar_query_agent import ARQueryAgent
                
                agent = ARQueryAgent()
                result = agent.ask(query)
                
                if result.get('error'):
                    st.error(f"❌ {result['answer']}")
                else:
                    st.success("✅ Answer:")
                    st.markdown(result['answer'])
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        with st.expander("🔧 View Generated SQL"):
                            st.code(result['sql'], language='sql')
                    
                    with col2:
                        if result['results'] is not None and len(result['results']) > 0:
                            with st.expander(f"📊 View Raw Data ({len(result['results'])} rows)"):
                                st.dataframe(result['results'], use_container_width=True)
                
                agent.close()
                
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    # Tips section
    st.markdown("---")
    st.markdown("### 💡 Query Tips")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        - Ask about **totals**, **averages**, **counts**
        - Filter by **customer**, **segment**, **region**
        - Request **top N** or **bottom N** records
        """)
    with col2:
        st.markdown("""
        - Ask about **aging buckets** and **risk categories**
        - Query **payment methods** and **trends**
        - Combine multiple criteria
        """)

# ============================================================================
# TAB 5: ABOUT
# ============================================================================
with tab5:
    st.header("ℹ️ About This Project")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ## 🏭 Finance Modernization Platform
        
        This project demonstrates the **complete modernization** of a legacy AS400/IBM i 
        financial system into a modern, cloud-native data platform with ML and AI capabilities.
        
        ### 🎯 What Was Modernized
        
        | Legacy AS400 | Modern Platform |
        |--------------|-----------------|
        | RPGLE Batch Programs | Python + Airflow DAGs |
        | DB2/400 Tables | DuckDB / Snowflake |
        | Query/400 Reports | dbt Models + Streamlit |
        | Green Screen UI | Interactive Dashboard |
        | Manual Collections | AI-Powered Email Agent |
        | Static Reports | Natural Language Queries |
        
        ### 🛠️ Technology Stack
        
        | Layer | Technologies |
        |-------|-------------|
        | **Data Extraction** | Python, Custom AS400 Parser (CYYMMDD dates, fixed-width) |
        | **Storage** | DuckDB (dev), Snowflake/BigQuery (prod) |
        | **Transformation** | dbt Core (Bronze → Silver → Gold) |
        | **Orchestration** | Apache Airflow |
        | **Machine Learning** | scikit-learn, XGBoost, MLflow |
        | **LLM Integration** | Groq (Llama 3.3 70B) - FREE! |
        | **Dashboard** | Streamlit, Plotly |
        | **Infrastructure** | Docker, GitHub Actions |
        
        ### 📁 Project Structure
        
        ```
        finance-modernization/
        ├── src/
        │   ├── ingestion/       # AS400 file parsers
        │   ├── ml/              # ML models (payment propensity, risk scoring)
        │   └── llm_agents/      # AI agents (collections, query, documenter)
        ├── dbt_project/         # dbt models (staging → marts)
        ├── airflow/             # DAG definitions
        ├── streamlit_app/       # This dashboard
        ├── data/                # DuckDB database
        └── docs/                # Legacy system documentation
        ```
        """)
    
    with col2:
        st.markdown("### 👤 Author")
        st.markdown("""
        **Vignesh**
        
        *Data Analytics Engineering*
        *Northeastern University*
        
        ---
        
        ### 🔗 Links
        
        [📂 GitHub Repository](https://github.com/Vignesh4110/finance-modernization)
        
        ---
        
        ### 🏆 Key Features
        
        ✅ AS400 Fixed-Width Parser
        
        ✅ CYYMMDD Date Conversion
        
        ✅ dbt Data Models
        
        ✅ Airflow Orchestration
        
        ✅ ML Risk Scoring
        
        ✅ AI Query Assistant
        
        ✅ Smart Collections Agent
        """)
    
    # System Status
    st.markdown("---")
    st.subheader("🔧 System Status")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        try:
            conn = get_connection()
            conn.execute("SELECT 1")
            conn.close()
            st.success("✅ Database")
        except:
            st.error("❌ Database")
    
    with col2:
        if os.getenv("GROQ_API_KEY"):
            st.success("✅ Groq API")
        else:
            st.warning("⚠️ Groq API")
    
    with col3:
        try:
            summary = load_ar_summary()
            st.success("✅ dbt Models")
        except:
            st.error("❌ dbt Models")
    
    with col4:
        ml_path = PROJECT_ROOT / "src" / "ml"
        if ml_path.exists():
            st.success("✅ ML Pipeline")
        else:
            st.warning("⚠️ ML Pipeline")

# ============================================================================
# FOOTER
# ============================================================================
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 20px;'>
        <strong>Finance Modernization Platform</strong> | 
        Built using Streamlit, dbt, Airflow & Groq AI from Existing AS400, DB2|
        © 2026 Vignesh
    </div>
    """,
    unsafe_allow_html=True
)