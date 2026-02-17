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
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================
@st.cache_resource
def initialize_database():
    """Initialize database if it doesn't exist"""
    if not DB_PATH.exists():
        st.info("🔄 Building data warehouse for the first time... This may take a minute.")
        
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        dbt_profiles_dir = Path.home() / ".dbt"
        dbt_profiles_dir.mkdir(parents=True, exist_ok=True)
        
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
        
        try:
            os.chdir(DBT_PROJECT)
            
            result = subprocess.run(
                ["dbt", "seed", "--profiles-dir", str(dbt_profiles_dir)], 
                capture_output=True, text=True
            )
            if result.returncode != 0:
                st.error(f"❌ dbt seed failed:")
                st.code(result.stdout)
                return False
            
            st.write("✅ Seeds loaded")
            
            result = subprocess.run(
                ["dbt", "run", "--profiles-dir", str(dbt_profiles_dir)], 
                capture_output=True, text=True
            )
            if result.returncode != 0:
                st.error(f"❌ dbt run failed:")
                st.code(result.stdout)
                return False
            
            st.success("✅ Data warehouse built successfully!")
            
        except Exception as e:
            st.error(f"❌ Error: {e}")
            return False
        finally:
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
# LLM FUNCTIONS
# ============================================================================
def query_llm(prompt, system_prompt="You are a helpful AR/finance assistant."):
    """Query Groq LLM"""
    if not GROQ_API_KEY:
        return "⚠️ GROQ_API_KEY not found in environment variables"
    
    try:
        from groq import Groq
        
        # Create client without proxies parameter
        client = Groq(api_key=GROQ_API_KEY)
        
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=1000
        )
        
        return response.choices[0].message.content
        
    except ImportError:
        return "❌ Error: groq package not installed"
    except Exception as e:
        return f"❌ Error calling Groq API: {str(e)}"

def generate_collection_email(customer_name, balance, days_overdue):
    """Generate collection email"""
    prompt = f"""Write a professional but firm collection email for:
Customer: {customer_name}
Outstanding Balance: ${balance:,.2f}
Days Overdue: {days_overdue}

Make it polite but clear about urgency. Keep it under 150 words. Include:
1. Reference to the overdue amount
2. Request for immediate payment
3. Offer to discuss payment plan
4. Contact information"""
    
    return query_llm(prompt, "You are a professional collections specialist.")

# ============================================================================
# CUSTOM CSS
# ============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        text-align: center;
        color: #666;
        margin-bottom: 2rem;
    }
    .stMetric {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .stMetric label {
        color: #2c3e50 !important;
        font-weight: 600;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #1f77b4 !important;
        font-size: 1.8rem !important;
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
    st.markdown("### 💼 Finance Platform")
    st.caption("*AS400 Modernization*")
    st.markdown("---")
    
    page = st.radio(
        "Navigation", 
        ["📊 AR Dashboard", "👥 Customer Analysis", "📋 Collection Worklist", "🤖 AI Assistant"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # Quick Stats
    st.markdown("### 📈 Quick Stats")
    try:
        quick_stats = run_query("""
            SELECT 
                COUNT(DISTINCT customer_id) as total_customers,
                COUNT(*) as total_invoices,
                SUM(current_balance) as total_ar
            FROM main_marts.fct_invoices
            WHERE status = 'Open'
        """)
        if not quick_stats.empty:
            st.metric("Active Customers", f"{int(quick_stats['total_customers'].iloc[0]):,}")
            st.metric("Open Invoices", f"{int(quick_stats['total_invoices'].iloc[0]):,}")
            st.metric("Total AR", f"${quick_stats['total_ar'].iloc[0]/1000:.0f}K")
    except:
        pass
    
    st.markdown("---")
    st.markdown("### 📅 Last Updated")
    st.caption(datetime.now().strftime("%Y-%m-%d %H:%M"))
    
    st.markdown("---")
    st.markdown("### ℹ️ About")
    if st.button("Show Info"):
        st.info("Built with Streamlit, dbt, DuckDB & Groq AI")

# ============================================================================
# MAIN CONTENT - AR DASHBOARD
# ============================================================================

if page == "📊 AR Dashboard":
    st.header("📊 Accounts Receivable Dashboard")
    
    # KPI Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    
    metrics = run_query("""
        SELECT 
            COUNT(*) as total_invoices,
            SUM(current_balance) as total_ar,
            AVG(days_outstanding) as avg_days,
            SUM(CASE WHEN days_outstanding > 90 THEN current_balance ELSE 0 END) as past_due_90
        FROM main_marts.fct_invoices
        WHERE status = 'Open'
    """)
    
    if not metrics.empty:
        with col1:
            st.metric(
                label="📄 Total Open Invoices", 
                value=f"{int(metrics['total_invoices'].iloc[0]):,}",
                delta="Active"
            )
        with col2:
            st.metric(
                label="💵 Total AR", 
                value=f"${metrics['total_ar'].iloc[0]:,.0f}",
                delta=f"${metrics['total_ar'].iloc[0]/1000:.0f}K"
            )
        with col3:
            st.metric(
                label="⏱️ Avg Days Outstanding", 
                value=f"{metrics['avg_days'].iloc[0]:.0f}",
                delta="Days"
            )
        with col4:
            st.metric(
                label="⚠️ Past Due 90+", 
                value=f"${metrics['past_due_90'].iloc[0]:,.0f}",
                delta="High Risk"
            )
    
    st.markdown("---")
    
    # Charts Row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔴 AR Aging Analysis")
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
            colors = ['#27ae60', '#f39c12', '#e67e22', '#e74c3c', '#8e44ad']
            fig = px.bar(
                aging, 
                x='aging_bucket', 
                y='balance', 
                title='AR Balance by Aging Bucket',
                labels={'balance': 'Balance ($)', 'aging_bucket': 'Aging Bucket'},
                color='aging_bucket',
                color_discrete_sequence=colors,
                text='balance'
            )
            fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("👥 Top 10 Customers by AR Balance")
        top_customers = run_query("""
            SELECT 
                customer_name,
                SUM(current_balance) as balance
            FROM main_marts.fct_invoices
            WHERE status = 'Open'
            GROUP BY customer_name
            ORDER BY balance DESC
            LIMIT 10
        """)
        
        if not top_customers.empty:
            fig = px.bar(
                top_customers, 
                x='balance', 
                y='customer_name', 
                orientation='h',
                title='Top 10 Customers',
                labels={'balance': 'Balance ($)', 'customer_name': 'Customer'},
                color='balance',
                color_continuous_scale='Reds',
                text='balance'
            )
            fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Charts Row 2
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 AR by Customer Segment")
        segments = run_query("""
            SELECT 
                segment_name,
                SUM(current_balance) as balance,
                COUNT(*) as invoice_count
            FROM main_marts.fct_invoices
            WHERE status = 'Open'
            GROUP BY segment_name
        """)
        
        if not segments.empty:
            fig = px.pie(
                segments, 
                values='balance', 
                names='segment_name',
                title='AR Distribution by Segment',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🌎 AR by Region")
        regions = run_query("""
            SELECT 
                region_name,
                SUM(current_balance) as balance
            FROM main_marts.fct_invoices
            WHERE status = 'Open'
            GROUP BY region_name
            ORDER BY balance DESC
        """)
        
        if not regions.empty:
            fig = px.bar(
                regions, 
                x='region_name', 
                y='balance',
                title='AR by Geographic Region',
                labels={'balance': 'Balance ($)', 'region_name': 'Region'},
                color='balance',
                color_continuous_scale='Blues',
                text='balance'
            )
            fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# CUSTOMER ANALYSIS
# ============================================================================

elif page == "👥 Customer Analysis":
    st.header("👥 Customer Analysis")
    
    # Overview Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    customer_metrics = run_query("""
        SELECT 
            COUNT(*) as total_customers,
            SUM(total_ar_balance) as total_ar,
            AVG(credit_limit) as avg_credit,
            SUM(CASE WHEN risk_category = 'High Risk' THEN 1 ELSE 0 END) as high_risk_count
        FROM main_marts.dim_customers
    """)
    
    if not customer_metrics.empty:
        with col1:
            st.metric("Total Customers", f"{int(customer_metrics['total_customers'].iloc[0]):,}")
        with col2:
            st.metric("Total AR Balance", f"${customer_metrics['total_ar'].iloc[0]:,.0f}")
        with col3:
            st.metric("Avg Credit Limit", f"${customer_metrics['avg_credit'].iloc[0]:,.0f}")
        with col4:
            st.metric("High Risk Customers", f"{int(customer_metrics['high_risk_count'].iloc[0]):,}")
    
    st.markdown("---")
    
    # Customer Segment Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Customer Distribution by Segment")
        segments = run_query("""
            SELECT 
                segment_name,
                COUNT(*) as customer_count,
                SUM(credit_limit) as total_credit,
                SUM(credit_used) as total_used
            FROM main_marts.dim_customers
            GROUP BY segment_name
        """)
        
        if not segments.empty:
            fig = px.pie(
                segments, 
                values='customer_count', 
                names='segment_name',
                title='Customer Count by Segment',
                hole=0.5,
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💳 Credit Limit vs Usage by Segment")
        if not segments.empty:
            fig = go.Figure(data=[
                go.Bar(name='Credit Limit', x=segments['segment_name'], y=segments['total_credit'], marker_color='#3498db'),
                go.Bar(name='Credit Used', x=segments['segment_name'], y=segments['total_used'], marker_color='#e74c3c')
            ])
            fig.update_layout(
                barmode='group',
                title='Credit Limit vs Used',
                height=400,
                xaxis_title='Segment',
                yaxis_title='Amount ($)'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Risk Analysis
    st.subheader("⚠️ Customer Risk Analysis")
    risk_analysis = run_query("""
        SELECT 
            risk_category,
            COUNT(*) as customer_count,
            SUM(total_ar_balance) as total_ar,
            AVG(total_ar_balance) as avg_ar
        FROM main_marts.dim_customers
        WHERE total_ar_balance > 0
        GROUP BY risk_category
        ORDER BY 
            CASE risk_category
                WHEN 'High Risk' THEN 1
                WHEN 'Medium Risk' THEN 2
                WHEN 'Low Risk' THEN 3
            END
    """)
    
    if not risk_analysis.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            colors_risk = {'High Risk': '#e74c3c', 'Medium Risk': '#f39c12', 'Low Risk': '#27ae60'}
            fig = px.bar(
                risk_analysis, 
                x='risk_category', 
                y='customer_count',
                title='Customers by Risk Category',
                labels={'customer_count': 'Count', 'risk_category': 'Risk Level'},
                color='risk_category',
                color_discrete_map=colors_risk,
                text='customer_count'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                risk_analysis, 
                x='risk_category', 
                y='total_ar',
                title='AR Balance by Risk Category',
                labels={'total_ar': 'Balance ($)', 'risk_category': 'Risk Level'},
                color='risk_category',
                color_discrete_map=colors_risk,
                text='total_ar'
            )
            fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    # Top Customers Table
    st.markdown("---")
    st.subheader("🏆 Top 20 Customers by AR Balance")
    top_customers_detail = run_query("""
        SELECT 
            customer_name,
            segment_name,
            region_name,
            total_ar_balance as ar_balance,
            credit_limit,
            risk_category,
            open_invoice_count
        FROM main_marts.dim_customers
        WHERE total_ar_balance > 0
        ORDER BY total_ar_balance DESC
        LIMIT 20
    """)
    
    if not top_customers_detail.empty:
        # Format currency columns
        top_customers_detail['ar_balance'] = top_customers_detail['ar_balance'].apply(lambda x: f"${x:,.2f}")
        top_customers_detail['credit_limit'] = top_customers_detail['credit_limit'].apply(lambda x: f"${x:,.0f}")
        st.dataframe(top_customers_detail, use_container_width=True, height=500)

# ============================================================================
# COLLECTION WORKLIST
# ============================================================================

elif page == "📋 Collection Worklist":
    st.header("📋 Collection Worklist")
    
    st.markdown("### 🎯 Priority Collections - Past Due Accounts")
    
    collections = run_query("""
        SELECT 
            customer_name,
            segment_name,
            SUM(current_balance) as balance,
            MAX(days_past_due) as max_days,
            COUNT(*) as invoice_count,
            MAX(collection_priority_score) as priority_score
        FROM main_marts.fct_ar_aging
        WHERE days_past_due > 30
        GROUP BY customer_name, segment_name
        ORDER BY MAX(collection_priority_score) DESC, SUM(current_balance) DESC
        LIMIT 20
    """)
    
    if not collections.empty:
        # Format the dataframe
        collections['balance'] = collections['balance'].apply(lambda x: f"${x:,.2f}")
        collections['max_days'] = collections['max_days'].apply(lambda x: f"{int(x)} days")
        
        st.dataframe(
            collections,
            use_container_width=True, 
            height=400,
            column_config={
                "customer_name": "Customer",
                "segment_name": "Segment",
                "balance": "Outstanding Balance",
                "max_days": "Days Overdue",
                "invoice_count": "# Invoices",
                "priority_score": st.column_config.ProgressColumn(
                    "Priority Score",
                    help="Higher score = Higher priority",
                    format="%d",
                    min_value=0,
                    max_value=150,
                )
            }
        )
        
        # Collection Email Generator
        st.markdown("---")
        st.subheader("📧 AI Collection Email Generator")
        
        # Get original data for email generation
        collections_raw = run_query("""
            SELECT 
                customer_name,
                SUM(current_balance) as balance,
                MAX(days_past_due) as max_days,
                COUNT(*) as invoice_count
            FROM main_marts.fct_ar_aging
            WHERE days_past_due > 30
            GROUP BY customer_name
            ORDER BY SUM(current_balance) DESC
            LIMIT 20
        """)
        
        selected_customer = st.selectbox(
            "Select Customer for Email Generation", 
            collections_raw['customer_name'].tolist()
        )
        
        if selected_customer:
            customer_data = collections_raw[collections_raw['customer_name'] == selected_customer].iloc[0]
            
            col1, col2 = st.columns([3, 1])
            
            with col2:
                st.metric("Outstanding Balance", f"${customer_data['balance']:,.2f}")
                st.metric("Days Overdue", f"{int(customer_data['max_days'])}")
                st.metric("Open Invoices", f"{int(customer_data['invoice_count'])}")
            
            with col1:
                if st.button("🤖 Generate Collection Email", type="primary", use_container_width=True):
                    if GROQ_API_KEY:
                        with st.spinner("Generating personalized email..."):
                            email = generate_collection_email(
                                selected_customer,
                                customer_data['balance'],
                                customer_data['max_days']
                            )
                            st.success("Email Generated!")
                            st.text_area("📧 Collection Email", email, height=300)
                            st.download_button(
                                label="Download Email",
                                data=email,
                                file_name=f"collection_email_{selected_customer.replace(' ', '_')}.txt",
                                mime="text/plain"
                            )
                    else:
                        st.warning("⚠️ GROQ_API_KEY not configured. Add it to .env file or Streamlit secrets.")
    else:
        st.success("🎉 Excellent! No customers requiring collection action!")
        st.balloons()

# ============================================================================
# AI ASSISTANT
# ============================================================================

elif page == "🤖 AI Assistant":
    st.header("🤖 AI-Powered AR Query Assistant")
    
    if not GROQ_API_KEY:
        st.warning("⚠️ GROQ_API_KEY not found")
        st.info("""
        **To enable AI features:**
        1. Get a free API key from [console.groq.com](https://console.groq.com)
        2. Add to `.env` file: `GROQ_API_KEY=your_key_here`
        3. Or add to Streamlit Cloud secrets
        """)
    else:
        st.success("✅ AI Assistant Ready (Powered by Groq - Llama 3.3 70B)")
        
        # Example questions
        st.markdown("### 💡 Try These Example Questions:")
        
        example_cols = st.columns(3)
        examples = [
            "Show me invoices over 90 days past due",
            "Which customers have the highest outstanding balance?",
            "List all Enterprise segment customers",
            "Show me disputed invoices",
            "What's the total AR for Northeast region?",
            "Find High Risk category customers"
        ]
        
        for idx, example in enumerate(examples):
            with example_cols[idx % 3]:
                st.info(f"💬 _{example}_")
        
        st.markdown("---")
        
        # Main query interface
        st.markdown("### 🔍 Ask Your Question")
        
        question = st.text_area(
            "Enter your question about AR data:",
            placeholder="Example: Show me all customers with balance over $100,000 in the Enterprise segment",
            height=100,
            help="Ask natural language questions about invoices, customers, balances, aging, etc."
        )
        
        col1, col2 = st.columns([1, 4])
        with col1:
            ask_button = st.button("🚀 Ask AI", type="primary", use_container_width=True)
        
        if ask_button and question:
            with st.spinner("🤖 AI is thinking..."):
                # Schema information for better SQL generation
                schema_info = """
Available tables and key columns:

1. main_marts.fct_invoices:
   - invoice_number, customer_id, customer_name
   - segment_name (Enterprise, Mid-Market, Small Business, Startup)
   - region_name (Northeast, Southeast, Midwest, Southwest, West)
   - invoice_date, due_date, invoice_amount, current_balance
   - status (Open, Paid, Partial Payment, Disputed, Written Off)
   - aging_bucket (Current, 1-30 Days, 31-60 Days, 61-90 Days, 90+ Days)
   - days_outstanding, risk_category

2. main_marts.dim_customers:
   - customer_id, customer_name, segment_name, region_name
   - credit_limit, total_ar_balance, risk_category (High Risk, Medium Risk, Low Risk)
   - open_invoice_count

3. main_marts.fct_ar_aging:
   - Same as fct_invoices but only open invoices
   - days_past_due, collection_priority_score
"""
                
                prompt = f"""Based on this database schema:

{schema_info}

User question: "{question}"

Generate a valid DuckDB SQL query. Return ONLY the SQL query with no markdown formatting, no backticks, no code blocks, and no explanations. Just the raw SQL."""
                
                sql_query = query_llm(prompt, "You are a SQL expert. Generate clean, valid SQL queries only. No markdown, no backticks, no explanations.")
                
                # Clean the response
                sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
                if sql_query.startswith('sql'):
                    sql_query = sql_query[3:].strip()
                
                # Display generated SQL
                st.markdown("**🔧 Generated SQL Query:**")
                st.code(sql_query, language="sql")
                
                # Execute query
                try:
                    with st.spinner("Executing query..."):
                        result = run_query(sql_query)
                        
                        if not result.empty:
                            st.markdown(f"**📊 Results ({len(result)} rows):**")
                            st.dataframe(result, use_container_width=True, height=400)
                            
                            # AI interpretation
                            with st.spinner("AI is interpreting results..."):
                                result_preview = result.head(10).to_string()
                                interpretation_prompt = f"""Analyze these query results and provide a brief 2-3 sentence summary of key insights:

{result_preview}

Focus on: totals, trends, notable outliers, or actionable insights."""
                                
                                interpretation = query_llm(interpretation_prompt)
                                
                                st.markdown("**💡 AI Interpretation:**")
                                st.success(interpretation)
                                
                                # Download option
                                csv = result.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download Results as CSV",
                                    data=csv,
                                    file_name="query_results.csv",
                                    mime="text/csv"
                                )
                        else:
                            st.warning("⚠️ Query returned no results")
                            st.info("💡 Try rephrasing your question or using different criteria")
                            
                except Exception as e:
                    st.error(f"❌ Error executing query: {str(e)}")
                    st.info("💡 The AI-generated SQL might need adjustment. Try rephrasing your question.")
        
        elif ask_button and not question:
            st.warning("⚠️ Please enter a question")

# ============================================================================
# ABOUT SECTION
# ============================================================================
st.markdown("---")
with st.expander("ℹ️ **About This Platform**"):
    st.markdown("""
    ### 💰 Finance Modernization Platform
    
    **Purpose:**  
    A complete demonstration of modernizing legacy IBM AS400/iSeries financial systems into a modern cloud-native data platform.
    
    **Tech Stack:**
    - **Data Source**: Mock AS400 fixed-width files (ARMAS, CUSMAS, PAYTRAN, GLJRN)
    - **Database**: DuckDB (local development) → Production-ready for Snowflake/BigQuery
    - **Transformation**: dbt (Bronze → Silver → Gold medallion architecture)
    - **Orchestration**: Apache Airflow for ETL workflows
    - **ML Models**: scikit-learn & XGBoost for payment propensity and risk scoring
    - **AI/LLM**: Groq API with Llama 3.3 70B (FREE tier available!)
    - **Dashboard**: Streamlit with Plotly interactive visualizations
    - **CI/CD**: GitHub Actions for automated testing and deployment
    
    **Key Features:**
    - 📊 Real-time AR dashboards with comprehensive aging analysis
    - 👥 Customer segmentation, risk scoring, and credit analysis
    - 📋 Intelligent collection worklist with ML-based priority scoring
    - 🤖 AI-powered natural language query interface
    - 📧 Automated collection email generation with LLM
    - 🎯 ML models for payment propensity predictions
    - 📈 Interactive charts and KPI tracking
    
    **Data Flow:**
```
    AS400 Files → Python Parser → DuckDB → dbt Transformations → 
    ML Models → Streamlit Dashboard → AI-Powered Insights
```
    
    **Project Info:**
    - **Author**: Vignesh | Data Analytics Engineering @ Northeastern University
    - **Background**: AS400/IBM i, DB2, RPGLE, Modern Data Stack
    - **GitHub**: [github.com/Vignesh4110/finance-modernization](https://github.com/Vignesh4110/finance-modernization)
    - **License**: MIT
    
    ---
    *Built to showcase end-to-end data engineering skills and legacy system modernization expertise*
    """)

# ============================================================================
# FOOTER
# ============================================================================
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7f8c8d; padding: 20px;'>
    <p style='font-size: 0.9rem;'>
        <strong>Finance Modernization Platform</strong> | 
        Built with Streamlit, dbt, DuckDB & Groq AI<br>
        Transforming Legacy AS400/DB2 into Modern Data Architecture | 
        © 2026 Vignesh
    </p>
</div>
""", unsafe_allow_html=True)