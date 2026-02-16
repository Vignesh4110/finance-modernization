"""
AR Query Agent - Natural Language Interface for AR Data
Uses Groq (Llama 3) for free, fast LLM inference
"""

import os
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import pandas as pd
import duckdb

# Try to import Groq
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    print("Warning: groq package not installed. Run: pip install groq")


class ARQueryAgent:
    """
    Natural language query agent for AR data.
    Converts questions to SQL and returns formatted answers.
    Uses Groq (Llama 3) for free LLM inference.
    """
    
    def __init__(self, db_path: Optional[str] = None, api_key: Optional[str] = None):
        """Initialize the AR Query Agent."""
        self.db_path = db_path or str(PROJECT_ROOT / "data" / "finance.duckdb")
        self.conn = duckdb.connect(self.db_path, read_only=True)
        
        # Initialize Groq client if available
        self.api_key = api_key or os.getenv("GROQ_API_KEY")
        if GROQ_AVAILABLE and self.api_key:
            self.client = Groq(api_key=self.api_key)
            self.demo_mode = False
        else:
            self.client = None
            self.demo_mode = True
            
        # Load schema information for context
        self.schema_info = self._get_schema_info()
        
    def _get_schema_info(self) -> str:
        """Get database schema for LLM context."""
        schema = """
Available Tables in the AR Database:

1. main_staging.stg_customers
   - customer_id (INTEGER): Unique customer identifier
   - customer_name (VARCHAR): Company name
   - segment_name (VARCHAR): Enterprise, Mid-Market, Small Business, Startup
   - credit_limit (DECIMAL): Credit limit amount
   - credit_used (DECIMAL): Current credit utilized
   - payment_terms_days (INTEGER): Payment terms in days
   - is_active (BOOLEAN): Whether customer is active
   
2. main_staging.stg_invoices
   - invoice_number (INTEGER): Unique invoice ID
   - customer_id (INTEGER): Links to customers
   - invoice_date (DATE): Date invoice was created
   - due_date (DATE): Payment due date
   - invoice_amount (DECIMAL): Original invoice amount
   - current_balance (DECIMAL): Outstanding balance
   - status (VARCHAR): Open, Paid, Partial Payment, Disputed
   - aging_bucket (VARCHAR): Current, 1-30 Days, 31-60 Days, 61-90 Days, 90+ Days
   - days_past_due (INTEGER): Days past the due date
   
3. main_staging.stg_payments
   - payment_id (INTEGER): Unique payment ID
   - customer_id (INTEGER): Links to customers
   - payment_date (DATE): Date payment received
   - payment_amount (DECIMAL): Payment amount
   - payment_method (VARCHAR): Check, ACH, Wire, Credit Card
   - is_applied (BOOLEAN): Whether payment was applied
   
4. main_marts.dim_customers
   - Customer dimension with aggregated metrics
   - total_invoices, total_invoice_amount, total_paid
   - current_ar_balance, avg_days_to_pay
   - risk_category: Low, Medium, High, Critical
   
5. main_marts.fct_ar_aging
   - AR aging fact table
   - aging_bucket, customer details, invoice details
   
6. main_marts.metrics_ar_summary
   - Summary metrics: open_invoice_count, total_ar_balance
   - Amounts by aging bucket
"""
        return schema
    
    def _generate_sql(self, question: str) -> str:
        """Generate SQL from natural language question using Groq."""
        if self.demo_mode:
            return self._demo_sql_generation(question)
            
        prompt = f"""You are a SQL expert. Convert the following natural language question 
into a DuckDB SQL query. Use only the tables and columns described in the schema below.

SCHEMA:
{self.schema_info}

RULES:
1. Return ONLY the SQL query, no explanation or markdown
2. Use proper table aliases
3. Format numbers with appropriate precision
4. Limit results to 20 rows unless asked for more
5. Use main_staging or main_marts schema prefixes
6. Do not wrap in ```sql``` tags

QUESTION: {question}

SQL:"""

        response = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a SQL expert. Return only valid SQL queries, no explanations."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        sql = response.choices[0].message.content.strip()
        # Clean up any markdown formatting
        sql = sql.replace("```sql", "").replace("```", "").strip()
        return sql
    
    def _demo_sql_generation(self, question: str) -> str:
        """Demo SQL generation without API calls."""
        question_lower = question.lower()
        
        if "top" in question_lower and "customer" in question_lower:
            return """
            SELECT customer_name, current_ar_balance, risk_category
            FROM main_marts.dim_customers
            ORDER BY current_ar_balance DESC
            LIMIT 10
            """
        elif "aging" in question_lower or "overdue" in question_lower:
            return """
            SELECT aging_bucket, COUNT(*) as invoice_count, 
                   SUM(current_balance) as total_balance
            FROM main_staging.stg_invoices
            WHERE current_balance > 0
            GROUP BY aging_bucket
            ORDER BY 
                CASE aging_bucket
                    WHEN 'Current' THEN 1
                    WHEN '1-30 Days' THEN 2
                    WHEN '31-60 Days' THEN 3
                    WHEN '61-90 Days' THEN 4
                    ELSE 5
                END
            """
        elif "total" in question_lower and ("ar" in question_lower or "receivable" in question_lower or "balance" in question_lower):
            return """
            SELECT 
                COUNT(*) as open_invoices,
                SUM(current_balance) as total_ar_balance,
                AVG(days_past_due) as avg_days_past_due
            FROM main_staging.stg_invoices
            WHERE current_balance > 0
            """
        elif "risk" in question_lower or "high risk" in question_lower:
            return """
            SELECT customer_name, current_ar_balance, 
                   avg_days_to_pay, risk_category
            FROM main_marts.dim_customers
            WHERE risk_category IN ('High', 'Critical')
            ORDER BY current_ar_balance DESC
            LIMIT 15
            """
        elif "payment" in question_lower and "method" in question_lower:
            return """
            SELECT payment_method, 
                   COUNT(*) as payment_count,
                   SUM(payment_amount) as total_amount
            FROM main_staging.stg_payments
            GROUP BY payment_method
            ORDER BY total_amount DESC
            """
        else:
            return """
            SELECT 
                (SELECT COUNT(*) FROM main_staging.stg_customers WHERE is_active) as active_customers,
                (SELECT COUNT(*) FROM main_staging.stg_invoices WHERE current_balance > 0) as open_invoices,
                (SELECT SUM(current_balance) FROM main_staging.stg_invoices WHERE current_balance > 0) as total_ar
            """
    
    def _format_answer(self, question: str, sql: str, results: pd.DataFrame) -> str:
        """Format the query results into a natural language answer."""
        if self.demo_mode:
            return self._demo_format_answer(question, results)
            
        prompt = f"""Based on the following SQL query results, provide a clear, 
concise answer to the user's question. Format numbers with commas and currency symbols where appropriate.

QUESTION: {question}

SQL QUERY: {sql}

RESULTS:
{results.to_string()}

Provide a brief, professional answer (2-4 sentences):"""

        response = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a financial analyst providing clear, concise answers about AR data."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=300
        )
        
        return response.choices[0].message.content.strip()
    
    def _demo_format_answer(self, question: str, results: pd.DataFrame) -> str:
        """Demo answer formatting without API calls."""
        if results.empty:
            return "No data found for your query."
            
        # Format based on result structure
        answer_parts = [f"Based on the AR data:\n"]
        
        for col in results.columns:
            if results[col].dtype in ['float64', 'int64']:
                if 'balance' in col.lower() or 'amount' in col.lower():
                    val = results[col].iloc[0] if len(results) == 1 else results[col].sum()
                    answer_parts.append(f"- {col.replace('_', ' ').title()}: ${val:,.2f}")
                else:
                    val = results[col].iloc[0] if len(results) == 1 else results[col].sum()
                    answer_parts.append(f"- {col.replace('_', ' ').title()}: {val:,.0f}")
        
        if len(results) > 1:
            answer_parts.append(f"\nShowing {len(results)} records.")
            
        return "\n".join(answer_parts)
    
    def ask(self, question: str) -> dict:
        """
        Ask a natural language question about AR data.
        
        Returns dict with: question, sql, results (DataFrame), answer
        """
        print(f"\n{'='*60}")
        print(f"QUESTION: {question}")
        print(f"{'='*60}")
        
        # Generate SQL
        try:
            sql = self._generate_sql(question)
            print(f"\nGenerated SQL:\n{sql}")
        except Exception as e:
            return {
                "question": question,
                "sql": None,
                "results": None,
                "answer": f"Error generating SQL: {str(e)}",
                "error": True
            }
        
        # Execute query
        try:
            results = self.conn.execute(sql).fetchdf()
            print(f"\nResults: {len(results)} rows")
        except Exception as e:
            return {
                "question": question,
                "sql": sql,
                "results": None,
                "answer": f"Error executing query: {str(e)}",
                "error": True
            }
        
        # Format answer
        try:
            answer = self._format_answer(question, sql, results)
        except Exception as e:
            answer = f"Query executed successfully but error formatting answer: {str(e)}\n\nRaw results:\n{results.to_string()}"
        
        print(f"\nANSWER:\n{answer}")
        
        return {
            "question": question,
            "sql": sql,
            "results": results,
            "answer": answer,
            "error": False
        }
    
    def close(self):
        """Close database connection."""
        self.conn.close()


def demo_ar_query_agent():
    """Demonstrate the AR Query Agent capabilities."""
    print("\n" + "="*70)
    print("AR QUERY AGENT - Natural Language Interface for AR Data")
    print("="*70)
    
    agent = ARQueryAgent()
    
    if agent.demo_mode:
        print("\n⚠️  Running in DEMO MODE (no API key)")
        print("   Set GROQ_API_KEY environment variable for AI-powered queries")
    else:
        print("\n✅ Running with Groq (Llama 3.1 70B)")
    
    # Sample questions
    questions = [
        "What is our total accounts receivable balance?",
        "Show me the top 10 customers by AR balance",
        "What is the aging breakdown of our receivables?",
        "Which customers are high risk?",
        "What payment methods do our customers use most?"
    ]
    
    for question in questions:
        result = agent.ask(question)
        print("\n" + "-"*60)
        input("Press Enter to continue to next question...")
    
    agent.close()
    print("\n✅ AR Query Agent demo complete!")


if __name__ == "__main__":
    demo_ar_query_agent()