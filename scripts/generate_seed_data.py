"""
Seed Data Generator for Finance Modernization Platform

Generates realistic AR data with proper aging distribution for dbt seeds.
Run this script to regenerate test data with current dates.

Usage:
    python scripts/generate_seed_data.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import random
from faker import Faker
import sys

# Setup
fake = Faker()
random.seed(42)
np.random.seed(42)

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEEDS_DIR = PROJECT_ROOT / "dbt_project" / "seeds"

# Date configuration - relative to TODAY for realistic aging
TODAY = datetime.now().date()
DATA_START = TODAY - timedelta(days=365)  # 1 year of history

# Business configuration
REGIONS = ["NE", "SE", "MW", "SW", "WE"]
INDUSTRIES = ["MFG", "HLT", "TEC", "RET", "FIN", "CON", "TRN", "PRO"]
SEGMENTS = {
    "E": {"name": "Enterprise", "credit_range": (100000, 500000), "terms": [30, 45, 60]},
    "M": {"name": "Mid-Market", "credit_range": (25000, 100000), "terms": [30, 45]},
    "S": {"name": "Small Business", "credit_range": (5000, 25000), "terms": [15, 30]},
    "T": {"name": "Startup", "credit_range": (1000, 10000), "terms": [15, 30]},
}

# Aging distribution for realistic AR
AGING_DISTRIBUTION = [
    (0.30, 0, 0),       # 30% Current (not past due)
    (0.25, 1, 30),      # 25% 1-30 days past due
    (0.20, 31, 60),     # 20% 31-60 days past due
    (0.15, 61, 90),     # 15% 61-90 days past due
    (0.10, 91, 180),    # 10% 90+ days past due
]


def generate_customers(num_customers: int = 500) -> pd.DataFrame:
    """Generate customer master data."""
    print(f"Generating {num_customers} customers...")
    
    customers = []
    for i in range(num_customers):
        segment_code = random.choice(list(SEGMENTS.keys()))
        segment = SEGMENTS[segment_code]
        
        customers.append({
            "customer_id": 100000 + i,
            "customer_name": fake.company(),
            "contact_name": fake.name(),
            "email": fake.company_email(),
            "address_line1": fake.street_address(),
            "address_line2": "",
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "region": random.choice(REGIONS),
            "industry_code": random.choice(INDUSTRIES),
            "segment": segment_code,
            "credit_limit": random.randint(*segment["credit_range"]),
            "credit_used": 0,
            "payment_terms": random.choice(segment["terms"]),
            "credit_status": random.choices(["A", "H", "S"], weights=[85, 10, 5])[0],
            "account_status": random.choices(["A", "I"], weights=[95, 5])[0],
            "created_date": (DATA_START - timedelta(days=random.randint(30, 365))).isoformat(),
            "updated_date": TODAY.isoformat(),
            "updated_by": "SYSTEM",
        })
    
    df = pd.DataFrame(customers)
    print(f"  ✓ Generated {len(df)} customers")
    return df


def generate_invoices(customers_df: pd.DataFrame, num_invoices: int = 5000) -> pd.DataFrame:
    """Generate invoice data with realistic aging distribution."""
    print(f"Generating {num_invoices} invoices...")
    
    customers = customers_df.to_dict("records")
    invoices = []
    
    for i in range(num_invoices):
        customer = random.choice(customers)
        
        # Determine aging bucket based on distribution
        rand = random.random()
        cumulative = 0
        days_past_due = 0
        
        for pct, min_days, max_days in AGING_DISTRIBUTION:
            cumulative += pct
            if rand <= cumulative:
                days_past_due = random.randint(min_days, max_days) if max_days > 0 else 0
                break
        
        # Calculate dates based on days past due
        due_date = TODAY - timedelta(days=days_past_due)
        invoice_date = due_date - timedelta(days=customer["payment_terms"])
        
        # Determine status and payment amounts
        if days_past_due == 0:
            # Current invoices - mix of paid and open
            if random.random() < 0.4:
                status = "PD"  # Paid
                amount_paid_pct = 1.0
            else:
                status = "OP"  # Open
                amount_paid_pct = 0.0
        else:
            # Past due - mostly open, some partial payments
            status = random.choices(["OP", "PP", "DP"], weights=[70, 20, 10])[0]
            if status == "PP":
                amount_paid_pct = random.uniform(0.3, 0.7)
            else:
                amount_paid_pct = 0.0
        
        invoice_amount = round(random.uniform(500, 50000), 2)
        amount_paid = round(invoice_amount * amount_paid_pct, 2)
        current_balance = round(invoice_amount - amount_paid, 2)
        
        invoices.append({
            "invoice_number": 1000000 + i,
            "customer_id": customer["customer_id"],
            "invoice_date": invoice_date.isoformat(),
            "due_date": due_date.isoformat(),
            "ship_date": (invoice_date - timedelta(days=random.randint(1, 3))).isoformat(),
            "po_number": f"PO-{random.randint(10000, 99999)}",
            "reference1": fake.bs()[:30],
            "reference2": "",
            "invoice_amount": invoice_amount,
            "tax_amount": round(invoice_amount * 0.08, 2),
            "freight_amount": round(random.uniform(0, 100), 2) if random.random() < 0.3 else 0,
            "discount_amount": 0,
            "amount_paid": amount_paid,
            "current_balance": current_balance,
            "status": status,
            "hold_flag": "N",
            "dispute_flag": "Y" if status == "DP" else "N",
            "dispute_reason": random.choice(["PRICE", "QUALITY", "SHIPPING"]) if status == "DP" else "",
            "payment_terms": customer["payment_terms"],
            "document_type": "INV",
            "division": random.choice(["01", "02", "03"]),
            "gl_account": "1200",
            "gl_post_date": invoice_date.isoformat(),
            "gl_posted_flag": "Y",
            "created_date": invoice_date.isoformat(),
            "updated_date": TODAY.isoformat(),
            "updated_time": "120000",
            "updated_by": "SYSTEM",
            "batch_session": random.randint(100000, 999999),
        })
    
    df = pd.DataFrame(invoices)
    
    # Print status distribution
    print(f"  ✓ Generated {len(df)} invoices")
    print(f"    Status distribution:")
    for status, count in df["status"].value_counts().items():
        print(f"      {status}: {count} ({count/len(df)*100:.1f}%)")
    
    return df


def generate_payments(invoices_df: pd.DataFrame) -> pd.DataFrame:
    """Generate payment data for paid/partial invoices."""
    print("Generating payments...")
    
    # Get invoices with payments
    paid_invoices = invoices_df[invoices_df["amount_paid"] > 0]
    
    payments = []
    for _, inv in paid_invoices.iterrows():
        due_date = datetime.fromisoformat(inv["due_date"]).date()
        payment_date = due_date - timedelta(days=random.randint(-10, 30))
        payment_date = min(payment_date, TODAY)
        
        payments.append({
            "payment_id": 2000000 + len(payments),
            "customer_id": inv["customer_id"],
            "invoice_reference": inv["invoice_number"],
            "payment_date": payment_date.isoformat(),
            "payment_amount": inv["amount_paid"],
            "payment_method": random.choice(["CK", "AC", "WR", "CC"]),
            "check_number": str(random.randint(1000, 9999)) if random.random() < 0.4 else "",
            "bank_reference": fake.uuid4()[:20],
            "remittance_name": "",
            "applied_flag": "Y",
            "applied_date": payment_date.isoformat(),
            "applied_amount": inv["amount_paid"],
            "unapplied_amount": 0,
            "payment_type": "PM",
            "status": "AP",
            "batch_id": f"B{random.randint(1000, 9999)}",
            "created_date": payment_date.isoformat(),
            "updated_date": TODAY.isoformat(),
            "updated_by": "SYSTEM",
        })
    
    df = pd.DataFrame(payments)
    print(f"  ✓ Generated {len(df)} payments")
    
    # Payment method distribution
    print(f"    Payment methods:")
    for method, count in df["payment_method"].value_counts().items():
        method_name = {"CK": "Check", "AC": "ACH", "WR": "Wire", "CC": "Credit Card"}.get(method, method)
        print(f"      {method_name}: {count}")
    
    return df


def generate_gl_entries(invoices_df: pd.DataFrame) -> pd.DataFrame:
    """Generate GL journal entries for invoices."""
    print("Generating GL entries...")
    
    gl_entries = []
    
    for _, inv in invoices_df.iterrows():
        post_date = datetime.fromisoformat(inv["invoice_date"]).date()
        base_id = 3000000 + len(gl_entries)
        
        # Debit AR (Account 1200)
        gl_entries.append({
            "journal_id": base_id,
            "line_number": 1,
            "post_date": post_date.isoformat(),
            "period": post_date.month,
            "fiscal_year": post_date.year,
            "gl_account": "1200",
            "department": inv["division"],
            "project": "",
            "debit_amount": inv["invoice_amount"],
            "credit_amount": 0,
            "description": f"Invoice {inv['invoice_number']}",
            "reference": str(inv["invoice_number"]),
            "source": "AR",
            "document_type": "INV",
            "status": "P",
            "reversal_flag": "N",
            "reversal_journal": None,
            "created_date": post_date.isoformat(),
            "updated_date": TODAY.isoformat(),
            "updated_by": "SYSTEM",
        })
        
        # Credit Revenue (Account 4100)
        gl_entries.append({
            "journal_id": base_id + 1,
            "line_number": 2,
            "post_date": post_date.isoformat(),
            "period": post_date.month,
            "fiscal_year": post_date.year,
            "gl_account": "4100",
            "department": inv["division"],
            "project": "",
            "debit_amount": 0,
            "credit_amount": inv["invoice_amount"],
            "description": f"Revenue - Invoice {inv['invoice_number']}",
            "reference": str(inv["invoice_number"]),
            "source": "AR",
            "document_type": "INV",
            "status": "P",
            "reversal_flag": "N",
            "reversal_journal": None,
            "created_date": post_date.isoformat(),
            "updated_date": TODAY.isoformat(),
            "updated_by": "SYSTEM",
        })
    
    df = pd.DataFrame(gl_entries)
    print(f"  ✓ Generated {len(df)} GL entries")
    
    return df


def main():
    """Main function to generate all seed data."""
    print("=" * 60)
    print("SEED DATA GENERATOR")
    print("=" * 60)
    print(f"Date Range: {DATA_START} to {TODAY}")
    print(f"Output Directory: {SEEDS_DIR}")
    print("=" * 60)
    print()
    
    # Ensure output directory exists
    SEEDS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generate data
    customers_df = generate_customers(500)
    invoices_df = generate_invoices(customers_df, 5000)
    payments_df = generate_payments(invoices_df)
    gl_df = generate_gl_entries(invoices_df)
    
    # Save to CSV
    print("\nSaving seed files...")
    customers_df.to_csv(SEEDS_DIR / "cusmas.csv", index=False)
    invoices_df.to_csv(SEEDS_DIR / "armas.csv", index=False)
    payments_df.to_csv(SEEDS_DIR / "paytran.csv", index=False)
    gl_df.to_csv(SEEDS_DIR / "gljrn.csv", index=False)
    
    print(f"  ✓ Saved cusmas.csv ({len(customers_df)} rows)")
    print(f"  ✓ Saved armas.csv ({len(invoices_df)} rows)")
    print(f"  ✓ Saved paytran.csv ({len(payments_df)} rows)")
    print(f"  ✓ Saved gljrn.csv ({len(gl_df)} rows)")
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    open_invoices = invoices_df[invoices_df["current_balance"] > 0]
    total_ar = open_invoices["current_balance"].sum()
    
    print(f"Total Customers: {len(customers_df)}")
    print(f"Total Invoices: {len(invoices_df)}")
    print(f"Open Invoices: {len(open_invoices)}")
    print(f"Total AR Balance: ${total_ar:,.2f}")
    print(f"Total Payments: {len(payments_df)}")
    print(f"Total GL Entries: {len(gl_df)}")
    
    print("\n✅ Seed data generation complete!")
    print("\nNext steps:")
    print("  1. cd dbt_project")
    print("  2. dbt seed")
    print("  3. dbt run")
    print("  4. Refresh Streamlit dashboard")


if __name__ == "__main__":
    main()