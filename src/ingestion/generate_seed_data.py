"""
Generate clean seed data for dbt
Creates CSV files directly with proper values
"""

import random
from datetime import datetime, timedelta, date
from pathlib import Path
import csv

from faker import Faker

# Initialize
fake = Faker()
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = Path(__file__).parent.parent.parent / "dbt_project" / "seeds"

# Config
NUM_CUSTOMERS = 500
NUM_INVOICES = 5000
NUM_PAYMENTS = 4000

REGIONS = ["NE", "SE", "MW", "SW", "WE"]
INDUSTRIES = ["MFG", "HLT", "TEC", "RET", "CON"]
SEGMENTS = ["E", "M", "S", "T"]
SEGMENT_WEIGHTS = [0.1, 0.3, 0.45, 0.15]

def generate_customers():
    """Generate customer data"""
    print(f"Generating {NUM_CUSTOMERS} customers...")
    
    customers = []
    for i in range(NUM_CUSTOMERS):
        segment = random.choices(SEGMENTS, SEGMENT_WEIGHTS)[0]
        
        if segment == "E":
            credit_limit = random.randint(100000, 500000)
            terms = random.choice([30, 45, 60])
        elif segment == "M":
            credit_limit = random.randint(25000, 100000)
            terms = random.choice([30, 45])
        elif segment == "S":
            credit_limit = random.randint(5000, 25000)
            terms = random.choice([15, 30])
        else:
            credit_limit = random.randint(1000, 10000)
            terms = random.choice([15, 30])
        
        create_date = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 1, 1))
        
        customers.append({
            "customer_id": 100000 + i,
            "customer_name": fake.company()[:40],
            "contact_name": fake.name()[:30],
            "address_line1": fake.street_address()[:40],
            "address_line2": "",
            "city": fake.city()[:25],
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "phone": fake.msisdn()[:10],
            "email": fake.company_email()[:50],
            "region": random.choice(REGIONS),
            "industry_code": random.choice(INDUSTRIES),
            "segment": segment,
            "customer_type": "R",
            "credit_limit": credit_limit,
            "credit_used": 0,
            "payment_terms": terms,
            "credit_status": random.choices(["A", "H", "S"], [0.9, 0.07, 0.03])[0],
            "account_status": random.choices(["A", "I"], [0.95, 0.05])[0],
            "created_date": create_date,
            "updated_date": create_date,
            "updated_time": "120000",
            "updated_by": "SYSTEM"
        })
    
    return customers


def generate_invoices(customers):
    """Generate invoice data with realistic statuses"""
    print(f"Generating {NUM_INVOICES} invoices...")
    
    active_customers = [c for c in customers if c["account_status"] == "A"]
    invoices = []
    
    for i in range(NUM_INVOICES):
        customer = random.choice(active_customers)
        
        # Invoice date within last 2 years
        invoice_date = fake.date_between(start_date=date(2023, 1, 1), end_date=date(2024, 12, 31))
        due_date = invoice_date + timedelta(days=customer["payment_terms"])
        
        # Amount based on segment
        segment = customer["segment"]
        if segment == "E":
            amount = round(random.uniform(5000, 50000), 2)
        elif segment == "M":
            amount = round(random.uniform(1000, 10000), 2)
        elif segment == "S":
            amount = round(random.uniform(200, 2000), 2)
        else:
            amount = round(random.uniform(100, 1000), 2)
        
        tax = round(amount * 0.08, 2)
        total = amount + tax
        
        # Determine status based on age (relative to end of 2024)
        ref_date = date(2024, 12, 31)
        days_old = (ref_date - due_date).days
        
        # Status logic
        rand = random.random()
        if days_old < 0:
            # Not yet due
            status = "OP"
            paid = 0
            balance = total
        elif rand < 0.60:
            # Paid
            status = "PD"
            paid = total
            balance = 0
        elif rand < 0.75:
            # Partial payment
            status = "PP"
            paid = round(total * random.choice([0.25, 0.5, 0.75]), 2)
            balance = round(total - paid, 2)
        elif rand < 0.85:
            # Open (late)
            status = "OP"
            paid = 0
            balance = total
        elif rand < 0.92:
            # Disputed
            status = "DP"
            paid = 0
            balance = total
        else:
            # Written off
            status = "WO"
            paid = 0
            balance = 0
        
        invoices.append({
            "invoice_number": 1000000 + i,
            "customer_id": customer["customer_id"],
            "invoice_date": invoice_date,
            "due_date": due_date,
            "ship_date": invoice_date - timedelta(days=random.randint(1, 3)),
            "po_number": f"PO-{random.randint(10000, 99999)}",
            "reference1": fake.bs()[:30],
            "reference2": "",
            "invoice_amount": amount,
            "tax_amount": tax,
            "freight_amount": 0,
            "discount_amount": 0,
            "amount_paid": paid,
            "current_balance": balance,
            "status": status,
            "hold_flag": "N",
            "dispute_flag": "Y" if status == "DP" else "N",
            "dispute_reason": random.choice(["PRC", "DMG", "NRC"]) if status == "DP" else "",
            "payment_terms": customer["payment_terms"],
            "document_type": "IN",
            "division": "001",
            "gl_account": "1200",
            "gl_post_date": invoice_date,
            "gl_posted_flag": "Y",
            "created_date": invoice_date,
            "updated_date": invoice_date,
            "updated_time": "120000",
            "updated_by": "BATCH",
            "batch_session": random.randint(100000, 999999)
        })
    
    return invoices


def generate_payments(invoices, customers):
    """Generate payment data"""
    print(f"Generating payments...")
    
    cust_lookup = {c["customer_id"]: c for c in customers}
    
    # Get invoices with payments
    paid_invoices = [inv for inv in invoices if inv["amount_paid"] > 0]
    
    payments = []
    for i, inv in enumerate(paid_invoices):
        customer = cust_lookup.get(inv["customer_id"])
        if not customer:
            continue
        
        pay_date = inv["invoice_date"] + timedelta(days=random.randint(5, 60))
        if pay_date > date(2024, 12, 31):
            pay_date = date(2024, 12, 31)
        
        method = random.choices(["CK", "AC", "WR", "CC"], [0.35, 0.4, 0.15, 0.1])[0]
        
        payments.append({
            "payment_id": 500000 + i,
            "customer_id": customer["customer_id"],
            "payment_date": pay_date,
            "payment_amount": inv["amount_paid"],
            "payment_method": method,
            "check_number": str(random.randint(1000, 9999)) if method == "CK" else "",
            "bank_reference": f"REF{random.randint(100000, 999999)}" if method in ["AC", "WR"] else "",
            "remittance_name": customer["customer_name"],
            "invoice_reference": inv["invoice_number"] if random.random() > 0.2 else 0,
            "applied_flag": "Y" if random.random() > 0.15 else "N",
            "applied_date": pay_date if random.random() > 0.15 else None,
            "applied_amount": inv["amount_paid"] if random.random() > 0.15 else 0,
            "unapplied_amount": 0 if random.random() > 0.15 else inv["amount_paid"],
            "payment_type": "PM",
            "status": "AP" if random.random() > 0.15 else "RV",
            "batch_session": random.randint(100000, 999999),
            "batch_id": f"BATCH-{pay_date.strftime('%Y%m%d')}",
            "created_date": pay_date,
            "updated_date": pay_date,
            "updated_time": "120000",
            "updated_by": "BATCH"
        })
    
    print(f"  Generated {len(payments)} payments")
    return payments


def generate_gl_entries(invoices, payments):
    """Generate GL journal entries"""
    print("Generating GL entries...")
    
    entries = []
    journal_id = 1000000
    
    for inv in invoices:
        total = inv["invoice_amount"] + inv["tax_amount"]
        
        # Debit AR
        entries.append({
            "journal_id": journal_id,
            "line_number": 1,
            "post_date": inv["invoice_date"],
            "period": int(inv["invoice_date"].strftime("%Y%m")),
            "fiscal_year": inv["invoice_date"].year,
            "gl_account": "1200",
            "department": "0000",
            "project": "",
            "debit_amount": total,
            "credit_amount": 0,
            "description": f"Invoice {inv['invoice_number']}",
            "reference": str(inv["invoice_number"]),
            "source": "AR",
            "document_type": "INV",
            "status": "P",
            "reversal_flag": "N",
            "reversal_journal": 0,
            "created_date": inv["invoice_date"],
            "updated_date": inv["invoice_date"],
            "updated_time": "120000",
            "updated_by": "BATCH",
            "batch_session": inv["batch_session"]
        })
        
        # Credit Revenue
        entries.append({
            "journal_id": journal_id,
            "line_number": 2,
            "post_date": inv["invoice_date"],
            "period": int(inv["invoice_date"].strftime("%Y%m")),
            "fiscal_year": inv["invoice_date"].year,
            "gl_account": "4100",
            "department": "0000",
            "project": "",
            "debit_amount": 0,
            "credit_amount": total,
            "description": f"Invoice {inv['invoice_number']}",
            "reference": str(inv["invoice_number"]),
            "source": "AR",
            "document_type": "INV",
            "status": "P",
            "reversal_flag": "N",
            "reversal_journal": 0,
            "created_date": inv["invoice_date"],
            "updated_date": inv["invoice_date"],
            "updated_time": "120000",
            "updated_by": "BATCH",
            "batch_session": inv["batch_session"]
        })
        
        journal_id += 1
    
    # Payment entries
    for pmt in payments:
        if pmt["applied_flag"] == "Y":
            # Debit Cash
            entries.append({
                "journal_id": journal_id,
                "line_number": 1,
                "post_date": pmt["payment_date"],
                "period": int(pmt["payment_date"].strftime("%Y%m")),
                "fiscal_year": pmt["payment_date"].year,
                "gl_account": "1100",
                "department": "0000",
                "project": "",
                "debit_amount": pmt["payment_amount"],
                "credit_amount": 0,
                "description": f"Payment {pmt['payment_id']}",
                "reference": str(pmt["payment_id"]),
                "source": "AR",
                "document_type": "PMT",
                "status": "P",
                "reversal_flag": "N",
                "reversal_journal": 0,
                "created_date": pmt["payment_date"],
                "updated_date": pmt["payment_date"],
                "updated_time": "120000",
                "updated_by": "BATCH",
                "batch_session": pmt["batch_session"]
            })
            
            # Credit AR
            entries.append({
                "journal_id": journal_id,
                "line_number": 2,
                "post_date": pmt["payment_date"],
                "period": int(pmt["payment_date"].strftime("%Y%m")),
                "fiscal_year": pmt["payment_date"].year,
                "gl_account": "1200",
                "department": "0000",
                "project": "",
                "debit_amount": 0,
                "credit_amount": pmt["payment_amount"],
                "description": f"Payment {pmt['payment_id']}",
                "reference": str(pmt["payment_id"]),
                "source": "AR",
                "document_type": "PMT",
                "status": "P",
                "reversal_flag": "N",
                "reversal_journal": 0,
                "created_date": pmt["payment_date"],
                "updated_date": pmt["payment_date"],
                "updated_time": "120000",
                "updated_by": "BATCH",
                "batch_session": pmt["batch_session"]
            })
            
            journal_id += 1
    
    print(f"  Generated {len(entries)} GL entries")
    return entries


def write_csv(data, filename, fieldnames):
    """Write data to CSV"""
    filepath = OUTPUT_DIR / filename
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"  Written {len(data)} rows to {filepath}")


def main():
    print("="*50)
    print("Generating Clean Seed Data")
    print("="*50)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    customers = generate_customers()
    invoices = generate_invoices(customers)
    payments = generate_payments(invoices, customers)
    gl_entries = generate_gl_entries(invoices, payments)
    
    print("\nWriting CSV files...")
    
    write_csv(customers, "cusmas.csv", customers[0].keys())
    write_csv(invoices, "armas.csv", invoices[0].keys())
    write_csv(payments, "paytran.csv", payments[0].keys())
    write_csv(gl_entries, "gljrn.csv", gl_entries[0].keys())
    
    # Print summary
    print("\n" + "="*50)
    print("Summary")
    print("="*50)
    print(f"Customers: {len(customers)}")
    print(f"Invoices: {len(invoices)}")
    
    # Invoice status breakdown
    status_counts = {}
    for inv in invoices:
        status_counts[inv["status"]] = status_counts.get(inv["status"], 0) + 1
    print(f"Invoice statuses: {status_counts}")
    
    open_balance = sum(inv["current_balance"] for inv in invoices)
    print(f"Total Open AR: ${open_balance:,.2f}")
    
    print(f"Payments: {len(payments)}")
    print(f"GL Entries: {len(gl_entries)}")


if __name__ == "__main__":
    main()