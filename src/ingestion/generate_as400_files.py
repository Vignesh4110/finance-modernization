"""
AS400 Fixed-Width File Generator

Generates authentic AS400-style data files including:
- Fixed-width record layouts
- Packed decimal representation (displayed as numeric strings)
- CYYMMDD date format (IBM century format)
- Right-justified numerics, left-justified text

These files simulate what CPYTOIMPF produces when extracting from AS400.

Author: Vignesh
"""

import random
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Tuple
import sys

from faker import Faker

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Initialize Faker
fake = Faker()
RANDOM_SEED = 42
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "mock_as400" / "physical_files"

# =============================================================================
# AS400 DATA FORMATTING UTILITIES
# =============================================================================

def date_to_cyymmdd(d: date) -> str:
    """
    Convert Python date to IBM CYYMMDD format.
    C = 0 for 1900s, 1 for 2000s
    Example: 2024-01-15 -> 1240115
             1999-12-31 -> 0991231
    """
    if d is None:
        return "0000000"
    
    century = 1 if d.year >= 2000 else 0
    year_part = d.year - (1900 if century == 0 else 2000)
    return f"{century}{year_part:02d}{d.month:02d}{d.day:02d}"


def time_to_hhmmss(t: datetime = None) -> str:
    """Convert time to HHMMSS format"""
    if t is None:
        t = datetime.now()
    return f"{t.hour:02d}{t.minute:02d}{t.second:02d}"


def format_packed_decimal(value: float, total_digits: int, decimal_places: int) -> str:
    """
    Format a number as it appears when packed decimal is exported to text.
    In CPYTOIMPF output, packed decimals appear as right-justified numbers.
    
    Example: 12345.67 with (11,2) -> "00001234567" (no decimal point, implied)
    
    For simplicity in parsing, we'll include the decimal point in our simulation,
    but maintain fixed width with right justification.
    """
    if value is None:
        value = 0
    
    # Format with decimal places
    formatted = f"{value:.{decimal_places}f}"
    
    # Right justify to total width (including decimal point)
    width = total_digits + (1 if decimal_places > 0 else 0)
    return formatted.rjust(width)


def format_char(value: str, length: int) -> str:
    """Format character field - left justified, space padded"""
    if value is None:
        value = ""
    return value[:length].ljust(length)


def format_numeric_char(value: int, length: int) -> str:
    """Format numeric as character - right justified, zero padded"""
    if value is None:
        value = 0
    return str(value).zfill(length)


# =============================================================================
# RECORD LAYOUT DEFINITIONS (matching DDS)
# =============================================================================

# CUSMAS - Customer Master (350 bytes)
CUSMAS_LAYOUT = [
    ("CMCUST", 6, "packed", (6, 0)),      # Customer Number
    ("CMNAME", 40, "char", None),          # Customer Name
    ("CMCONT", 30, "char", None),          # Contact Name
    ("CMADR1", 40, "char", None),          # Address Line 1
    ("CMADR2", 40, "char", None),          # Address Line 2
    ("CMCITY", 25, "char", None),          # City
    ("CMSTAT", 2, "char", None),           # State
    ("CMZIPC", 10, "char", None),          # Zip Code
    ("CMPHON", 10, "packed", (10, 0)),     # Phone
    ("CMEMAL", 50, "char", None),          # Email
    ("CMREGN", 2, "char", None),           # Region
    ("CMINDS", 3, "char", None),           # Industry
    ("CMSEGM", 1, "char", None),           # Segment
    ("CMTYPE", 1, "char", None),           # Type
    ("CMCRLT", 14, "packed", (11, 2)),     # Credit Limit
    ("CMCRUS", 14, "packed", (11, 2)),     # Credit Used
    ("CMPMTM", 3, "packed", (3, 0)),       # Payment Terms
    ("CMCRST", 1, "char", None),           # Credit Status
    ("CMSTAT2", 1, "char", None),          # Account Status
    ("CMCDAT", 7, "date", None),           # Create Date (CYYMMDD)
    ("CMUDAT", 7, "date", None),           # Update Date (CYYMMDD)
    ("CMUTIM", 6, "time", None),           # Update Time (HHMMSS)
    ("CMUUSR", 10, "char", None),          # Update User
]

# ARMAS - Invoice Master (400 bytes)
ARMAS_LAYOUT = [
    ("AMINVN", 9, "packed", (9, 0)),       # Invoice Number
    ("AMCUST", 6, "packed", (6, 0)),       # Customer Number
    ("AMINVD", 7, "date", None),           # Invoice Date
    ("AMDUED", 7, "date", None),           # Due Date
    ("AMSHPD", 7, "date", None),           # Ship Date
    ("AMPONM", 20, "char", None),          # PO Number
    ("AMREF1", 30, "char", None),          # Reference 1
    ("AMREF2", 30, "char", None),          # Reference 2
    ("AMINVA", 14, "packed", (11, 2)),     # Invoice Amount
    ("AMTAXA", 12, "packed", (9, 2)),      # Tax Amount
    ("AMFRTA", 12, "packed", (9, 2)),      # Freight Amount
    ("AMDISA", 12, "packed", (9, 2)),      # Discount Amount
    ("AMPAID", 14, "packed", (11, 2)),     # Amount Paid
    ("AMCURB", 14, "packed", (11, 2)),     # Current Balance
    ("AMSTAT", 2, "char", None),           # Status
    ("AMHOLD", 1, "char", None),           # Hold Flag
    ("AMDISP", 1, "char", None),           # Dispute Flag
    ("AMDRSN", 3, "char", None),           # Dispute Reason
    ("AMTERM", 3, "packed", (3, 0)),       # Payment Terms
    ("AMTYPE", 2, "char", None),           # Document Type
    ("AMDIVN", 3, "char", None),           # Division
    ("AMGLAC", 10, "char", None),          # GL Account
    ("AMGLDT", 7, "date", None),           # GL Post Date
    ("AMGLFL", 1, "char", None),           # GL Posted Flag
    ("AMCDAT", 7, "date", None),           # Create Date
    ("AMUDAT", 7, "date", None),           # Update Date
    ("AMUTIM", 6, "time", None),           # Update Time
    ("AMUUSR", 10, "char", None),          # Update User
    ("AMBESSION", 9, "packed", (9, 0)),    # Batch Session
]

# PAYTRAN - Payment Transactions (300 bytes)
PAYTRAN_LAYOUT = [
    ("PTPAYID", 9, "packed", (9, 0)),      # Payment ID
    ("PTCUST", 6, "packed", (6, 0)),       # Customer Number
    ("PTPAYDT", 7, "date", None),          # Payment Date
    ("PTPAYAM", 14, "packed", (11, 2)),    # Payment Amount
    ("PTPAYMTH", 2, "char", None),         # Payment Method
    ("PTCKNUM", 15, "char", None),         # Check Number
    ("PTBNKRF", 25, "char", None),         # Bank Reference
    ("PTREMIT", 40, "char", None),         # Remittance Name
    ("PTINVRF", 9, "packed", (9, 0)),      # Invoice Reference
    ("PTAPFLG", 1, "char", None),          # Applied Flag
    ("PTAPDAT", 7, "date", None),          # Applied Date
    ("PTAPAMT", 14, "packed", (11, 2)),    # Applied Amount
    ("PTUNAPP", 14, "packed", (11, 2)),    # Unapplied Amount
    ("PTTYPE", 2, "char", None),           # Type
    ("PTSTAT", 2, "char", None),           # Status
    ("PTBESSION", 9, "packed", (9, 0)),    # Batch Session
    ("PTBATCH", 15, "char", None),         # Batch ID
    ("PTCDAT", 7, "date", None),           # Create Date
    ("PTUDAT", 7, "date", None),           # Update Date
    ("PTUTIM", 6, "time", None),           # Update Time
    ("PTUUSR", 10, "char", None),          # Update User
]

# GLJRN - GL Journal (250 bytes)
GLJRN_LAYOUT = [
    ("GLJRNID", 10, "packed", (10, 0)),    # Journal ID
    ("GLJRNLN", 5, "packed", (5, 0)),      # Line Number
    ("GLPOST", 7, "date", None),           # Post Date
    ("GLPERD", 6, "packed", (6, 0)),       # Period (YYYYMM)
    ("GLFYEAR", 4, "packed", (4, 0)),      # Fiscal Year
    ("GLACCT", 10, "char", None),          # GL Account
    ("GLDEPT", 4, "char", None),           # Department
    ("GLPROJ", 6, "char", None),           # Project
    ("GLDRAM", 16, "packed", (13, 2)),     # Debit Amount
    ("GLCRAM", 16, "packed", (13, 2)),     # Credit Amount
    ("GLDESC", 50, "char", None),          # Description
    ("GLREF", 20, "char", None),           # Reference
    ("GLSRC", 2, "char", None),            # Source
    ("GLDOCTY", 3, "char", None),          # Document Type
    ("GLSTAT", 1, "char", None),           # Status
    ("GLRVFL", 1, "char", None),           # Reversal Flag
    ("GLRVJN", 10, "packed", (10, 0)),     # Reversal Journal
    ("GLCDAT", 7, "date", None),           # Create Date
    ("GLUCDAT", 7, "date", None),          # Update Date
    ("GLUCTIM", 6, "time", None),          # Update Time
    ("GLUCUSR", 10, "char", None),         # Update User
    ("GLBESSION", 9, "packed", (9, 0)),    # Batch Session
]


def format_record(data: dict, layout: list) -> str:
    """Format a data dictionary into a fixed-width record based on layout"""
    record = ""
    for field_name, width, field_type, decimal_spec in layout:
        value = data.get(field_name)
        
        if field_type == "char":
            record += format_char(value, width)
        elif field_type == "packed":
            total_digits, decimal_places = decimal_spec
            record += format_packed_decimal(value, total_digits, decimal_places)
        elif field_type == "date":
            if isinstance(value, (date, datetime)):
                record += date_to_cyymmdd(value)
            elif value:
                record += str(value).zfill(7)
            else:
                record += "0000000"
        elif field_type == "time":
            if isinstance(value, datetime):
                record += time_to_hhmmss(value)
            elif value:
                record += str(value).zfill(6)
            else:
                record += "000000"
    
    return record


# =============================================================================
# DATA GENERATORS
# =============================================================================

# Configuration
NUM_CUSTOMERS = 500
NUM_INVOICES = 5000
NUM_PAYMENTS = 4000
DATA_START = date(2023, 1, 1)
DATA_END = date(2024, 12, 31)

REGIONS = ["NE", "SE", "MW", "SW", "WE"]
INDUSTRIES = ["MFG", "HLT", "TEC", "RET", "CON", "TRN", "FIN", "PRO", "HOS", "ENR"]
SEGMENTS = {"E": (100000, 500000, [30, 45, 60]),   # Enterprise
            "M": (25000, 100000, [30, 45]),         # Mid-market
            "S": (5000, 25000, [15, 30]),           # Small
            "T": (1000, 10000, [15, 30])}           # Startup

SEGMENT_WEIGHTS = [0.10, 0.30, 0.45, 0.15]
SEGMENT_KEYS = ["E", "M", "S", "T"]

PAYMENT_METHODS = ["CK", "AC", "WR", "CC"]
PAYMENT_WEIGHTS = [0.35, 0.40, 0.15, 0.10]

USERS = ["JSMITH", "MWILSON", "KJOHNSON", "PBROWN", "SYSTEM", "BATCH"]


def generate_customers() -> list:
    """Generate customer records"""
    print(f"Generating {NUM_CUSTOMERS} customers...")
    customers = []
    
    for i in range(NUM_CUSTOMERS):
        segment = random.choices(SEGMENT_KEYS, SEGMENT_WEIGHTS)[0]
        seg_config = SEGMENTS[segment]
        
        cust_id = 100000 + i
        create_date = fake.date_between(start_date=DATA_START - timedelta(days=1000), 
                                        end_date=DATA_START)
        
        customer = {
            "CMCUST": cust_id,
            "CMNAME": fake.company()[:40],
            "CMCONT": fake.name()[:30],
            "CMADR1": fake.street_address()[:40],
            "CMADR2": fake.secondary_address()[:40] if random.random() > 0.7 else "",
            "CMCITY": fake.city()[:25],
            "CMSTAT": fake.state_abbr(),
            "CMZIPC": fake.zipcode()[:10],
            "CMPHON": int(fake.msisdn()[:10]),
            "CMEMAL": fake.company_email()[:50],
            "CMREGN": random.choice(REGIONS),
            "CMINDS": random.choice(INDUSTRIES),
            "CMSEGM": segment,
            "CMTYPE": random.choices(["R", "G", "I"], [0.90, 0.07, 0.03])[0],
            "CMCRLT": random.randint(seg_config[0], seg_config[1]),
            "CMCRUS": 0,  # Will be updated based on invoices
            "CMPMTM": random.choice(seg_config[2]),
            "CMCRST": random.choices(["A", "H", "S"], [0.90, 0.07, 0.03])[0],
            "CMSTAT2": random.choices(["A", "I", "C"], [0.95, 0.04, 0.01])[0],
            "CMCDAT": create_date,
            "CMUDAT": fake.date_between(start_date=create_date, end_date=DATA_END),
            "CMUTIM": fake.date_time(),
            "CMUUSR": random.choice(USERS),
        }
        customers.append(customer)
    
    return customers


def generate_invoices(customers: list) -> list:
    """Generate invoice records"""
    print(f"Generating {NUM_INVOICES} invoices...")
    invoices = []
    
    active_customers = [c for c in customers if c["CMSTAT2"] == "A"]
    
    for i in range(NUM_INVOICES):
        customer = random.choice(active_customers)
        segment = customer["CMSEGM"]
        seg_config = SEGMENTS[segment]
        
        inv_date = fake.date_between(start_date=DATA_START, end_date=DATA_END)
        due_date = inv_date + timedelta(days=customer["CMPMTM"])
        
        # Base invoice amount based on segment
        if segment == "E":
            amount = round(random.uniform(5000, 50000), 2)
        elif segment == "M":
            amount = round(random.uniform(1000, 10000), 2)
        elif segment == "S":
            amount = round(random.uniform(200, 2000), 2)
        else:
            amount = round(random.uniform(100, 1000), 2)
        
        tax = round(amount * random.uniform(0, 0.10), 2)
        freight = round(random.uniform(0, 100), 2) if random.random() > 0.7 else 0
        
        # Determine status based on age
        days_past_due = (DATA_END - due_date).days
        
        if days_past_due < 0:
            status, paid, balance = "OP", 0, amount + tax + freight
        elif random.random() < 0.65:
            status, paid, balance = "PD", amount + tax + freight, 0
        elif random.random() < 0.15:
            partial = round((amount + tax + freight) * random.choice([0.25, 0.5, 0.75]), 2)
            status, paid, balance = "PP", partial, amount + tax + freight - partial
        elif random.random() < 0.05:
            status, paid, balance = "DP", 0, amount + tax + freight
        else:
            status, paid, balance = "OP", 0, amount + tax + freight
        
        invoice = {
            "AMINVN": 1000000 + i,
            "AMCUST": customer["CMCUST"],
            "AMINVD": inv_date,
            "AMDUED": due_date,
            "AMSHPD": inv_date - timedelta(days=random.randint(1, 5)),
            "AMPONM": f"PO-{random.randint(10000, 99999)}" if random.random() > 0.3 else "",
            "AMREF1": fake.bs()[:30],
            "AMREF2": "",
            "AMINVA": amount,
            "AMTAXA": tax,
            "AMFRTA": freight,
            "AMDISA": 0,
            "AMPAID": paid,
            "AMCURB": balance,
            "AMSTAT": status,
            "AMHOLD": "N",
            "AMDISP": "Y" if status == "DP" else "N",
            "AMDRSN": random.choice(["PRC", "NRC", "DMG", "WRG", "DUP", ""]) if status == "DP" else "",
            "AMTERM": customer["CMPMTM"],
            "AMTYPE": "IN",
            "AMDIVN": "001",
            "AMGLAC": "1200",
            "AMGLDT": inv_date,
            "AMGLFL": "Y",
            "AMCDAT": inv_date,
            "AMUDAT": fake.date_between(start_date=inv_date, end_date=DATA_END),
            "AMUTIM": fake.date_time(),
            "AMUUSR": random.choice(USERS),
            "AMBESSION": random.randint(100000, 999999),
        }
        invoices.append(invoice)
    
    return invoices


def generate_payments(invoices: list, customers: list) -> list:
    """Generate payment records"""
    print(f"Generating {NUM_PAYMENTS} payments...")
    payments = []
    
    # Get invoices that have payments
    paid_invoices = [inv for inv in invoices if inv["AMPAID"] > 0]
    
    cust_lookup = {c["CMCUST"]: c for c in customers}
    
    for i, inv in enumerate(paid_invoices[:NUM_PAYMENTS]):
        customer = cust_lookup.get(inv["AMCUST"])
        if not customer:
            continue
        
        pay_date = fake.date_between(
            start_date=inv["AMINVD"],
            end_date=min(inv["AMINVD"] + timedelta(days=120), DATA_END)
        )
        
        method = random.choices(PAYMENT_METHODS, PAYMENT_WEIGHTS)[0]
        
        # Simulate remittance name variations (cash application challenge)
        remit_name = customer["CMNAME"]
        if random.random() < 0.15:
            variations = [
                remit_name.replace(" INC", "").replace(" LLC", "").strip()[:40],
                remit_name.upper()[:40],
                remit_name.split()[0][:40] if " " in remit_name else remit_name[:40],
            ]
            remit_name = random.choice(variations)
        
        payment = {
            "PTPAYID": 500000 + i,
            "PTCUST": customer["CMCUST"],
            "PTPAYDT": pay_date,
            "PTPAYAM": inv["AMPAID"],
            "PTPAYMTH": method,
            "PTCKNUM": str(random.randint(1000, 9999)) if method == "CK" else "",
            "PTBNKRF": f"REF{random.randint(100000000, 999999999)}" if method in ["AC", "WR"] else "",
            "PTREMIT": remit_name[:40],
            "PTINVRF": inv["AMINVN"] if random.random() > 0.2 else 0,
            "PTAPFLG": "Y" if random.random() > 0.15 else "N",
            "PTAPDAT": pay_date if random.random() > 0.15 else None,
            "PTAPAMT": inv["AMPAID"] if random.random() > 0.15 else 0,
            "PTUNAPP": 0 if random.random() > 0.15 else inv["AMPAID"],
            "PTTYPE": "PM",
            "PTSTAT": "AP" if random.random() > 0.15 else "RV",
            "PTBESSION": random.randint(100000, 999999),
            "PTBATCH": f"BATCH-{pay_date.strftime('%Y%m%d')}-{random.randint(1, 5)}",
            "PTCDAT": pay_date,
            "PTUDAT": pay_date,
            "PTUTIM": fake.date_time(),
            "PTUUSR": random.choice(USERS),
        }
        payments.append(payment)
    
    return payments


def generate_gl_entries(invoices: list, payments: list) -> list:
    """Generate GL journal entries"""
    print("Generating GL journal entries...")
    gl_entries = []
    journal_id = 1000000
    
    # Invoice entries
    for inv in invoices:
        # Debit AR
        gl_entries.append({
            "GLJRNID": journal_id,
            "GLJRNLN": 1,
            "GLPOST": inv["AMINVD"],
            "GLPERD": int(inv["AMINVD"].strftime("%Y%m")),
            "GLFYEAR": inv["AMINVD"].year,
            "GLACCT": "1200",
            "GLDEPT": "0000",
            "GLPROJ": "",
            "GLDRAM": inv["AMINVA"] + inv["AMTAXA"] + inv["AMFRTA"],
            "GLCRAM": 0,
            "GLDESC": f"Invoice {inv['AMINVN']}"[:50],
            "GLREF": str(inv["AMINVN"]),
            "GLSRC": "AR",
            "GLDOCTY": "INV",
            "GLSTAT": "P",
            "GLRVFL": "N",
            "GLRVJN": 0,
            "GLCDAT": inv["AMINVD"],
            "GLUCDAT": inv["AMINVD"],
            "GLUCTIM": fake.date_time(),
            "GLUCUSR": "BATCH",
            "GLBESSION": inv["AMBESSION"],
        })
        
        # Credit Revenue
        gl_entries.append({
            "GLJRNID": journal_id,
            "GLJRNLN": 2,
            "GLPOST": inv["AMINVD"],
            "GLPERD": int(inv["AMINVD"].strftime("%Y%m")),
            "GLFYEAR": inv["AMINVD"].year,
            "GLACCT": "4100",
            "GLDEPT": "0000",
            "GLPROJ": "",
            "GLDRAM": 0,
            "GLCRAM": inv["AMINVA"] + inv["AMTAXA"] + inv["AMFRTA"],
            "GLDESC": f"Invoice {inv['AMINVN']}"[:50],
            "GLREF": str(inv["AMINVN"]),
            "GLSRC": "AR",
            "GLDOCTY": "INV",
            "GLSTAT": "P",
            "GLRVFL": "N",
            "GLRVJN": 0,
            "GLCDAT": inv["AMINVD"],
            "GLUCDAT": inv["AMINVD"],
            "GLUCTIM": fake.date_time(),
            "GLUCUSR": "BATCH",
            "GLBESSION": inv["AMBESSION"],
        })
        
        journal_id += 1
    
    # Payment entries
    for pmt in payments:
        if pmt["PTAPFLG"] == "Y":
            # Debit Cash
            gl_entries.append({
                "GLJRNID": journal_id,
                "GLJRNLN": 1,
                "GLPOST": pmt["PTPAYDT"],
                "GLPERD": int(pmt["PTPAYDT"].strftime("%Y%m")),
                "GLFYEAR": pmt["PTPAYDT"].year,
                "GLACCT": "1100",
                "GLDEPT": "0000",
                "GLPROJ": "",
                "GLDRAM": pmt["PTPAYAM"],
                "GLCRAM": 0,
                "GLDESC": f"Payment {pmt['PTPAYID']}"[:50],
                "GLREF": str(pmt["PTPAYID"]),
                "GLSRC": "AR",
                "GLDOCTY": "PMT",
                "GLSTAT": "P",
                "GLRVFL": "N",
                "GLRVJN": 0,
                "GLCDAT": pmt["PTPAYDT"],
                "GLUCDAT": pmt["PTPAYDT"],
                "GLUCTIM": fake.date_time(),
                "GLUCUSR": "BATCH",
                "GLBESSION": pmt["PTBESSION"],
            })
            
            # Credit AR
            gl_entries.append({
                "GLJRNID": journal_id,
                "GLJRNLN": 2,
                "GLPOST": pmt["PTPAYDT"],
                "GLPERD": int(pmt["PTPAYDT"].strftime("%Y%m")),
                "GLFYEAR": pmt["PTPAYDT"].year,
                "GLACCT": "1200",
                "GLDEPT": "0000",
                "GLPROJ": "",
                "GLDRAM": 0,
                "GLCRAM": pmt["PTPAYAM"],
                "GLDESC": f"Payment {pmt['PTPAYID']}"[:50],
                "GLREF": str(pmt["PTPAYID"]),
                "GLSRC": "AR",
                "GLDOCTY": "PMT",
                "GLSTAT": "P",
                "GLRVFL": "N",
                "GLRVJN": 0,
                "GLCDAT": pmt["PTPAYDT"],
                "GLUCDAT": pmt["PTPAYDT"],
                "GLUCTIM": fake.date_time(),
                "GLUCUSR": "BATCH",
                "GLBESSION": pmt["PTBESSION"],
            })
            
            journal_id += 1
    
    return gl_entries


def write_fixed_width_file(records: list, layout: list, filename: str):
    """Write records to a fixed-width file"""
    filepath = OUTPUT_DIR / filename
    
    with open(filepath, 'w', encoding='ascii', errors='replace') as f:
        for record in records:
            line = format_record(record, layout)
            f.write(line + '\r\n')  # CRLF line ending (AS400 style)
    
    print(f"  Written {len(records)} records to {filepath}")
    print(f"  Record length: {len(format_record(records[0], layout))} characters")


def write_copybook(layout: list, filename: str):
    """Write a field position reference file"""
    filepath = OUTPUT_DIR.parent / "copybooks" / filename
    
    with open(filepath, 'w') as f:
        f.write(f"{'Field':<12} {'Start':>6} {'End':>6} {'Len':>5} {'Type':<8} {'Decimal':<8}\n")
        f.write("=" * 60 + "\n")
        
        position = 1
        for field_name, width, field_type, decimal_spec in layout:
            end_pos = position + width - 1
            dec_str = f"({decimal_spec[0]},{decimal_spec[1]})" if decimal_spec else ""
            f.write(f"{field_name:<12} {position:>6} {end_pos:>6} {width:>5} {field_type:<8} {dec_str:<8}\n")
            position = end_pos + 1
    
    print(f"  Written copybook to {filepath}")


def main():
    """Main execution"""
    print("=" * 60)
    print("AS400 Fixed-Width File Generator")
    print("Simulating CPYTOIMPF exports from IBM i")
    print("=" * 60)
    print()
    
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR.parent / "copybooks").mkdir(parents=True, exist_ok=True)
    
    # Generate data
    customers = generate_customers()
    invoices = generate_invoices(customers)
    payments = generate_payments(invoices, customers)
    gl_entries = generate_gl_entries(invoices, payments)
    
    print()
    print("Writing fixed-width files...")
    
    # Write data files
    write_fixed_width_file(customers, CUSMAS_LAYOUT, "CUSMAS.txt")
    write_fixed_width_file(invoices, ARMAS_LAYOUT, "ARMAS.txt")
    write_fixed_width_file(payments, PAYTRAN_LAYOUT, "PAYTRAN.txt")
    write_fixed_width_file(gl_entries, GLJRN_LAYOUT, "GLJRN.txt")
    
    print()
    print("Writing copybooks (field position references)...")
    
    # Write copybooks
    write_copybook(CUSMAS_LAYOUT, "CUSMAS.cpy")
    write_copybook(ARMAS_LAYOUT, "ARMAS.cpy")
    write_copybook(PAYTRAN_LAYOUT, "PAYTRAN.cpy")
    write_copybook(GLJRN_LAYOUT, "GLJRN.cpy")
    
    print()
    print("=" * 60)
    print("Generation complete!")
    print()
    print("Generated files simulate AS400 CPYTOIMPF output:")
    print("  - Fixed-width records")
    print("  - CYYMMDD date format (e.g., 1240115 = 2024-01-15)")
    print("  - HHMMSS time format")
    print("  - Right-justified numeric fields")
    print("  - Left-justified character fields")
    print("  - CRLF line endings")
    print("=" * 60)


if __name__ == "__main__":
    main()