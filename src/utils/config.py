"""
Configuration for Finance Modernization Platform
"""

from datetime import datetime
from pathlib import Path


def get_project_root() -> Path:
    """Find project root by looking for known markers"""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "requirements.txt").exists() or (current / ".git").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parent.parent.parent


# Path Configuration
PROJECT_ROOT = get_project_root()
DATA_DIR = PROJECT_ROOT / "data"
MOCK_AS400_DIR = DATA_DIR / "mock_as400"
PHYSICAL_FILES_DIR = MOCK_AS400_DIR / "physical_files"
COPYBOOKS_DIR = MOCK_AS400_DIR / "copybooks"
SPOOL_DIR = MOCK_AS400_DIR / "spool_files"
EXTRACTS_DIR = MOCK_AS400_DIR / "extracts"

# Data Generation Parameters
NUM_CUSTOMERS = 500
NUM_INVOICES = 5000
NUM_PAYMENTS = 4000
DATA_START_DATE = datetime(2023, 1, 1)
DATA_END_DATE = datetime(2024, 12, 31)
RANDOM_SEED = 42

# AS400 Business Rules
REGIONS = ["NE", "SE", "MW", "SW", "WE"]
INDUSTRIES = ["MFG", "HLT", "TEC", "RET", "CON", "TRN", "FIN", "PRO", "HOS", "ENR"]

SEGMENTS = {
    "E": (100000, 500000, [30, 45, 60]),
    "M": (25000, 100000, [30, 45]),
    "S": (5000, 25000, [15, 30]),
    "T": (1000, 10000, [15, 30])
}

SEGMENT_WEIGHTS = [0.10, 0.30, 0.45, 0.15]

PAYMENT_METHODS = {"CK": "Check", "AC": "ACH", "WR": "Wire", "CC": "Credit Card"}
PAYMENT_METHOD_WEIGHTS = [0.35, 0.40, 0.15, 0.10]

GL_ACCOUNTS = {
    "1100": "Cash",
    "1200": "Accounts Receivable", 
    "4100": "Product Revenue",
    "4200": "Service Revenue",
    "6100": "Bad Debt Expense"
}

SYSTEM_USERS = ["JSMITH", "MWILSON", "KJOHNSON", "PBROWN", "SYSTEM", "BATCH"]