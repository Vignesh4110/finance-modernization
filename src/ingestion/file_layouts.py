"""
AS400 File Layout Definitions

Defines the structure of each AS400 physical file for parsing.
These layouts match the DDS definitions in docs/legacy_system/db2_ddl/

Each layout is a list of tuples:
    (field_name, width, field_type, decimal_places, target_name)
    
    - field_name: AS400 field name (from DDS)
    - width: Character width in the fixed-width file
    - field_type: 'char', 'packed', 'date', 'time'
    - decimal_places: For 'packed' type, number of decimal places (None for others)
    - target_name: Clean Python/database column name for output

Author: Vignesh
"""

from dataclasses import dataclass
from typing import List, Tuple, Optional

# Type alias for layout definition
# (as400_name, width, type, decimals, target_name)
LayoutField = Tuple[str, int, str, Optional[int], str]


@dataclass
class FileLayout:
    """Defines an AS400 file layout with metadata"""
    name: str                    # File name (e.g., 'CUSMAS')
    description: str             # Human-readable description
    record_length: int           # Expected record length
    fields: List[LayoutField]    # Field definitions
    
    def get_field_names(self) -> List[str]:
        """Get list of target field names"""
        return [f[4] for f in self.fields]
    
    def get_as400_names(self) -> List[str]:
        """Get list of AS400 field names"""
        return [f[0] for f in self.fields]
    
    def get_total_width(self) -> int:
        """Calculate total width from field definitions"""
        return sum(f[1] for f in self.fields)


# =============================================================================
# CUSMAS - Customer Master File
# =============================================================================
CUSMAS_LAYOUT = FileLayout(
    name="CUSMAS",
    description="Customer Master File",
    record_length=350,
    fields=[
        # (as400_name, width, type, decimals, target_name)
        ("CMCUST", 6, "packed", 0, "customer_id"),
        ("CMNAME", 40, "char", None, "customer_name"),
        ("CMCONT", 30, "char", None, "contact_name"),
        ("CMADR1", 40, "char", None, "address_line1"),
        ("CMADR2", 40, "char", None, "address_line2"),
        ("CMCITY", 25, "char", None, "city"),
        ("CMSTAT", 2, "char", None, "state"),
        ("CMZIPC", 10, "char", None, "zip_code"),
        ("CMPHON", 10, "packed", 0, "phone"),
        ("CMEMAL", 50, "char", None, "email"),
        ("CMREGN", 2, "char", None, "region"),
        ("CMINDS", 3, "char", None, "industry_code"),
        ("CMSEGM", 1, "char", None, "segment"),
        ("CMTYPE", 1, "char", None, "customer_type"),
        ("CMCRLT", 14, "packed", 2, "credit_limit"),
        ("CMCRUS", 14, "packed", 2, "credit_used"),
        ("CMPMTM", 3, "packed", 0, "payment_terms"),
        ("CMCRST", 1, "char", None, "credit_status"),
        ("CMSTAT2", 1, "char", None, "account_status"),
        ("CMCDAT", 7, "date", None, "created_date"),
        ("CMUDAT", 7, "date", None, "updated_date"),
        ("CMUTIM", 6, "time", None, "updated_time"),
        ("CMUUSR", 10, "char", None, "updated_by"),
    ]
)


# =============================================================================
# ARMAS - Accounts Receivable Invoice Master
# =============================================================================
ARMAS_LAYOUT = FileLayout(
    name="ARMAS",
    description="AR Invoice Master File",
    record_length=400,
    fields=[
        ("AMINVN", 9, "packed", 0, "invoice_number"),
        ("AMCUST", 6, "packed", 0, "customer_id"),
        ("AMINVD", 7, "date", None, "invoice_date"),
        ("AMDUED", 7, "date", None, "due_date"),
        ("AMSHPD", 7, "date", None, "ship_date"),
        ("AMPONM", 20, "char", None, "po_number"),
        ("AMREF1", 30, "char", None, "reference1"),
        ("AMREF2", 30, "char", None, "reference2"),
        ("AMINVA", 14, "packed", 2, "invoice_amount"),
        ("AMTAXA", 12, "packed", 2, "tax_amount"),
        ("AMFRTA", 12, "packed", 2, "freight_amount"),
        ("AMDISA", 12, "packed", 2, "discount_amount"),
        ("AMPAID", 14, "packed", 2, "amount_paid"),
        ("AMCURB", 14, "packed", 2, "current_balance"),
        ("AMSTAT", 2, "char", None, "status"),
        ("AMHOLD", 1, "char", None, "hold_flag"),
        ("AMDISP", 1, "char", None, "dispute_flag"),
        ("AMDRSN", 3, "char", None, "dispute_reason"),
        ("AMTERM", 3, "packed", 0, "payment_terms"),
        ("AMTYPE", 2, "char", None, "document_type"),
        ("AMDIVN", 3, "char", None, "division"),
        ("AMGLAC", 10, "char", None, "gl_account"),
        ("AMGLDT", 7, "date", None, "gl_post_date"),
        ("AMGLFL", 1, "char", None, "gl_posted_flag"),
        ("AMCDAT", 7, "date", None, "created_date"),
        ("AMUDAT", 7, "date", None, "updated_date"),
        ("AMUTIM", 6, "time", None, "updated_time"),
        ("AMUUSR", 10, "char", None, "updated_by"),
        ("AMBESSION", 9, "packed", 0, "batch_session"),
    ]
)


# =============================================================================
# PAYTRAN - Payment Transaction File
# =============================================================================
PAYTRAN_LAYOUT = FileLayout(
    name="PAYTRAN",
    description="Payment Transaction File",
    record_length=300,
    fields=[
        ("PTPAYID", 9, "packed", 0, "payment_id"),
        ("PTCUST", 6, "packed", 0, "customer_id"),
        ("PTPAYDT", 7, "date", None, "payment_date"),
        ("PTPAYAM", 14, "packed", 2, "payment_amount"),
        ("PTPAYMTH", 2, "char", None, "payment_method"),
        ("PTCKNUM", 15, "char", None, "check_number"),
        ("PTBNKRF", 25, "char", None, "bank_reference"),
        ("PTREMIT", 40, "char", None, "remittance_name"),
        ("PTINVRF", 9, "packed", 0, "invoice_reference"),
        ("PTAPFLG", 1, "char", None, "applied_flag"),
        ("PTAPDAT", 7, "date", None, "applied_date"),
        ("PTAPAMT", 14, "packed", 2, "applied_amount"),
        ("PTUNAPP", 14, "packed", 2, "unapplied_amount"),
        ("PTTYPE", 2, "char", None, "payment_type"),
        ("PTSTAT", 2, "char", None, "status"),
        ("PTBESSION", 9, "packed", 0, "batch_session"),
        ("PTBATCH", 15, "char", None, "batch_id"),
        ("PTCDAT", 7, "date", None, "created_date"),
        ("PTUDAT", 7, "date", None, "updated_date"),
        ("PTUTIM", 6, "time", None, "updated_time"),
        ("PTUUSR", 10, "char", None, "updated_by"),
    ]
)


# =============================================================================
# GLJRN - General Ledger Journal File
# =============================================================================
GLJRN_LAYOUT = FileLayout(
    name="GLJRN",
    description="GL Journal Entry File",
    record_length=250,
    fields=[
        ("GLJRNID", 10, "packed", 0, "journal_id"),
        ("GLJRNLN", 5, "packed", 0, "line_number"),
        ("GLPOST", 7, "date", None, "post_date"),
        ("GLPERD", 6, "packed", 0, "period"),
        ("GLFYEAR", 4, "packed", 0, "fiscal_year"),
        ("GLACCT", 10, "char", None, "gl_account"),
        ("GLDEPT", 4, "char", None, "department"),
        ("GLPROJ", 6, "char", None, "project"),
        ("GLDRAM", 16, "packed", 2, "debit_amount"),
        ("GLCRAM", 16, "packed", 2, "credit_amount"),
        ("GLDESC", 50, "char", None, "description"),
        ("GLREF", 20, "char", None, "reference"),
        ("GLSRC", 2, "char", None, "source"),
        ("GLDOCTY", 3, "char", None, "document_type"),
        ("GLSTAT", 1, "char", None, "status"),
        ("GLRVFL", 1, "char", None, "reversal_flag"),
        ("GLRVJN", 10, "packed", 0, "reversal_journal"),
        ("GLCDAT", 7, "date", None, "created_date"),
        ("GLUCDAT", 7, "date", None, "updated_date"),
        ("GLUCTIM", 6, "time", None, "updated_time"),
        ("GLUCUSR", 10, "char", None, "updated_by"),
        ("GLBESSION", 9, "packed", 0, "batch_session"),
    ]
)


# =============================================================================
# LAYOUT REGISTRY
# =============================================================================

LAYOUTS = {
    "CUSMAS": CUSMAS_LAYOUT,
    "ARMAS": ARMAS_LAYOUT,
    "PAYTRAN": PAYTRAN_LAYOUT,
    "GLJRN": GLJRN_LAYOUT,
}


def get_layout(file_name: str) -> FileLayout:
    """
    Get layout definition by file name.
    
    Args:
        file_name: AS400 file name (e.g., 'CUSMAS')
        
    Returns:
        FileLayout object
        
    Raises:
        KeyError: If file name not found
    """
    file_name = file_name.upper().replace(".TXT", "")
    if file_name not in LAYOUTS:
        raise KeyError(f"Unknown file layout: {file_name}. Available: {list(LAYOUTS.keys())}")
    return LAYOUTS[file_name]


def print_layout(layout: FileLayout):
    """Print a layout definition in a readable format"""
    print(f"\n{'='*70}")
    print(f"File: {layout.name} - {layout.description}")
    print(f"Record Length: {layout.record_length} bytes")
    print(f"{'='*70}")
    print(f"{'AS400':<12} {'Target':<20} {'Start':>6} {'End':>6} {'Len':>5} {'Type':<8} {'Dec':<4}")
    print("-" * 70)
    
    position = 1
    for as400_name, width, field_type, decimals, target_name in layout.fields:
        end = position + width - 1
        dec_str = str(decimals) if decimals is not None else ""
        print(f"{as400_name:<12} {target_name:<20} {position:>6} {end:>6} {width:>5} {field_type:<8} {dec_str:<4}")
        position = end + 1
    
    print(f"\nTotal width from fields: {layout.get_total_width()}")


if __name__ == "__main__":
    # Print all layouts
    for name, layout in LAYOUTS.items():
        print_layout(layout)