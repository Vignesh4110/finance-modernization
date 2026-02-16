# AR001R - Invoice Processing Program

## Program Information

| Attribute | Value |
|-----------|-------|
| **Program ID** | AR001R |
| **Type** | RPGLE (ILE RPG) |
| **Library** | FINPROD |
| **Source File** | QRPGLESRC |
| **Created** | 1998-04-22 |
| **Last Modified** | 2021-11-15 |
| **Author** | J. Smith (Original), M. Wilson (Modifications) |

## Purpose

This program processes new invoices entered through the 5250 green screen interface (program AR001D - display file). It validates invoice data, updates the AR Master file (ARMAS), creates GL journal entries, and updates customer credit usage.

## Program Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    AR001R - Main Logic                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. INIT - Initialize work fields, open files               │
│     │                                                       │
│     ▼                                                       │
│  2. GETINV - Read invoice data from display file            │
│     │                                                       │
│     ▼                                                       │
│  3. VALCUST - Validate customer exists and is active        │
│     │         Chain to CUSMAS by CMCUST                     │
│     │         Check CMSTAT2 = 'A' (Active)                  │
│     │         Check CMCRST <> 'S' (Not Suspended)           │
│     │                                                       │
│     ▼                                                       │
│  4. VALCRDT - Validate credit limit not exceeded            │
│     │         Calculate: CMCRUS + Invoice Amount            │
│     │         Compare to CMCRLT (Credit Limit)              │
│     │         If exceeded, set error and return             │
│     │                                                       │
│     ▼                                                       │
│  5. WRTINV - Write invoice record to ARMAS                  │
│     │         Generate next invoice number (GETINVN)        │
│     │         Set AMINVD = Current Date (CYYMMDD)           │
│     │         Set AMDUED = AMINVD + CMPMTM (Payment Terms)  │
│     │         Set AMCURB = AMINVA (Full balance)            │
│     │         Set AMSTAT = 'OP' (Open)                      │
│     │                                                       │
│     ▼                                                       │
│  6. UPDCUST - Update customer credit usage                  │
│     │         CMCRUS = CMCRUS + Invoice Amount              │
│     │         CMUDAT = Current Date                         │
│     │         CMUTIM = Current Time                         │
│     │                                                       │
│     ▼                                                       │
│  7. CREATGL - Create GL Journal Entries                     │
│     │         Debit:  1200 (AR) for Invoice Amount          │
│     │         Credit: 4100 (Revenue) for Invoice Amount     │
│     │         Call GLPOSTJN stored procedure                │
│     │                                                       │
│     ▼                                                       │
│  8. ENDPGM - Commit transaction, close files                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Files Used

| File | Type | Usage | Key |
|------|------|-------|-----|
| CUSMAS | Physical | Update | CMCUST |
| CUSMASL1 | Logical | Read | CMREGN, CMCUST |
| ARMAS | Physical | Add/Update | AMINVN |
| GLJRN | Physical | Add | GLJRNID, GLJRNLN |
| AR001D | Display | Input/Output | N/A |

## Key Subroutines

### GETINVN - Get Next Invoice Number
```rpgle
// Reads control file ARCTL to get last invoice number
// Increments by 1 and updates control file
// Returns formatted invoice number

CHAIN 'INVNUM' ARCTL;
IF %FOUND;
    LASTINV = LASTINV + 1;
    UPDATE ARCTLR;
ELSE;
    // Error - control record not found
    *INLR = *ON;
    RETURN;
ENDIF;
```

### CVTDAT - Convert Date to CYYMMDD
```rpgle
// Converts system date to IBM century format
// C = 0 for 1900s, 1 for 2000s
// Example: 2024-01-15 becomes 1240115

DCL-S CYYMMDD PACKED(7:0);
DCL-S CENTURY PACKED(1:0);

IF %SUBDT(%DATE():*YEARS) >= 2000;
    CENTURY = 1;
ELSE;
    CENTURY = 0;
ENDIF;

CYYMMDD = (CENTURY * 1000000) +
          (%SUBDT(%DATE():*YEARS) - (CENTURY * 100 + 19) * 100) * 10000 +
          %SUBDT(%DATE():*MONTHS) * 100 +
          %SUBDT(%DATE():*DAYS);
```

### CALCDUED - Calculate Due Date
```rpgle
// Adds payment terms to invoice date
// Handles month-end scenarios

DCL-S DUEDATE DATE;
DCL-S INVDATE DATE;
DCL-S TERMS PACKED(3:0);

INVDATE = CVTTODAT(AMINVD);  // Convert CYYMMDD to date
DUEDATE = INVDATE + %DAYS(TERMS);
AMDUED = CVTFRDAT(DUEDATE);  // Convert back to CYYMMDD
```

## Error Handling

| Error Code | Message | Action |
|------------|---------|--------|
| AR0001 | Customer not found | Return to screen with error |
| AR0002 | Customer account inactive | Return to screen with error |
| AR0003 | Customer on credit hold | Return to screen with error |
| AR0004 | Credit limit exceeded | Return to screen with error |
| AR0005 | GL posting failed | Rollback and notify |

## Modernization Notes

### What This Program Does That We Need to Replicate:
1. **Validation Logic** → dbt tests + Great Expectations
2. **Credit Limit Check** → Python business rule in transformation layer
3. **Invoice Number Generation** → Handled by target database sequence
4. **Date Conversion (CYYMMDD)** → Python parsing in ingestion layer
5. **GL Entry Creation** → dbt model with double-entry logic
6. **Customer Credit Update** → dbt incremental model

### Data Quality Rules to Implement:
- Customer must exist and be active
- Invoice amount must be positive
- Due date must be after invoice date
- Credit limit check (warning, not blocking in modern system)
- GL entries must balance

### Batch vs Real-time:
- Legacy: Interactive (5250 screen entry)
- Modern: Batch ingestion from source, near real-time for new systems