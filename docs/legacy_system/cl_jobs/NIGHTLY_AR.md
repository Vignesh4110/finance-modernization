# NIGHTLY_AR - Nightly AR Batch Processing

## Job Information

| Attribute | Value |
|-----------|-------|
| **Job Name** | NIGHTLY_AR |
| **Job Queue** | QBATCH |
| **Scheduled Time** | 02:00 AM (WRKJOBSCDE entry) |
| **Average Runtime** | 45 minutes |
| **Submitted By** | QPGMR |
| **Job Description** | FINPROD/ARJOBD |

## CL Program: NIGHTARCL

```cl
/*==================================================================*/
/* NIGHTARCL - Nightly AR Processing Control Program                 */
/* Library: FINPROD                                                  */
/* Created: 1999-02-15                                               */
/* Modified: 2022-08-10 - Added email notification                   */
/*==================================================================*/

PGM

    DCL VAR(&RUNDATE) TYPE(*CHAR) LEN(7)
    DCL VAR(&JOBSTS)  TYPE(*CHAR) LEN(10)
    DCL VAR(&ERRMSG)  TYPE(*CHAR) LEN(100)
    
    /* Get current date in CYYMMDD format */
    CVTDAT DATE(*CURRENT) TOVAR(&RUNDATE) TOFMT(*CYMD) TOSEP(*NONE)
    
    /* ============================================================ */
    /* STEP 1: Apply Unapplied Cash (Cash Application)              */
    /* ============================================================ */
    SNDMSG MSG('NIGHTLY_AR Step 1: Starting Cash Application') +
           TOUSR(*SYSOPR)
    
    CALL PGM(FINPROD/AR010R) PARM(&RUNDATE)
    MONMSG MSGID(CPF0000) EXEC(GOTO CMDLBL(ERROR))
    
    /* ============================================================ */
    /* STEP 2: Calculate AR Aging                                    */
    /* ============================================================ */
    SNDMSG MSG('NIGHTLY_AR Step 2: Starting AR Aging Calculation') +
           TOUSR(*SYSOPR)
           
    CALL PGM(FINPROD/AR020R) PARM(&RUNDATE)
    MONMSG MSGID(CPF0000) EXEC(GOTO CMDLBL(ERROR))
    
    /* ============================================================ */
    /* STEP 3: Generate Aging Report (Spool File)                   */
    /* ============================================================ */
    SNDMSG MSG('NIGHTLY_AR Step 3: Generating Aging Report') +
           TOUSR(*SYSOPR)
           
    OVRPRTF FILE(QSYSPRT) SPLFNAME(ARAGIN01) HOLD(*YES)
    CALL PGM(FINPROD/AR030R) PARM(&RUNDATE)
    MONMSG MSGID(CPF0000) EXEC(GOTO CMDLBL(ERROR))
    DLTOVR FILE(QSYSPRT)
    
    /* ============================================================ */
    /* STEP 4: Identify Past Due Accounts for Collection            */
    /* ============================================================ */
    SNDMSG MSG('NIGHTLY_AR Step 4: Identifying Collection Accounts') +
           TOUSR(*SYSOPR)
           
    CALL PGM(FINPROD/AR040R) PARM(&RUNDATE)
    MONMSG MSGID(CPF0000) EXEC(GOTO CMDLBL(ERROR))
    
    /* ============================================================ */
    /* STEP 5: Extract Data for External Systems                    */
    /* ============================================================ */
    SNDMSG MSG('NIGHTLY_AR Step 5: Creating Extract Files') +
           TOUSR(*SYSOPR)
    
    /* Customer Extract */
    CPYTOIMPF FROMFILE(FINPROD/CUSMAS) +
              TOSTMF('/finance/extracts/CUSMAS.txt') +
              MBROPT(*REPLACE) STMFCCSID(*PCASCII) +
              RCDDLM(*CRLF) FLDDLM(*NONE) +
              STRDLM(*NONE)
    
    /* Invoice Extract */
    CPYTOIMPF FROMFILE(FINPROD/ARMAS) +
              TOSTMF('/finance/extracts/ARMAS.txt') +
              MBROPT(*REPLACE) STMFCCSID(*PCASCII) +
              RCDDLM(*CRLF) FLDDLM(*NONE) +
              STRDLM(*NONE)
    
    /* Payment Extract */
    CPYTOIMPF FROMFILE(FINPROD/PAYTRAN) +
              TOSTMF('/finance/extracts/PAYTRAN.txt') +
              MBROPT(*REPLACE) STMFCCSID(*PCASCII) +
              RCDDLM(*CRLF) FLDDLM(*NONE) +
              STRDLM(*NONE)
    
    /* GL Journal Extract */
    CPYTOIMPF FROMFILE(FINPROD/GLJRN) +
              TOSTMF('/finance/extracts/GLJRN.txt') +
              MBROPT(*REPLACE) STMFCCSID(*PCASCII) +
              RCDDLM(*CRLF) FLDDLM(*NONE) +
              STRDLM(*NONE)
    
    /* Aging Report Extract */
    CPYTOIMPF FROMFILE(FINPROD/ARAGING) +
              TOSTMF('/finance/extracts/ARAGING.txt') +
              MBROPT(*REPLACE) STMFCCSID(*PCASCII) +
              RCDDLM(*CRLF) FLDDLM(*NONE) +
              STRDLM(*NONE)
    
    MONMSG MSGID(CPF0000) EXEC(GOTO CMDLBL(ERROR))
    
    /* ============================================================ */
    /* STEP 6: Transfer to External Server (FTP)                    */
    /* ============================================================ */
    SNDMSG MSG('NIGHTLY_AR Step 6: FTP Transfer to Analytics Server') +
           TOUSR(*SYSOPR)
           
    FTP RMTSYS('analytics.company.com')
    /* FTP script in FINPROD/QFTPSRC(NIGHTFTP) */
    MONMSG MSGID(CPF0000) EXEC(GOTO CMDLBL(ERROR))
    
    /* ============================================================ */
    /* SUCCESS - Send completion notification                        */
    /* ============================================================ */
    SNDMSG MSG('NIGHTLY_AR completed successfully') TOUSR(*SYSOPR)
    
    /* Send email notification */
    SNDSMTPEMM RCP(('ar-team@company.com')) +
               SUBJECT('NIGHTLY_AR Job Complete') +
               NOTE('Nightly AR processing completed successfully.')
    
    GOTO CMDLBL(ENDPGM)
    
    /* ============================================================ */
    /* ERROR HANDLING                                                */
    /* ============================================================ */
    ERROR:
        RCVMSG MSGTYPE(*EXCP) MSG(&ERRMSG)
        
        SNDMSG MSG('NIGHTLY_AR FAILED: ' *CAT &ERRMSG) +
               TOUSR(*SYSOPR)
               
        SNDSMTPEMM RCP(('ar-team@company.com') +
                       ('it-oncall@company.com')) +
                   SUBJECT('ALERT: NIGHTLY_AR Job FAILED') +
                   NOTE('Nightly AR processing failed. Error: ' +
                        *CAT &ERRMSG)
        
        MONMSG MSGID(CPF0000)
    
    ENDPGM:
        ENDPGM

```

## Job Schedule Entry (WRKJOBSCDE)

```
Job: NIGHTLY_AR    Status: *ACTIVE
Schedule: *DAILY at 02:00
Job Queue: QBATCH/FINPROD
Recovery: *SBMRLS (Submit on release if system was down)
```

## Dependent Programs

| Step | Program | Description | Input | Output |
|------|---------|-------------|-------|--------|
| 1 | AR010R | Cash Application | PAYTRAN, ARMAS | ARMAS (updated) |
| 2 | AR020R | Aging Calculation | ARMAS, CUSMAS | ARAGING |
| 3 | AR030R | Aging Report | ARAGING | Spool: ARAGIN01 |
| 4 | AR040R | Collection Flagging | ARAGING | ARCOLL |
| 5 | CPYTOIMPF | Extract to IFS | All files | /finance/extracts/*.txt |
| 6 | FTP | Transfer files | Extract files | Remote server |

## Output Files

### Extract File Specifications

All extracts are **fixed-width** files matching the physical file record layouts.

| File | Record Length | Encoding | Line Ending |
|------|---------------|----------|-------------|
| CUSMAS.txt | 350 bytes | ASCII (CCSID 819) | CRLF |
| ARMAS.txt | 400 bytes | ASCII (CCSID 819) | CRLF |
| PAYTRAN.txt | 300 bytes | ASCII (CCSID 819) | CRLF |
| GLJRN.txt | 250 bytes | ASCII (CCSID 819) | CRLF |
| ARAGING.txt | 200 bytes | ASCII (CCSID 819) | CRLF |

### Spool File: ARAGIN01

```
+------------------------------------------------------------------------+
|                     ACCOUNTS RECEIVABLE AGING REPORT                    |
|                          AS OF: 12/31/2024                              |
|                          RUN DATE: 01/01/2025                           |
+------------------------------------------------------------------------+
| CUST#  | CUSTOMER NAME                    | CURRENT    | 1-30 DAYS    |
|--------|----------------------------------|------------|--------------|
| 100234 | ACME MANUFACTURING INC           |   5,234.50 |    1,200.00  |
| 100235 | BETA CORP                        |  12,500.00 |        0.00  |
...
+------------------------------------------------------------------------+
| TOTALS:                                  | 125,432.50 |   45,230.00  |
+------------------------------------------------------------------------+
```

## Modernization Mapping

| Legacy Step | Modern Equivalent | Technology |
|-------------|-------------------|------------|
| WRKJOBSCDE 02:00 | Airflow DAG schedule | `schedule_interval='0 2 * * *'` |
| AR010R Cash Application | `ml_cash_application` task | Python + ML model |
| AR020R Aging Calculation | `dbt run --select fct_ar_aging` | dbt model |
| AR030R Aging Report | Streamlit dashboard | Real-time |
| AR040R Collection Flagging | `ml_collection_scoring` task | Python + ML model |
| CPYTOIMPF extracts | Replaced by CDC/direct DB connection | Debezium or Python |
| FTP transfer | S3 upload or direct warehouse load | boto3 / dbt |
| SNDSMTPEMM | Airflow email/Slack operator | `SlackWebhookOperator` |
| Error handling | Airflow `on_failure_callback` | Python function |

## Critical Business Rules

1. **Cash Application must complete before Aging** - Payment data affects aging buckets
2. **Aging must complete before Collection flagging** - Collection uses aging data
3. **Extracts must happen after all updates** - Ensure data consistency
4. **Job must complete before 6 AM** - Reports needed for morning AR meeting
5. **If job fails, IT on-call must be notified immediately**