-- ============================================================================
-- DB2/400 Finance System - Table Definitions
-- System: FINPROD (Production Finance Library)
-- Created: 1998-03-15
-- Last Modified: 2019-07-22
-- 
-- These tables represent the core AR/GL subsystem running on IBM i (AS400)
-- Field naming follows IBM i conventions (10 char max, abbreviated)
-- ============================================================================

-- ============================================================================
-- CUSMAS - Customer Master File
-- Physical File with 3 Logical Files (by region, by name, by credit status)
-- Record Length: 350 bytes
-- ============================================================================
CREATE TABLE FINPROD.CUSMAS (
    -- Key Fields
    CMCUST      DECIMAL(6, 0)    NOT NULL,           -- Customer Number (packed)
    
    -- Name and Address
    CMNAME      CHAR(40)         NOT NULL,           -- Customer Name
    CMCONT      CHAR(30)         DEFAULT '',         -- Contact Name
    CMADR1      CHAR(40)         DEFAULT '',         -- Address Line 1
    CMADR2      CHAR(40)         DEFAULT '',         -- Address Line 2
    CMCITY      CHAR(25)         DEFAULT '',         -- City
    CMSTAT      CHAR(2)          DEFAULT '',         -- State Code
    CMZIPC      CHAR(10)         DEFAULT '',         -- Zip Code
    CMPHON      DECIMAL(10, 0)   DEFAULT 0,          -- Phone (packed, no formatting)
    CMEMAL      CHAR(50)         DEFAULT '',         -- Email Address
    
    -- Classification
    CMREGN      CHAR(2)          DEFAULT '',         -- Region Code (NE,SE,MW,SW,WE)
    CMINDS      CHAR(3)          DEFAULT '',         -- Industry Code
    CMSEGM      CHAR(1)          DEFAULT '',         -- Segment (E=Enterprise,M=Mid,S=Small,T=Startup)
    CMTYPE      CHAR(1)          DEFAULT 'R',        -- Type (R=Regular,G=Government,I=Internal)
    
    -- Credit Information
    CMCRLT      DECIMAL(11, 2)   DEFAULT 0,          -- Credit Limit (packed decimal)
    CMCRUS      DECIMAL(11, 2)   DEFAULT 0,          -- Current Credit Used
    CMPMTM      DECIMAL(3, 0)    DEFAULT 30,         -- Payment Terms (days)
    CMCRST      CHAR(1)          DEFAULT 'A',        -- Credit Status (A=Active,H=Hold,S=Suspend)
    
    -- Status and Audit
    CMSTAT2     CHAR(1)          DEFAULT 'A',        -- Account Status (A=Active,I=Inactive,C=Closed)
    CMCDAT      DECIMAL(7, 0)    NOT NULL,           -- Create Date (CYYMMDD)
    CMUDAT      DECIMAL(7, 0)    NOT NULL,           -- Last Update Date (CYYMMDD)
    CMUTIM      DECIMAL(6, 0)    DEFAULT 0,          -- Last Update Time (HHMMSS)
    CMUUSR      CHAR(10)         DEFAULT '',         -- Last Update User
    
    PRIMARY KEY (CMCUST)
);

-- Logical File: Customer by Region
CREATE INDEX FINPROD.CUSMASL1 ON FINPROD.CUSMAS (CMREGN, CMCUST);

-- Logical File: Customer by Name  
CREATE INDEX FINPROD.CUSMASL2 ON FINPROD.CUSMAS (CMNAME, CMCUST);

-- Logical File: Customer by Credit Status
CREATE INDEX FINPROD.CUSMASL3 ON FINPROD.CUSMAS (CMCRST, CMCUST);


-- ============================================================================
-- ARMAS - Accounts Receivable Invoice Master
-- Physical File 
-- Record Length: 400 bytes
-- ============================================================================
CREATE TABLE FINPROD.ARMAS (
    -- Key Fields
    AMINVN      DECIMAL(9, 0)    NOT NULL,           -- Invoice Number (packed)
    AMCUST      DECIMAL(6, 0)    NOT NULL,           -- Customer Number (packed)
    
    -- Invoice Details
    AMINVD      DECIMAL(7, 0)    NOT NULL,           -- Invoice Date (CYYMMDD)
    AMDUED      DECIMAL(7, 0)    NOT NULL,           -- Due Date (CYYMMDD)
    AMSHPD      DECIMAL(7, 0)    DEFAULT 0,          -- Ship Date (CYYMMDD)
    AMPONM      CHAR(20)         DEFAULT '',         -- PO Number
    AMREF1      CHAR(30)         DEFAULT '',         -- Reference 1
    AMREF2      CHAR(30)         DEFAULT '',         -- Reference 2
    
    -- Amounts (all packed decimal)
    AMINVA      DECIMAL(11, 2)   NOT NULL,           -- Invoice Amount
    AMTAXA      DECIMAL(9, 2)    DEFAULT 0,          -- Tax Amount
    AMFRTA      DECIMAL(9, 2)    DEFAULT 0,          -- Freight Amount
    AMDISA      DECIMAL(9, 2)    DEFAULT 0,          -- Discount Amount
    AMPAID      DECIMAL(11, 2)   DEFAULT 0,          -- Amount Paid
    AMCURB      DECIMAL(11, 2)   NOT NULL,           -- Current Balance
    
    -- Status
    AMSTAT      CHAR(2)          DEFAULT 'OP',       -- Status (OP=Open,PD=Paid,PP=Partial,DP=Disputed,WO=WriteOff)
    AMHOLD      CHAR(1)          DEFAULT 'N',        -- Hold Flag (Y/N)
    AMDISP      CHAR(1)          DEFAULT 'N',        -- Dispute Flag (Y/N)
    AMDRSN      CHAR(3)          DEFAULT '',         -- Dispute Reason Code
    
    -- Terms and Classification
    AMTERM      DECIMAL(3, 0)    DEFAULT 30,         -- Payment Terms
    AMTYPE      CHAR(2)          DEFAULT 'IN',       -- Doc Type (IN=Invoice,CM=Credit Memo,DM=Debit Memo)
    AMDIVN      CHAR(3)          DEFAULT '001',      -- Division
    
    -- GL Information
    AMGLAC      CHAR(10)         DEFAULT '',         -- GL Account
    AMGLDT      DECIMAL(7, 0)    DEFAULT 0,          -- GL Post Date
    AMGLFL      CHAR(1)          DEFAULT 'N',        -- GL Posted Flag
    
    -- Audit
    AMCDAT      DECIMAL(7, 0)    NOT NULL,           -- Create Date (CYYMMDD)
    AMUDAT      DECIMAL(7, 0)    NOT NULL,           -- Last Update Date
    AMUTIM      DECIMAL(6, 0)    DEFAULT 0,          -- Last Update Time
    AMUUSR      CHAR(10)         DEFAULT '',         -- Last Update User
    AMBESSION   DECIMAL(9, 0)    DEFAULT 0,          -- Batch Session ID
    
    PRIMARY KEY (AMINVN)
);

-- Logical File: Invoice by Customer
CREATE INDEX FINPROD.ARMASL1 ON FINPROD.ARMAS (AMCUST, AMINVN);

-- Logical File: Invoice by Due Date (for aging)
CREATE INDEX FINPROD.ARMASL2 ON FINPROD.ARMAS (AMDUED, AMSTAT);

-- Logical File: Open Items Only
CREATE INDEX FINPROD.ARMASL3 ON FINPROD.ARMAS (AMSTAT, AMCUST) WHERE AMSTAT IN ('OP', 'PP', 'DP');


-- ============================================================================
-- PAYTRAN - Payment Transaction File
-- Physical File
-- Record Length: 300 bytes
-- ============================================================================
CREATE TABLE FINPROD.PAYTRAN (
    -- Key Fields
    PTPAYID     DECIMAL(9, 0)    NOT NULL,           -- Payment ID (packed)
    PTCUST      DECIMAL(6, 0)    NOT NULL,           -- Customer Number
    
    -- Payment Details
    PTPAYDT     DECIMAL(7, 0)    NOT NULL,           -- Payment Date (CYYMMDD)
    PTPAYAM     DECIMAL(11, 2)   NOT NULL,           -- Payment Amount
    PTPAYMTH    CHAR(2)          NOT NULL,           -- Payment Method (CK=Check,AC=ACH,WR=Wire,CC=Card)
    
    -- Check/Reference Info
    PTCKNUM     CHAR(15)         DEFAULT '',         -- Check Number
    PTBNKRF     CHAR(25)         DEFAULT '',         -- Bank Reference
    PTREMIT     CHAR(40)         DEFAULT '',         -- Remittance Name (as received)
    
    -- Application Info
    PTINVRF     DECIMAL(9, 0)    DEFAULT 0,          -- Invoice Reference (if provided)
    PTAPFLG     CHAR(1)          DEFAULT 'N',        -- Applied Flag (Y/N)
    PTAPDAT     DECIMAL(7, 0)    DEFAULT 0,          -- Applied Date
    PTAPAMT     DECIMAL(11, 2)   DEFAULT 0,          -- Applied Amount
    PTUNAPP     DECIMAL(11, 2)   DEFAULT 0,          -- Unapplied Amount
    
    -- Classification
    PTTYPE      CHAR(2)          DEFAULT 'PM',       -- Type (PM=Payment,RF=Refund,AD=Adjustment)
    PTSTAT      CHAR(2)          DEFAULT 'RV',       -- Status (RV=Received,AP=Applied,RJ=Rejected,RT=Returned)
    PTBESSION   DECIMAL(9, 0)    DEFAULT 0,          -- Batch Session ID
    PTBATCH     CHAR(15)         DEFAULT '',         -- Batch ID
    
    -- Audit
    PTCDAT      DECIMAL(7, 0)    NOT NULL,           -- Create Date
    PTUDAT      DECIMAL(7, 0)    NOT NULL,           -- Last Update Date
    PTUTIM      DECIMAL(6, 0)    DEFAULT 0,          -- Last Update Time
    PTUUSR      CHAR(10)         DEFAULT '',         -- Last Update User
    
    PRIMARY KEY (PTPAYID)
);

-- Logical File: Payments by Customer
CREATE INDEX FINPROD.PAYTRANL1 ON FINPROD.PAYTRAN (PTCUST, PTPAYDT);

-- Logical File: Unapplied Payments
CREATE INDEX FINPROD.PAYTRANL2 ON FINPROD.PAYTRAN (PTAPFLG, PTCUST) WHERE PTAPFLG = 'N';


-- ============================================================================
-- GLJRN - General Ledger Journal Entry File
-- Physical File
-- Record Length: 250 bytes
-- ============================================================================
CREATE TABLE FINPROD.GLJRN (
    -- Key Fields
    GLJRNID     DECIMAL(10, 0)   NOT NULL,           -- Journal Entry ID (packed)
    GLJRNLN     DECIMAL(5, 0)    NOT NULL,           -- Journal Line Number
    
    -- Posting Info
    GLPOST      DECIMAL(7, 0)    NOT NULL,           -- Post Date (CYYMMDD)
    GLPERD      DECIMAL(6, 0)    NOT NULL,           -- Period (YYYYMM)
    GLFYEAR     DECIMAL(4, 0)    NOT NULL,           -- Fiscal Year
    
    -- Account
    GLACCT      CHAR(10)         NOT NULL,           -- GL Account
    GLDEPT      CHAR(4)          DEFAULT '0000',     -- Department
    GLPROJ      CHAR(6)          DEFAULT '',         -- Project Code
    
    -- Amounts
    GLDRAM      DECIMAL(13, 2)   DEFAULT 0,          -- Debit Amount
    GLCRAM      DECIMAL(13, 2)   DEFAULT 0,          -- Credit Amount
    
    -- Reference
    GLDESC      CHAR(50)         DEFAULT '',         -- Description
    GLREF       CHAR(20)         DEFAULT '',         -- Reference Number
    GLSRC       CHAR(2)          DEFAULT '',         -- Source (AR,AP,GL,PR)
    GLDOCTY     CHAR(3)          DEFAULT '',         -- Document Type
    
    -- Status
    GLSTAT      CHAR(1)          DEFAULT 'P',        -- Status (P=Posted,R=Reversed,E=Error)
    GLRVFL      CHAR(1)          DEFAULT 'N',        -- Reversal Flag
    GLRVJN      DECIMAL(10, 0)   DEFAULT 0,          -- Reversal Journal ID
    
    -- Audit
    GLCDAT      DECIMAL(7, 0)    NOT NULL,           -- Create Date
    GLUCDAT     DECIMAL(7, 0)    NOT NULL,           -- Last Update Date
    GLUCTIM     DECIMAL(6, 0)    DEFAULT 0,          -- Last Update Time
    GLUCUSR     CHAR(10)         DEFAULT '',         -- Last Update User
    GLBESSION   DECIMAL(9, 0)    DEFAULT 0,          -- Batch Session ID
    
    PRIMARY KEY (GLJRNID, GLJRNLN)
);

-- Logical File: By Account and Period
CREATE INDEX FINPROD.GLJRNL1 ON FINPROD.GLJRN (GLACCT, GLPERD);

-- Logical File: By Post Date
CREATE INDEX FINPROD.GLJRNL2 ON FINPROD.GLJRN (GLPOST);


-- ============================================================================
-- AROPN - AR Open Items View (Logical File over ARMAS)
-- This is what the nightly aging job reads from
-- ============================================================================
CREATE VIEW FINPROD.AROPN AS
SELECT 
    AMINVN, AMCUST, AMINVD, AMDUED, AMINVA, AMCURB, 
    AMSTAT, AMTERM, AMDISP, AMDRSN, AMUDAT
FROM FINPROD.ARMAS
WHERE AMSTAT IN ('OP', 'PP', 'DP')
  AND AMCURB > 0;


-- ============================================================================
-- Stored Procedure: ARCALCAG - Calculate AR Aging
-- Called by nightly batch job NIGHTLY_AR
-- ============================================================================
CREATE PROCEDURE FINPROD.ARCALCAG (
    IN P_ASOFDT DECIMAL(7,0),
    IN P_CUSTID DECIMAL(6,0)
)
LANGUAGE SQL
BEGIN
    DECLARE V_CURRENT   DECIMAL(13,2) DEFAULT 0;
    DECLARE V_DAYS30    DECIMAL(13,2) DEFAULT 0;
    DECLARE V_DAYS60    DECIMAL(13,2) DEFAULT 0;
    DECLARE V_DAYS90    DECIMAL(13,2) DEFAULT 0;
    DECLARE V_OVER90    DECIMAL(13,2) DEFAULT 0;
    
    -- Calculate aging buckets
    SELECT 
        SUM(CASE WHEN (P_ASOFDT - AMDUED) <= 0 THEN AMCURB ELSE 0 END),
        SUM(CASE WHEN (P_ASOFDT - AMDUED) BETWEEN 1 AND 30 THEN AMCURB ELSE 0 END),
        SUM(CASE WHEN (P_ASOFDT - AMDUED) BETWEEN 31 AND 60 THEN AMCURB ELSE 0 END),
        SUM(CASE WHEN (P_ASOFDT - AMDUED) BETWEEN 61 AND 90 THEN AMCURB ELSE 0 END),
        SUM(CASE WHEN (P_ASOFDT - AMDUED) > 90 THEN AMCURB ELSE 0 END)
    INTO V_CURRENT, V_DAYS30, V_DAYS60, V_DAYS90, V_OVER90
    FROM FINPROD.AROPN
    WHERE AMCUST = P_CUSTID;
    
    -- Update or insert into aging summary table
    -- (Simplified - actual procedure would be more complex)
END;


-- ============================================================================
-- Stored Procedure: GLPOSTJN - Post Journal Entries
-- Called after invoice creation and payment application
-- ============================================================================
CREATE PROCEDURE FINPROD.GLPOSTJN (
    IN P_JRNID  DECIMAL(10,0),
    IN P_POSTDT DECIMAL(7,0)
)
LANGUAGE SQL
BEGIN
    DECLARE V_TOTDR DECIMAL(13,2);
    DECLARE V_TOTCR DECIMAL(13,2);
    
    -- Verify debits = credits
    SELECT SUM(GLDRAM), SUM(GLCRAM)
    INTO V_TOTDR, V_TOTCR
    FROM FINPROD.GLJRN
    WHERE GLJRNID = P_JRNID;
    
    IF V_TOTDR = V_TOTCR THEN
        UPDATE FINPROD.GLJRN
        SET GLSTAT = 'P', GLPOST = P_POSTDT
        WHERE GLJRNID = P_JRNID;
    ELSE
        SIGNAL SQLSTATE '75001'
        SET MESSAGE_TEXT = 'Journal entry out of balance';
    END IF;
END;