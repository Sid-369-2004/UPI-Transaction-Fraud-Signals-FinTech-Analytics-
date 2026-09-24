-- ===============================================================================
-- CAPSTONE PROJECT: TOPIC 06 - UPI TRANSACTION FRAUD SIGNALS
-- SNOWFLAKE PRODUCTION WAREHOUSE SETUP, DYNAMIC DATA MASKING & ANALYTICAL SUITE
-- ===============================================================================

-- -------------------------------------------------------------------------------
-- SECTION 1: WAREHOUSE, DATABASE, & SCHEMA CREATION
-- -------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE DATABASE IF NOT EXISTS CAPSTONE_UPI_FRAUD;
CREATE SCHEMA IF NOT EXISTS CAPSTONE_UPI_FRAUD.GOLD;
USE SCHEMA CAPSTONE_UPI_FRAUD.GOLD;

-- -------------------------------------------------------------------------------
-- SECTION 2: STAGE & TABLE DDL SPECIFICATIONS
-- -------------------------------------------------------------------------------
CREATE OR REPLACE STAGE SNOWFLAKE_CAPSTONE_STAGE
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- DDL for GOLD_ACCOUNT_DAY
CREATE OR REPLACE TABLE GOLD_ACCOUNT_DAY (
    account_id VARCHAR(32) NOT NULL,
    account_day DATE NOT NULL,
    txns INT NOT NULL,
    max_amount NUMBER(18,2) NOT NULL,
    avg_amount_30d_trailing NUMBER(18,2),
    prior_txn_count INT,
    distinct_devices INT NOT NULL,
    is_amount_spike BOOLEAN NOT NULL,
    is_device_burst BOOLEAN NOT NULL,
    PRIMARY KEY (account_id, account_day)
);

-- DDL for GOLD_MERCHANT_HOUR
CREATE OR REPLACE TABLE GOLD_MERCHANT_HOUR (
    merchant_id VARCHAR(32) NOT NULL,
    hour_of_day INT NOT NULL,
    txns INT NOT NULL,
    failed_txns INT NOT NULL,
    failure_rate NUMBER(8,4) NOT NULL,
    PRIMARY KEY (merchant_id, hour_of_day)
);

-- DDL for SILVER_REJECTS (Audit Table)
CREATE OR REPLACE TABLE SILVER_REJECTS (
    txn_id VARCHAR(32),
    account_id VARCHAR(32),
    merchant_id VARCHAR(32),
    txn_ts VARCHAR(64),
    reject_reason VARCHAR(64),
    _rejected_at TIMESTAMP_NTZ
);

-- -------------------------------------------------------------------------------
-- SECTION 3: SNOWFLAKE SECURITY - DYNAMIC DATA MASKING & RBAC
-- -------------------------------------------------------------------------------
-- Create Roles
CREATE ROLE IF NOT EXISTS FRAUD_OPS;
CREATE ROLE IF NOT EXISTS DATA_ANALYST;

-- Grant Roles Access
GRANT USAGE ON DATABASE CAPSTONE_UPI_FRAUD TO ROLE FRAUD_OPS;
GRANT USAGE ON SCHEMA CAPSTONE_UPI_FRAUD.GOLD TO ROLE FRAUD_OPS;
GRANT SELECT ON ALL TABLES IN SCHEMA CAPSTONE_UPI_FRAUD.GOLD TO ROLE FRAUD_OPS;

GRANT USAGE ON DATABASE CAPSTONE_UPI_FRAUD TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA CAPSTONE_UPI_FRAUD.GOLD TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CAPSTONE_UPI_FRAUD.GOLD TO ROLE DATA_ANALYST;

-- Define Masking Policy: FRAUD_OPS sees clear text; all other roles see masked account numbers (XXXXXXXX1234)
CREATE OR REPLACE MASKING POLICY mask_acct AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() = 'FRAUD_OPS' THEN val
    ELSE 'XXXXXXXX' || RIGHT(val, 4)
  END;

-- Apply Policy to GOLD_ACCOUNT_DAY
ALTER TABLE GOLD_ACCOUNT_DAY MODIFY COLUMN account_id SET MASKING POLICY mask_acct;

-- -------------------------------------------------------------------------------
-- SECTION 4: CAPSTONE BUSINESS INQUIRY QUERIES
-- -------------------------------------------------------------------------------

-- ===============================================================================
-- QUESTION 1: ACCOUNTS WITH TRANSACTION AMOUNT >= 5x TRAILING 30-DAY AVERAGE
-- Target Check: Exactly 560 of the 600 planted spikes are flagged. 
-- The first 40 on 2025-05-01 drop out because prior_txn_count < 5.
-- ===============================================================================
SELECT 
    account_id,
    account_day,
    max_amount,
    avg_amount_30d_trailing AS baseline_30d,
    ROUND(max_amount / avg_amount_30d_trailing, 2) AS spike_multiplier,
    prior_txn_count
FROM GOLD_ACCOUNT_DAY
QUALIFY prior_txn_count >= 5 AND max_amount >= 5 * avg_amount_30d_trailing
ORDER BY max_amount / avg_amount_30d_trailing DESC;

-- ===============================================================================
-- QUESTION 2: ACCOUNTS USING 3 OR MORE DISTINCT DEVICES IN A SINGLE DAY
-- Target Check: Exactly 200 account-days flagged.
-- ===============================================================================
SELECT 
    account_id,
    account_day,
    distinct_devices,
    txns AS total_daily_txns
FROM GOLD_ACCOUNT_DAY
WHERE is_device_burst = TRUE
ORDER BY distinct_devices DESC, txns DESC;

-- ===============================================================================
-- QUESTION 3: MERCHANTS WITH OFF-HOUR FAILURE RATE SPIKES (AFTER 11 PM)
-- Target Check: MER0000-MER0039 failure rate spikes to ~34% in hours 23, 0, 1
-- vs ~6% baseline during normal operating hours.
-- ===============================================================================
WITH hourly_breakdown AS (
    SELECT 
        CASE WHEN merchant_id BETWEEN 'MER0000' AND 'MER0039' THEN 'TARGET_HIGH_RISK_GROUP' ELSE 'STANDARD_MERCHANTS' END AS merchant_group,
        CASE WHEN hour_of_day IN (23, 0, 1) THEN 'LATE_NIGHT_OFF_HOURS' ELSE 'REGULAR_HOURS' END AS time_window,
        SUM(txns) AS total_txns,
        SUM(failed_txns) AS total_failed
    FROM GOLD_MERCHANT_HOUR
    GROUP BY 1, 2
)
SELECT 
    merchant_group,
    time_window,
    total_txns,
    total_failed,
    ROUND(total_failed / total_txns * 100, 2) AS aggregate_failure_rate_pct
FROM hourly_breakdown
ORDER BY merchant_group, time_window;
