"""
Raw Data Generator for Topic 06: UPI Transaction Fraud Signals
Generates exact raw CSV files for Bronze layer ingestion.
Matches the PySpark generator specification (Seed 99) in the Capstone Brief.
"""

import os
import duckdb

def generate_raw_data(output_dir="Project/data/raw"):
    os.makedirs(output_dir, exist_ok=True)
    con = duckdb.connect()
    
    # 1. Accounts Master Dataset (5,000 accounts)
    accounts_query = """
    COPY (
        SELECT 
            printf('ACC%08d', i) AS account_id,
            printf('BANK%d', i % 8) AS bank,
            printf('CITY%d', i % 10) AS home_city,
            printf('DEV%07d', i) AS primary_device,
            printf('DEV9%06d', i) AS secondary_device
        FROM range(0, 5000) tbl(i)
    ) TO '{output_dir}/accounts.csv' (HEADER, DELIMITER ',');
    """.format(output_dir=output_dir)
    
    con.execute(accounts_query)
    print(f"Generated {output_dir}/accounts.csv (5,000 rows)")
    
    # 2. Transactions Raw Dataset (401,600 rows including duplicates, anomalies, corrupt records)
    # Using DuckDB SQL matching PySpark logic
    txns_query = """
    COPY (
    WITH base AS (
        SELECT 
            id,
            CAST(id % 1200 AS INT) AS mer,
            CAST(id / 1200 AS INT) AS ix,
            id - 1200 AS j
        FROM range(0, 400000) tbl(id)
    ),
    t1 AS (
        SELECT 
            *,
            ix % 24 AS hh,
            CASE 
                WHEN id < 600 THEN (CAST(id / 3 AS INT)) % 45 
                WHEN id < 640 THEN 0
                WHEN id < 1200 THEN 10 + (id % 35) 
                ELSE ix % 45 
            END AS dd,
            CASE 
                WHEN id < 600 THEN CAST(id / 3 AS INT) 
                WHEN id < 1200 THEN id - 400
                ELSE (abs(hash(id * 99 + 99)) % 5000)
            END AS a
        FROM base
    ),
    t2 AS (
        SELECT 
            *,
            (abs(hash(id * 104 + 104)) % 1000) AS fr,
            CASE 
                WHEN id BETWEEN 600 AND 1199 THEN round(560.0 * (8 + (id % 13)), 2)
                ELSE round(exp(6.2 + (sin(id) * 0.5)), 2) 
            END AS amt,
            CASE 
                WHEN id < 600 THEN printf('DEVX%06d', id)
                WHEN (abs(hash(id * 5 + 5)) % 100) < 8 THEN printf('DEV9%06d', a)
                ELSE printf('DEV%07d', a) 
            END AS device_id,
            printf('%s %02d:%02d:00', strftime(date '2025-05-01' + CAST(dd AS INT), '%Y-%m-%d'), hh, id % 60) AS ts
        FROM t1
    ),
    t3 AS (
        SELECT 
            id, j,
            printf('T%08d', id) AS txn_id,
            CASE WHEN id >= 1200 AND (j % 700 = 1) AND (j < 350000) THEN 'ACC99999999' ELSE printf('ACC%08d', a) END AS account_id,
            printf('MER%04d', mer) AS merchant_id,
            printf('MC%02d', mer % 12) AS merchant_category,
            CASE WHEN id >= 1200 AND (j % 1200 = 5) AND (j < 360000) THEN '1970-01-01 00:00:00' ELSE ts END AS txn_ts,
            device_id,
            CASE 
                WHEN mer < 40 AND hh IN (23,0,1) THEN CASE WHEN fr < 340 THEN 'FAILED' ELSE 'SUCCESS' END
                WHEN mer < 40 THEN CASE WHEN fr < 60 THEN 'FAILED' ELSE 'SUCCESS' END
                ELSE CASE WHEN fr < 40 THEN 'FAILED' ELSE 'SUCCESS' END
            END AS status,
            CASE WHEN id >= 1200 AND (j % 398 = 0) AND (j < 398000) THEN 'NA' ELSE CAST(amt AS STRING) END AS amount,
            (id >= 1200 AND (j % 100 = 9) AND (j < 160000)) AS dp
        FROM t2
    ),
    main_rows AS (
        SELECT txn_id, account_id, merchant_id, merchant_category, txn_ts, device_id, status, amount FROM t3
    ),
    dupe_rows AS (
        SELECT txn_id, account_id, merchant_id, merchant_category, txn_ts, device_id, status, amount FROM t3 WHERE dp = true
    ),
    all_rows AS (
        SELECT * FROM main_rows
        UNION ALL
        SELECT * FROM dupe_rows
    )
    SELECT * FROM all_rows
    ) TO '{output_dir}/txns.csv' (HEADER, DELIMITER ',');
    """.format(output_dir=output_dir)
    
    con.execute(txns_query)
    total_txns = con.execute(f"SELECT count(*) FROM read_csv_auto('{output_dir}/txns.csv')").fetchone()[0]
    print(f"Generated {output_dir}/txns.csv ({total_txns:,} rows)")

if __name__ == "__main__":
    generate_raw_data()
