"""
Quick smoke test for Snowflake connection.
Usage: py test_snowflake.py
"""
import os
import sys
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Windows consoles default to cp1252, which can't print emoji
sys.stdout.reconfigure(encoding="utf-8")

print("Connecting to Snowflake...")

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)

cur = conn.cursor()

# Test 1: basic connection
cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
row = cur.fetchone()
print(f"✅ Connected as: {row[0]} | Role: {row[1]} | Warehouse: {row[2]}")

# Test 2: tables exist
cur.execute("SHOW TABLES IN SCHEMA marketsignal.raw")
tables = [r[1] for r in cur.fetchall()]
print(f"✅ Tables in raw schema: {tables}")

# Test 3: can insert a test row
cur.execute("""
    INSERT INTO marketsignal.raw.edgar_filings
    (ticker, cik, file_date, form_type, display_names)
    VALUES ('TEST', '000000', '2024-01-01', '8-K', 'Test Company Inc.')
""")
print("✅ Test row inserted into edgar_filings")

# Test 4: can read it back
cur.execute("SELECT ticker, form_type, display_names FROM marketsignal.raw.edgar_filings WHERE ticker = 'TEST'")
row = cur.fetchone()
print(f"✅ Read back: {row}")

# Cleanup
cur.execute("DELETE FROM marketsignal.raw.edgar_filings WHERE ticker = 'TEST'")
print("✅ Test row cleaned up")

conn.close()
print("\n🎉 Snowflake is fully working!")