#!/usr/bin/env python3
"""
Test querying the PostgreSQL table using OAuth authentication
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from pg_connect import generate_token
from config import config
import psycopg2

def test_query_missions_scd():
    """Query the missions_scd table in PostgreSQL"""
    
    print("🔍 Testing PostgreSQL Query with OAuth Authentication")
    print("=" * 60)
    
    try:
        # Generate OAuth token
        print("Step 1: Generating OAuth token...")
        result = generate_token()
        token = result["token"]
        print(f"✅ OAuth token generated successfully (expires: {result['expiration_time']})")
        
        # Connect to PostgreSQL
        print("\nStep 2: Connecting to PostgreSQL...")
        import urllib.parse
        encoded_username = urllib.parse.quote(config.pg_username, safe='')
        hostname = config.get_pg_hostname(config.pg_instance_name)
        conn_string = f"postgresql://{encoded_username}:{token}@{hostname}:5432/{config.pg_database}?sslmode=require"
        
        conn = psycopg2.connect(conn_string)
        print("✅ Connected to PostgreSQL successfully")
        
        # Query the missions_scd table
        print("\nStep 3: Querying missions_scd table...")
        cursor = conn.cursor()
        
        # Get table info
        cursor.execute(f"""
            SELECT COUNT(*) as total_records,
                   COUNT(CASE WHEN is_current = true THEN 1 END) as current_records,
                   COUNT(CASE WHEN is_current = false THEN 1 END) as historical_records
            FROM "{config.pg_schema}".missions_scd
        """)
        
        counts = cursor.fetchone()
        print(f"📊 Table Statistics:")
        print(f"  Total records: {counts[0]}")
        print(f"  Current records: {counts[1]}")
        print(f"  Historical records: {counts[2]}")
        
        # Get sample data
        print("\n📋 Sample Records:")
        cursor.execute(f"""
            SELECT mission_id, mission_lifeline, mission_status_level2, incident_name,
                   effective_date, end_date, is_current, created_at
            FROM "{config.pg_schema}".missions_scd
            WHERE is_current = true
            ORDER BY mission_id
            LIMIT 5
        """)
        
        records = cursor.fetchall()
        for record in records:
            print(f"  Mission {record[0]}: {record[1]} | {record[2]} | {record[3]} | Current: {record[6]}")
        
        # Test SCD Type 2 structure
        print("\n🔍 SCD Type 2 Column Verification:")
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{config.pg_schema}'
            AND table_name = 'missions_scd'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        for col in columns:
            print(f"  {col[0]}: {col[1]} (nullable: {col[2]})")
        
        cursor.close()
        conn.close()
        
        print("\n✅ PostgreSQL query test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ PostgreSQL query failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_query_missions_scd()
    sys.exit(0 if success else 1)
