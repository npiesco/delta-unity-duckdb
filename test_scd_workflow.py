#!/usr/bin/env python3
"""
Test the Delta to PostgreSQL SCD Type 2 workflow
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from delta_to_postgres_scd import DeltaToPostgresSCD

def test_missions_table_sync():
    """Test syncing missions table with SCD Type 2"""
    
    # Initialize pipeline with credentials from environment
    pipeline = DeltaToPostgresSCD(
        workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL"),
        databricks_token=os.getenv("DATABRICKS_TOKEN")
    )
    
    # Test with missions table
    try:
        result = pipeline.sync_delta_to_postgres_scd(
            delta_table="grayskygenai_sandbox_dev.gold.missions",
            pg_schema="nicholas.piesco@slalom.com",
            pg_table="missions_scd",
            business_key_columns=["mission_id"],  # Assuming mission_id is the business key
            delta_query="SELECT * FROM $TABLE LIMIT 100"  # Limit for testing
        )
        
        print("✅ Test completed successfully!")
        print(f"Result: {result}")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

def test_fuel_transactions_sync():
    """Test syncing fuel transactions with SCD Type 2"""
    
    pipeline = DeltaToPostgresSCD(
        workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL"),
        databricks_token=os.getenv("DATABRICKS_TOKEN")
    )
    
    try:
        result = pipeline.sync_delta_to_postgres_scd(
            delta_table="grayskygenai_sandbox_dev.silver.fuel_transactions",
            pg_schema="nicholas.piesco@slalom.com", 
            pg_table="fuel_transactions_scd",
            business_key_columns=["transaction_id"],  # Assuming transaction_id is the business key
            delta_query="SELECT * FROM $TABLE LIMIT 50"  # Limit for testing
        )
        
        print("✅ Fuel transactions test completed!")
        print(f"Result: {result}")
        
    except Exception as e:
        print(f"❌ Fuel transactions test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Testing Delta to PostgreSQL SCD Type 2 Workflow")
    print("=" * 60)
    
    print("\n1. Testing Missions Table Sync...")
    test_missions_table_sync()
    
    print("\n" + "=" * 60)
    print("\n2. Testing Fuel Transactions Sync...")
    test_fuel_transactions_sync()
    
    print("\n🎉 All tests completed!")
