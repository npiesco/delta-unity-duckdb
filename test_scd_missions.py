#!/usr/bin/env python3
"""
Test the Delta to PostgreSQL SCD Type 2 workflow with missions data
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from delta_to_postgres_scd import DeltaToPostgresSCD
from config import config

def test_missions_scd():
    """Test SCD Type 2 with missions table"""
    
    print("🚀 Testing Delta to PostgreSQL SCD Type 2 with Missions Data")
    print("=" * 70)
    
    # Initialize pipeline using config
    pipeline = DeltaToPostgresSCD()
    
    try:
        # Test with a small subset of missions data
        result = pipeline.sync_delta_to_postgres_scd(
            delta_table=config.default_delta_table,
            pg_schema=config.pg_schema,
            pg_table="missions_scd",
            business_key_columns=config.default_business_keys,
            delta_query="SELECT mission_id, mission_lifeline, mission_status_level2, incident_name FROM $TABLE LIMIT 10"
        )
        
        print("\n✅ SCD Type 2 sync completed successfully!")
        print("\n📊 Sync Summary:")
        for key, value in result.items():
            print(f"  {key}: {value}")
            
        return True
        
    except Exception as e:
        print(f"\n❌ SCD sync failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_missions_scd()
    sys.exit(0 if success else 1)
