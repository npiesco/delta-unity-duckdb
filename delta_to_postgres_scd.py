#!/usr/bin/env python3
"""
Delta to PostgreSQL SCD Type 2 Pipeline
Combines delta scanner, OAuth authentication, and PostgreSQL UPSERT for SCD Type 2
"""

import os
import json
import subprocess
import urllib.parse
from datetime import datetime
from databricks.sdk import WorkspaceClient
import uuid
import psycopg2
from config import config

# Note: DeltaScanner import will be handled in read_delta_table method

class DeltaToPostgresSCD:
    """Delta to PostgreSQL SCD Type 2 pipeline"""
    
    def __init__(self, workspace_url=None, databricks_token=None, pg_username=None, pg_instance=None):
        self.workspace_url = workspace_url or config.databricks_workspace_url
        self.databricks_token = databricks_token or config.databricks_token
        self.pg_username = pg_username or config.pg_username
        self.pg_instance = pg_instance or config.pg_instance_name
        self.oauth_token = None
        self.oauth_expiry = None
        
        # Validate required configuration
        missing = config.validate()
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing.keys())}")
        
    def generate_oauth_token(self):
        """Generate OAuth token for PostgreSQL authentication"""
        w = WorkspaceClient(
            host=self.workspace_url,
            token=self.databricks_token
        )
        
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[self.pg_instance]
        )
        
        self.oauth_token = cred.token
        self.oauth_expiry = cred.expiration_time
        return cred.token, cred.expiration_time
    
    def read_delta_table(self, table_name, query=None):
        """Read Delta table using the delta scanner CLI"""
        if query is None:
            query = f"SELECT * FROM $TABLE"
        
        # Use the delta scanner CLI to read the table
        cmd = [
            "node", "delta-unity-duckdb.js",
            f"--table={table_name}",
            f"--query={query}",
            "--format=json"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Delta scanner failed: {result.stderr}")
        
        print(f"DEBUG: Delta scanner stdout: {result.stdout}")
        print(f"DEBUG: Delta scanner stderr: {result.stderr}")
        
        # Parse JSON output - extract the JSON array from the output
        data = []
        lines = result.stdout.strip().split('\n')
        
        # Look for the JSON array that starts with [ and contains the actual data
        in_json_array = False
        json_content = ""
        
        for line in lines:
            line = line.strip()
            
            # Start collecting when we see the opening bracket
            if line == '[':
                in_json_array = True
                json_content = line
                continue
            
            # Continue collecting until we see the closing bracket
            if in_json_array:
                json_content += '\n' + line
                if line == ']':
                    # We've collected the complete JSON array
                    try:
                        json_data = json.loads(json_content)
                        if isinstance(json_data, list):
                            data.extend(json_data)
                        break
                    except json.JSONDecodeError as e:
                        print(f"DEBUG: JSON parse error: {e}")
                        print(f"DEBUG: JSON content: {json_content}")
                        break
        
        print(f"DEBUG: Parsed {len(data)} records")
        return data
    
    def get_pg_connection_string(self):
        """Get PostgreSQL connection string with OAuth token"""
        if not self.oauth_token:
            self.generate_oauth_token()
        
        encoded_username = urllib.parse.quote(self.pg_username, safe='')
        
        # Map instance name to hostname
        hostname = config.get_pg_hostname(self.pg_instance)
        
        return f"postgresql://{encoded_username}:{self.oauth_token}@{hostname}:5432/{config.pg_database}?sslmode=require"
    
    def execute_pg_query(self, query, return_output=False):
        """Execute PostgreSQL query with OAuth authentication"""
        if not self.oauth_token:
            self.generate_oauth_token()
        
        env = os.environ.copy()
        env["PGPASSWORD"] = self.oauth_token
        
        conn_string = self.get_pg_connection_string()
        cmd = ["psql", conn_string, "-c", query]
        
        if return_output:
            cmd.extend(["-t", "-A"])  # Tuples only, unaligned output
        
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"PostgreSQL query failed: {result.stderr}")
        
        return result.stdout.strip() if return_output else None
    
    def create_scd_table(self, schema_name, table_name, columns):
        """Create SCD Type 2 table with required columns"""
        # Add SCD Type 2 columns if not present
        scd_columns = {
            'scd_id': 'SERIAL PRIMARY KEY',
            'effective_date': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'end_date': 'TIMESTAMP',
            'is_current': 'BOOLEAN DEFAULT TRUE',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
        
        # Merge business columns with SCD columns
        all_columns = {**columns, **scd_columns}
        
        column_defs = []
        for col_name, col_type in all_columns.items():
            column_defs.append(f"{col_name} {col_type}")
        
        create_table_sql = f"""
        CREATE SCHEMA IF NOT EXISTS "{schema_name}";
        
        CREATE TABLE IF NOT EXISTS "{schema_name}".{table_name} (
            {', '.join(column_defs)}
        );
        
        -- Create unique index on business key + effective_date for SCD Type 2
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_business_key_effective 
        ON "{schema_name}".{table_name} (
            {', '.join([col for col in columns.keys() if 'id' in col.lower() or 'key' in col.lower()][:1])}, 
            effective_date
        );
        """
        
        self.execute_pg_query(create_table_sql)
        print(f"Created SCD Type 2 table: {schema_name}.{table_name}")
    
    def upsert_scd_data(self, schema_name, table_name, data_rows, business_key_columns):
        """Perform SCD Type 2 UPSERT using PostgreSQL ON CONFLICT"""
        if not data_rows:
            print("No data to upsert")
            return
        
        # Build VALUES clause for the data
        values_clauses = []
        for row in data_rows:
            # Add SCD Type 2 metadata to each row
            row_values = []
            for col, value in row.items():
                if value is None:
                    row_values.append('NULL')
                elif isinstance(value, str):
                    row_values.append(f"'{value.replace("'", "''")}'")  # Escape single quotes
                else:
                    row_values.append(str(value))
            
            # Add SCD metadata
            row_values.extend([
                'CURRENT_TIMESTAMP',  # effective_date
                'NULL',               # end_date
                'TRUE',               # is_current
                'CURRENT_TIMESTAMP',  # created_at
                'CURRENT_TIMESTAMP'   # updated_at
            ])
            
            values_clauses.append(f"({', '.join(row_values)})")
        
        # Get column names from first row plus SCD columns
        if data_rows:
            data_columns = list(data_rows[0].keys())
            all_columns = data_columns + ['effective_date', 'end_date', 'is_current', 'created_at', 'updated_at']
        
        # Create conflict target (business key columns)
        conflict_columns = ', '.join(business_key_columns)
        
        # Build business key values for UPDATE WHERE clause
        business_key_values = []
        for row in data_rows:
            key_values = []
            for key_col in business_key_columns:
                value = row.get(key_col)
                if value is None:
                    key_values.append('NULL')
                elif isinstance(value, str):
                    key_values.append(f"'{value.replace("'", "''")}'")  # Escape single quotes
                else:
                    key_values.append(str(value))
            business_key_values.append(f"({', '.join(key_values)})")
        
        # Proper SCD Type 2 using single INSERT with ON CONFLICT
        # The key insight: use a partial unique index on business_key WHERE is_current = TRUE
        
        # Create partial unique index for SCD Type 2
        index_name = f"idx_{table_name}_business_key_current"
        index_sql = f"""
        DROP INDEX IF EXISTS "{schema_name}".{index_name};
        CREATE UNIQUE INDEX {index_name} 
        ON "{schema_name}".{table_name} ({conflict_columns}) 
        WHERE is_current = TRUE;
        """
        
        # SCD Type 2 UPSERT: close existing current record and insert new one
        upsert_sql = f"""
        INSERT INTO "{schema_name}".{table_name} ({', '.join(all_columns)})
        VALUES {', '.join(values_clauses)}
        ON CONFLICT ({conflict_columns}) WHERE is_current = TRUE
        DO UPDATE SET
            end_date = CURRENT_TIMESTAMP,
            is_current = FALSE,
            updated_at = CURRENT_TIMESTAMP
        WHERE (
            -- Only close if data has actually changed
            {' OR '.join([f'COALESCE("{schema_name}".{table_name}.{col}, \'\') != COALESCE(EXCLUDED.{col}, \'\')'
                         for col in data_columns if col not in business_key_columns])}
        );
        
        -- Insert new current records for all incoming data
        INSERT INTO "{schema_name}".{table_name} ({', '.join(all_columns)})
        VALUES {', '.join(values_clauses)}
        ON CONFLICT ({conflict_columns}) WHERE is_current = TRUE
        DO NOTHING;
        """
        
        # Execute index creation and upsert
        self.execute_pg_query(index_sql)
        self.execute_pg_query(upsert_sql)
        
        print(f"Upserted {len(data_rows)} records to {schema_name}.{table_name}")
    
    def sync_delta_to_postgres_scd(self, delta_table, pg_schema, pg_table, business_key_columns, 
                                   column_mapping=None, delta_query=None):
        """Complete workflow: Delta -> PostgreSQL SCD Type 2"""
        print(f"Starting Delta to PostgreSQL SCD Type 2 sync...")
        print(f"Source: {delta_table}")
        print(f"Target: {pg_schema}.{pg_table}")
        
        # Step 1: Read from Delta table
        print("Step 1: Reading Delta table...")
        data_rows = self.read_delta_table(delta_table, delta_query)
        print(f"Read {len(data_rows)} records from Delta table")
        
        if not data_rows:
            print("No data found in Delta table")
            return
        
        # Step 2: Apply column mapping if provided
        if column_mapping:
            mapped_rows = []
            for row in data_rows:
                mapped_row = {}
                for old_col, new_col in column_mapping.items():
                    if old_col in row:
                        mapped_row[new_col] = row[old_col]
                # Keep unmapped columns
                for col, value in row.items():
                    if col not in column_mapping:
                        mapped_row[col] = value
                mapped_rows.append(mapped_row)
            data_rows = mapped_rows
        
        # Step 3: Generate OAuth token
        print("Step 2: Generating OAuth token...")
        token, expiration = self.generate_oauth_token()
        print(f"OAuth token generated (expires: {expiration})")
        
        # Step 4: Create target table structure
        print("Step 3: Creating/updating target table...")
        
        # Infer column types from first row
        column_types = {}
        if data_rows:
            first_row = data_rows[0]
            for col, value in first_row.items():
                if isinstance(value, int):
                    column_types[col] = 'INTEGER'
                elif isinstance(value, float):
                    column_types[col] = 'DECIMAL'
                elif isinstance(value, bool):
                    column_types[col] = 'BOOLEAN'
                else:
                    column_types[col] = 'TEXT'
        
        self.create_scd_table(pg_schema, pg_table, column_types)
        
        # Step 5: Perform SCD Type 2 UPSERT
        print("Step 4: Performing SCD Type 2 UPSERT...")
        self.upsert_scd_data(pg_schema, pg_table, data_rows, business_key_columns)
        
        print("✅ Delta to PostgreSQL SCD Type 2 sync completed successfully!")
        
        # Return summary
        return {
            "source_table": delta_table,
            "target_table": f"{pg_schema}.{pg_table}",
            "records_processed": len(data_rows),
            "business_keys": business_key_columns,
            "oauth_token_expires": expiration
        }

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Delta to PostgreSQL SCD Type 2 Pipeline")
    parser.add_argument("--workspace", required=True, help="Databricks workspace URL")
    parser.add_argument("--token", required=True, help="Databricks personal access token")
    parser.add_argument("--delta-table", required=True, help="Delta table name (Unity Catalog format)")
    parser.add_argument("--pg-schema", required=True, help="PostgreSQL schema name")
    parser.add_argument("--pg-table", required=True, help="PostgreSQL table name")
    parser.add_argument("--business-keys", required=True, help="Comma-separated business key columns")
    parser.add_argument("--delta-query", help="Custom query for Delta table (optional)")
    parser.add_argument("--column-mapping", help="JSON column mapping (optional)")
    
    args = parser.parse_args()
    
    # Parse business keys
    business_keys = [key.strip() for key in args.business_keys.split(',')]
    
    # Parse column mapping if provided
    column_mapping = None
    if args.column_mapping:
        column_mapping = json.loads(args.column_mapping)
    
    # Initialize pipeline
    pipeline = DeltaToPostgresSCD(
        workspace_url=args.workspace,
        databricks_token=args.token
    )
    
    # Execute sync
    result = pipeline.sync_delta_to_postgres_scd(
        delta_table=args.delta_table,
        pg_schema=args.pg_schema,
        pg_table=args.pg_table,
        business_key_columns=business_keys,
        column_mapping=column_mapping,
        delta_query=args.delta_query
    )
    
    print("\n📊 Sync Summary:")
    for key, value in result.items():
        print(f"  {key}: {value}")

if __name__ == "__main__":
    main()
