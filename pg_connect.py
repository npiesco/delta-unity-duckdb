#!/usr/bin/env python3
"""
Connect to Databricks PostgreSQL using OAuth tokens
"""

import os
import sys
import uuid
import argparse
import subprocess
from databricks.sdk import WorkspaceClient
from config import config

def generate_token(workspace_url=None, instance_name=None, databricks_token=None):
    """Generate an OAuth token for PostgreSQL authentication"""
    
    # Use config or environment variables if not provided
    workspace_url = workspace_url or config.databricks_workspace_url
    instance_name = instance_name or config.pg_instance_name
    databricks_token = databricks_token or config.databricks_token
    
    if not workspace_url:
        raise ValueError("Databricks workspace URL not provided. Use --host or set DATABRICKS_HOST environment variable.")
    
    if not databricks_token:
        raise ValueError("Databricks token not provided. Use --token or set DATABRICKS_TOKEN environment variable.")
    
    if not instance_name:
        raise ValueError("PostgreSQL instance name not provided. Use --instance parameter.")
    
    # Initialize the Workspace client with explicit credentials
    w = WorkspaceClient(
        host=workspace_url,
        token=databricks_token
    )
    
    # Generate the database credential
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name]
    )
    
    return {
        "token": cred.token,
        "expiration_time": cred.expiration_time
    }

def run_psql_query(token, instance_name, username, database, query=None, interactive=False):
    """Run a PostgreSQL query using the OAuth token"""
    
    # Set PGPASSWORD environment variable
    env = os.environ.copy()
    env["PGPASSWORD"] = token
    
    # URL encode the username for the connection string
    import urllib.parse
    encoded_username = urllib.parse.quote(username, safe='')
    
    # Construct the connection string using config
    hostname = config.get_pg_hostname(instance_name)
    
    conn_string = f"postgresql://{encoded_username}@{hostname}:5432/{database}?sslmode=require"
    
    if interactive:
        # Run interactive psql session
        print(f"Starting interactive PostgreSQL session...")
        process = subprocess.Popen(["psql", conn_string], env=env)
        return process.wait()
    else:
        # Run the psql command with query
        cmd = ["psql", conn_string, "-c", query]
        result = subprocess.run(cmd, env=env)
        return result.returncode

def main():
    parser = argparse.ArgumentParser(description="Connect to Databricks PostgreSQL using OAuth")
    parser.add_argument("--workspace", "--host", help="Databricks workspace URL")
    parser.add_argument("--token", help="Databricks personal access token")
    parser.add_argument("--instance", default=config.pg_instance_name, 
                        help="PostgreSQL instance name")
    parser.add_argument("--username", default=config.pg_username, 
                        help="PostgreSQL username (email)")
    parser.add_argument("--database", default=config.pg_database, 
                        help="Database name")
    parser.add_argument("--query", help="SQL query to execute")
    parser.add_argument("--interactive", action="store_true", 
                        help="Start interactive psql session")
    parser.add_argument("--token-only", action="store_true", 
                        help="Only output the token")
    
    args = parser.parse_args()
    
    # Generate token
    try:
        result = generate_token(args.workspace, args.instance, args.token)
        token = result["token"]
        
        if args.token_only:
            print(token)
            return 0
            
        print(f"OAuth token generated successfully (expires: {result['expiration_time']})")
        
        # Run query or interactive session
        if args.interactive:
            return run_psql_query(token, args.instance, args.username, args.database, interactive=True)
        elif args.query:
            print(f"Executing query: {args.query}")
            return run_psql_query(token, args.instance, args.username, args.database, query=args.query)
        else:
            # Default to interactive mode if no query provided
            return run_psql_query(token, args.instance, args.username, args.database, interactive=True)
            
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
