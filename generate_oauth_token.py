#!/usr/bin/env python3
"""
Generate OAuth token using REST API directly
"""

import requests
import os
import uuid
from databricks.sdk import WorkspaceClient
from config import config

def generate_oauth_token(workspace_url, databricks_token, instance_name):
    """Generate OAuth token using REST API"""
    
    w = WorkspaceClient(
        host=workspace_url,
        token=databricks_token
    )
    
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name]
    )
    
    if cred.status_code == 200:
        data = cred.json()
        return data.get("token"), data.get("expiration_time")
    else:
        print(f"Error: {cred.status_code}")
        print(f"Response: {cred.text}")
        return None, None

if __name__ == "__main__":
    # Use config values
    token, expiration = generate_oauth_token(
        config.databricks_workspace_url, 
        config.databricks_token, 
        config.pg_instance_name
    )
    
    if token:
        print(f"OAuth Token: {token}")
        print(f"Expires: {expiration}")
    else:
        sys.exit(1)
