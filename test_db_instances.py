#!/usr/bin/env python3
"""
Test script to list available database instances
"""
import os
from databricks.sdk import WorkspaceClient
from config import config

# Initialize the Workspace client using config
w = WorkspaceClient(
    host=config.databricks_workspace_url,
    token=config.databricks_token
)

try:
    # Try to list database instances
    instances = w.database.list_database_instances()
    print("Available database instances:")
    for instance in instances:
        print(f"- {instance.name}")
        print(f"  ID: {instance.id}")
        print(f"  State: {instance.state}")
        print()
except Exception as e:
    print(f"Error listing instances: {e}")
    
    # Try alternative approach - generate credential with a different instance name
    import uuid
    try:
        print("\nTrying to generate credential with the known instance name...")
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=["lakebase-test"]
        )
        print(f"Success! Token generated: {cred.token[:50]}...")
        print(f"Expires: {cred.expiration_time}")
    except Exception as e2:
        print(f"Error generating credential: {e2}")
