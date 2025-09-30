#!/usr/bin/env python3
"""
Configuration management for Delta to PostgreSQL SCD pipeline
"""

import os
from typing import Dict, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class that loads from environment variables with defaults"""
    
    def __init__(self):
        # Databricks configuration
        self.databricks_workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
        self.databricks_token = os.getenv('DATABRICKS_TOKEN')
        
        # PostgreSQL configuration
        self.pg_instance_name = os.getenv('PG_INSTANCE_NAME')
        self.pg_username = os.getenv('PG_USERNAME')
        self.pg_database = os.getenv('PG_DATABASE')
        self.pg_schema = os.getenv('PG_SCHEMA')
        
        # Instance hostname mappings - load from environment or use dynamic mapping
        instance_hostnames_str = os.getenv('PG_INSTANCE_HOSTNAMES', '{}')
        try:
            import json
            self.instance_hostnames = json.loads(instance_hostnames_str)
        except (json.JSONDecodeError, ValueError):
            self.instance_hostnames = {}
        
        # Default Delta table configuration
        self.default_delta_table = os.getenv('DEFAULT_DELTA_TABLE')
        default_keys = os.getenv('DEFAULT_BUSINESS_KEYS')
        self.default_business_keys = default_keys.split(',') if default_keys else []
        
    def get_pg_hostname(self, instance_name: str) -> str:
        """Get PostgreSQL hostname for instance"""
        return self.instance_hostnames.get(
            instance_name, 
            f"{instance_name}.database.azuredatabricks.net"
        )
    
    def validate(self) -> Dict[str, str]:
        """Validate required configuration and return any missing values"""
        missing = {}
        
        if not self.databricks_workspace_url:
            missing['DATABRICKS_WORKSPACE_URL'] = 'Databricks workspace URL is required'
        
        if not self.databricks_token:
            missing['DATABRICKS_TOKEN'] = 'Databricks personal access token is required'
        
        if not self.pg_instance_name:
            missing['PG_INSTANCE_NAME'] = 'PostgreSQL instance name is required'
        
        if not self.pg_username:
            missing['PG_USERNAME'] = 'PostgreSQL username is required'
        
        if not self.pg_database:
            missing['PG_DATABASE'] = 'PostgreSQL database name is required'
        
        if not self.pg_schema:
            missing['PG_SCHEMA'] = 'PostgreSQL schema name is required'
            
        return missing
    
    def to_dict(self) -> Dict[str, any]:
        """Convert config to dictionary for logging/debugging"""
        return {
            'databricks_workspace_url': self.databricks_workspace_url,
            'databricks_token': '***' if self.databricks_token else None,
            'pg_instance_name': self.pg_instance_name,
            'pg_username': self.pg_username,
            'pg_database': self.pg_database,
            'pg_schema': self.pg_schema,
            'default_delta_table': self.default_delta_table,
            'default_business_keys': self.default_business_keys
        }

# Global config instance
config = Config()
