#!/usr/bin/env python3
"""
Dynamic Query Executor for missions_synced table
Allows passing SQL queries as command line arguments or interactive mode
"""

import sys
import argparse
import json
from datetime import datetime
import psycopg2
from pg_connect import generate_token
from config import config
import urllib.parse

class PostgreSQLQueryExecutor:
    """Execute queries against PostgreSQL tables"""
    
    def __init__(self, table_name=None):
        self.connection = None
        self.cursor = None
        self.table_name = table_name or self._get_table_name()
    
    def _get_table_name(self):
        """Get table name from user input or default"""
        # Show available tables first
        print(f"🔍 Connecting to {config.pg_database}.{config.pg_schema}...")
        temp_conn = None
        try:
            result = generate_token()
            token = result['token']
            encoded_username = urllib.parse.quote(config.pg_username, safe='')
            hostname = config.get_pg_hostname(config.pg_instance_name)
            conn_string = f'postgresql://{encoded_username}:{token}@{hostname}:5432/{config.pg_database}?sslmode=require'
            temp_conn = psycopg2.connect(conn_string)
            cursor = temp_conn.cursor()
            cursor.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{config.pg_schema}' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            if tables:
                print(f"📋 Available tables: {', '.join(tables)}")
            
        except Exception as e:
            print(f"⚠️  Could not list tables: {e}")
        finally:
            if temp_conn:
                temp_conn.close()
        
        return input("Enter table name (default: missions_synced): ").strip() or "missions_synced"
    
    def _get_available_tables(self):
        """Get list of available tables in the schema"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{config.pg_schema}' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return tables
        except:
            return []
        
    def connect(self):
        """Establish connection to PostgreSQL"""
        try:
            # Generate OAuth token
            result = generate_token()
            token = result['token']
            
            # Build connection string
            encoded_username = urllib.parse.quote(config.pg_username, safe='')
            hostname = config.get_pg_hostname(config.pg_instance_name)
            conn_string = f'postgresql://{encoded_username}:{token}@{hostname}:5432/{config.pg_database}?sslmode=require'
            
            self.connection = psycopg2.connect(conn_string)
            self.cursor = self.connection.cursor()
            print(f"✅ Connected to {config.pg_database}.{config.pg_schema}")
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            sys.exit(1)
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query, output_format='table'):
        """Execute SQL query and return results"""
        try:
            # Add schema prefix if not specified
            if 'FROM ' in query.upper() and f'{config.pg_schema}.' not in query:
                query = query.replace(self.table_name, f'{config.pg_schema}.{self.table_name}')
            
            print(f"🔍 Executing: {query}")
            self.cursor.execute(query)
            
            # Handle different query types
            if query.strip().upper().startswith(('SELECT', 'WITH')):
                results = self.cursor.fetchall()
                columns = [desc[0] for desc in self.cursor.description]
                
                if output_format == 'json':
                    return self._format_json(results, columns)
                else:
                    return self._format_table(results, columns)
                    
            else:
                # For INSERT, UPDATE, DELETE
                self.connection.commit()
                return f"✅ Query executed successfully. Rows affected: {self.cursor.rowcount}"
                
        except Exception as e:
            self.connection.rollback()
            return f"❌ Query failed: {e}"
    
    def _format_table(self, results, columns):
        """Format results as a table"""
        if not results:
            return "No results found."
        
        # Calculate column widths
        widths = [len(col) for col in columns]
        for row in results:
            for i, val in enumerate(row):
                widths[i] = max(widths[i], len(str(val)) if val is not None else 4)
        
        # Build table
        output = []
        
        # Header
        header = " | ".join(col.ljust(widths[i]) for i, col in enumerate(columns))
        output.append(header)
        output.append("-" * len(header))
        
        # Rows
        for row in results:
            formatted_row = " | ".join(
                str(val).ljust(widths[i]) if val is not None else "NULL".ljust(widths[i])
                for i, val in enumerate(row)
            )
            output.append(formatted_row)
        
        return "\n".join(output)
    
    def _format_json(self, results, columns):
        """Format results as JSON"""
        data = []
        for row in results:
            record = {}
            for i, col in enumerate(columns):
                val = row[i]
                # Handle datetime serialization
                if isinstance(val, datetime):
                    val = val.isoformat()
                record[col] = val
            data.append(record)
        
        return json.dumps(data, indent=2, default=str)
    
    def interactive_mode(self):
        """Interactive query mode"""
        print(f"🚀 Interactive Query Mode for {config.pg_schema}.{self.table_name}")
        
        # Show available tables
        tables = self._get_available_tables()
        if tables:
            print(f"📋 Available tables in {config.pg_schema}: {', '.join(tables)}")
        
        print("Type 'exit' to quit, 'help' for examples, 'tables' to list tables")
        print("-" * 50)
        
        while True:
            try:
                query = input("\nSQL> ").strip()
                
                if query.lower() == 'exit':
                    break
                elif query.lower() == 'help':
                    self._show_examples()
                    continue
                elif query.lower() == 'tables':
                    self._show_tables()
                    continue
                elif not query:
                    continue
                
                result = self.execute_query(query)
                print(result)
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except EOFError:
                break
    
    def _show_tables(self):
        """Show available tables"""
        tables = self._get_available_tables()
        if tables:
            print(f"\n📋 Available tables in {config.pg_schema}:")
            for i, table in enumerate(tables, 1):
                marker = " ⭐" if table == self.table_name else ""
                print(f"  {i}. {table}{marker}")
        else:
            print(f"\n❌ No tables found in {config.pg_schema}")
    
    def _show_examples(self):
        """Show example queries"""
        table_name = f"{config.pg_schema}.{self.table_name}"
        examples = [
            f"SELECT COUNT(*) FROM {table_name}",
            f"SELECT * FROM {table_name} LIMIT 5",
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table_name}'",
            f"SELECT * FROM {table_name} WHERE column_name = 'value' LIMIT 10"
        ]
        
        print("\n📋 Example queries:")
        for i, example in enumerate(examples, 1):
            print(f"  {i}. {example}")

def main():
    parser = argparse.ArgumentParser(description=f'Query PostgreSQL tables in {config.pg_schema}')
    parser.add_argument('query', nargs='?', help='SQL query to execute')
    parser.add_argument('--table', '-t', help='Table name to query (default: missions_synced)')
    parser.add_argument('--format', choices=['table', 'json'], default='table', 
                       help='Output format (default: table)')
    parser.add_argument('--interactive', '-i', action='store_true', 
                       help='Start interactive mode')
    parser.add_argument('--list-tables', action='store_true',
                       help='List available tables and exit')
    
    args = parser.parse_args()
    
    # Handle table listing
    if args.list_tables:
        executor = PostgreSQLQueryExecutor(table_name='missions_synced')  # temp for connection
        executor.connect()
        executor._show_tables()
        executor.disconnect()
        return
    
    # Get table name from args or prompt user
    table_name = args.table
    if not table_name and not args.interactive and not args.query:
        print(f"📋 Available options:")
        print(f"  1. Specify table with --table <name>")
        print(f"  2. Use interactive mode with -i")
        print(f"  3. List tables with --list-tables")
        table_name = input(f"\nEnter table name (default: missions_synced): ").strip() or "missions_synced"
    
    executor = PostgreSQLQueryExecutor(table_name=table_name)
    executor.connect()
    
    try:
        if args.interactive or not args.query:
            executor.interactive_mode()
        else:
            result = executor.execute_query(args.query, args.format)
            print(result)
    
    finally:
        executor.disconnect()

if __name__ == "__main__":
    main()
