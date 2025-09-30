#!/usr/bin/env python3
"""
Load Cypher queries from Mission_Cypher.csv into Memgraph database
"""

import json
import csv
import sys
from neo4j import GraphDatabase

def load_cypher_queries():
    """Load Cypher queries from CSV file"""
    try:
        csv_file_path = '/Users/nicholas.piesco/Downloads/Mission_Cypher.csv'
        
        queries = []
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # The CSV contains JSON strings with escaped quotes
                # Need to handle the escaped JSON format
                json_str = row['cypher_json']
                # Parse the JSON string from the cypher_json column
                cypher_data = json.loads(json_str)
                if 'cypher' in cypher_data and cypher_data['cypher']:
                    queries.append(cypher_data['cypher'])
        
        print(f"Loaded {len(queries)} Cypher queries from {csv_file_path}")
        return queries
    
    except Exception as e:
        print(f"Error loading queries: {e}")
        print(f"Sample row data: {row if 'row' in locals() else 'No row data'}")
        return []

def connect_to_memgraph():
    """Connect to Memgraph database"""
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", 
                                    auth=("memgraph", "memgraph"))
        return driver
    except Exception as e:
        print(f"Error connecting to Memgraph: {e}")
        return None

def execute_queries(driver, queries):
    """Execute Cypher queries in Memgraph"""
    success_count = 0
    error_count = 0
    
    with driver.session() as session:
        for i, query_block in enumerate(queries):
            # Split each query block by newlines and execute each statement separately
            # This avoids variable redeclaration issues in Memgraph
            statements = [stmt.strip() for stmt in query_block.split('\n') if stmt.strip()]
            
            block_success = True
            for stmt in statements:
                try:
                    session.run(stmt)
                except Exception as e:
                    block_success = False
                    error_count += 1
                    if error_count <= 5:  # Show first 5 errors only
                        print(f"Error executing statement in block {i+1}: {e}")
                        print(f"Statement: {stmt[:200]}...")
                        print("---")
                    break  # Skip remaining statements in this block
            
            if block_success:
                success_count += 1
            
            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1} query blocks...")
    
    print(f"Execution complete: {success_count} successful, {error_count} errors")

def verify_data(driver):
    """Verify data was loaded correctly"""
    with driver.session() as session:
        # Count total missions
        result = session.run("MATCH (m:Mission) RETURN count(m) as total")
        total_missions = result.single()['total']
        print(f"Total missions loaded: {total_missions}")
        
        # Count relationships
        result = session.run("MATCH ()-[r:IS_PARENT_TO]->() RETURN count(r) as total")
        total_relationships = result.single()['total']
        print(f"Total parent-child relationships: {total_relationships}")
        
        # Show sample data
        result = session.run("MATCH (p:Mission)-[r:IS_PARENT_TO]->(c:Mission) RETURN p.id, p.title, c.id, c.title LIMIT 5")
        print("\nSample parent-child relationships:")
        for record in result:
            print(f"  {record['p.id']} '{record['p.title']}' -> {record['c.id']} '{record['c.title']}'")

def main():
    print("Loading Cypher queries from Mission_Cypher.csv...")
    queries = load_cypher_queries()
    
    if not queries:
        print("No queries to execute")
        return
    
    print("Connecting to Memgraph...")
    driver = connect_to_memgraph()
    
    if not driver:
        print("Failed to connect to Memgraph")
        return
    
    try:
        print("Executing queries...")
        execute_queries(driver, queries)
        
        print("Verifying data...")
        verify_data(driver)
        
    finally:
        driver.close()
        print("Connection closed")

if __name__ == "__main__":
    main()
