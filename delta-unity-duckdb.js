#!/usr/bin/env node
/**
 * Standalone Delta Table Scanner using DuckDB
 * 
 * This script allows querying any Delta table using DuckDB.
 * It is designed to work with Azure Databricks Delta tables through Unity Catalog.
 * It can be used as a CLI tool or imported as a module.
 * 
 * Usage:
 *   delta-unity-duckdb --table=<table_path> [--query=<sql_query>] [--limit=<limit>]
 *   delta-unity-duckdb --table=<table_path> --databricks_host=<host> --databricks_token=<token>
 *   
 * Examples:
 *   # Using .env file configuration
 *   delta-unity-duckdb --table="catalog.schema.table_name"
 *   
 *   # Using CLI configuration
 *   delta-unity-duckdb --table="catalog.schema.table_name" \
 *     --databricks_host="https://your-workspace.cloud.databricks.com" \
 *     --databricks_token="your-token-here"
 *   
 *   # Direct ABFSS path
 *   delta-unity-duckdb --table="abfss://container@storage-account.dfs.core.windows.net/path/to/delta"
 */

// Load environment variables from .env file if present
try {
  require('dotenv').config();
} catch (error) {
  console.warn('dotenv not found, skipping .env loading');
}

// Import required packages
let duckdb;
try {
  duckdb = require('@duckdb/node-api');
} catch (error) {
  console.error('Failed to load @duckdb/node-api. Please install it with:');
  console.error('npm install @duckdb/node-api');
  process.exit(1);
}

// Import fetch for Unity Catalog API calls
let fetch;
try {
  fetch = require('node-fetch');
} catch (error) {
  console.warn('node-fetch not found, Unity Catalog API integration will be unavailable');
  console.warn('Install with: npm install node-fetch');
}

/**
 * DeltaScanner class for querying Delta tables with DuckDB
 */
class DeltaScanner {
  constructor(options = {}) {
    this.instance = null;
    this.conn = null;
    this.initialized = false;
    this.options = {
      memory: true,
      extensions: ['httpfs', 'delta', 'azure'],
      ...options
    };
  }

  /**
   * Initialize DuckDB connection and load required extensions
   */
  async initialize() {
    if (this.initialized) return;

    try {
      // Create a new database connection using the DuckDB Node.js API (matching server-reader.js)
      const { DuckDBConnection } = duckdb;
      this.conn = await DuckDBConnection.create();

      // Load required extensions
      console.log('INFO: Setting up DuckDB extensions');
      
      // Load Delta Lake extension
      await this.conn.run('INSTALL delta; LOAD delta;');
      console.log('INFO: Delta extension loaded');
      
      // Load Azure extension if needed
      if (this.options.extensions.includes('azure')) {
        await this.conn.run('INSTALL azure; LOAD azure;');
        console.log('INFO: Azure extension loaded');
      }
      
      // Load HTTP FS extension
      if (this.options.extensions.includes('httpfs')) {
        await this.conn.run('INSTALL httpfs; LOAD httpfs;');
        console.log('INFO: HTTP FS extension loaded');
      }

      // Configure Azure transport options (matching server-reader.js)
      await this.conn.run("SET azure_transport_option_type = 'curl'");
      console.log('INFO: Azure transport options configured');
      
      // Configure Unity Catalog credentials if available
      if (process.env.DATABRICKS_WORKSPACE_URL && process.env.DATABRICKS_TOKEN) {
        console.log('INFO: Using Databricks Unity Catalog API for credentials');
        this.databricksHost = process.env.DATABRICKS_WORKSPACE_URL;
        this.databricksToken = process.env.DATABRICKS_TOKEN;
      }

      this.initialized = true;
    } catch (error) {
      console.error('Failed to initialize DuckDB:', error);
      throw new Error(`Failed to initialize DuckDB: ${error}`);
    }
  }

  /**
   * Get table information from Unity Catalog API
   * @param {string} tableName - Table name in format catalog.schema.table
   * @returns {Promise<object>} - Table information
   */
  async getTableInfo(tableName) {
    if (!this.databricksHost || !this.databricksToken || !fetch) {
      throw new Error('Unity Catalog API credentials not configured or node-fetch not available');
    }
    
    // Parse table name
    const parts = tableName.split('.');
    if (parts.length !== 3) {
      throw new Error('Table name must be in format catalog.schema.table');
    }
    
    const [catalog, schema, table] = parts;
    const url = `${this.databricksHost}/api/2.1/unity-catalog/tables/${catalog}.${schema}.${table}`;
    
    try {
      console.log(`INFO: Getting table information for: ${tableName}`);
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${this.databricksToken}`,
          'Content-Type': 'application/json'
        }
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Unity Catalog API error: ${response.status} ${errorText}`);
      }
      
      const tableInfo = await response.json();
      console.log(`INFO: Table found - ID: ${tableInfo.table_id}`);
      return tableInfo;
    } catch (error) {
      console.error(`Error getting table information for ${tableName}:`, error);
      throw new Error(`Failed to get table information: ${error}`);
    }
  }
  
  /**
   * Generate temporary credentials for a Unity Catalog table
   * @param {string} tableId - Table ID from Unity Catalog
   * @param {string} operation - Table operation (READ or WRITE)
   * @returns {Promise<object>} - Temporary credentials
   */
  async generateTempCredentials(tableId, operation = 'READ') {
    if (!this.databricksHost || !this.databricksToken || !fetch) {
      throw new Error('Unity Catalog API credentials not configured or node-fetch not available');
    }
    
    const url = `${this.databricksHost}/api/2.1/unity-catalog/temporary-table-credentials`;
    
    try {
      console.log(`INFO: Generating temporary credentials for table ID: ${tableId}`);
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.databricksToken}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          table_id: tableId,
          operation: operation.toUpperCase()
        })
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Unity Catalog API error: ${response.status} ${errorText}`);
      }
      
      const cred = await response.json();
      
      // Extract storage account name from URL
      const storageAccountMatch = cred.url.match(/@([^.]+)\.dfs\./);
      
      if (!storageAccountMatch) {
        throw new Error(`Could not extract storage account from URL: ${cred.url}`);
      }
      
      const storageAccountName = storageAccountMatch[1];
      
      // Create result object matching server-reader.js format
      const result = {
        url: cred.url,
        expiration_time: cred.expiration_time,
        storage_account_name: storageAccountName,
        sas_token: cred.azure_user_delegation_sas.sas_token,
        delta_path: cred.url // Use the URL as the delta path
      };
      
      console.log(`INFO: Temporary credentials generated for table ID: ${tableId}`);
      return result;
    } catch (error) {
      console.error(`Error generating temporary credentials for table ID ${tableId}:`, error);
      throw new Error(`Failed to generate temporary credentials: ${error}`);
    }
  }
  
  /**
   * Configure Azure storage credentials for a table
   * @param {object} credentials - Temporary credentials from Unity Catalog
   */
  async configureAzureCredentials(credentials) {
    if (!this.conn) {
      throw new Error('DuckDB connection not initialized');
    }
    
    try {
      // Extract storage account from credentials
      const storageAccount = credentials.storage_account_name;
      const sasToken = credentials.sas_token;
      
      console.log(`INFO: Setting up Azure secret: azure_temp_secret for storage account: ${storageAccount}`);
      
      // Create Azure secret with SAS token using the correct DuckDB syntax (matching server-reader.js)
      const azureSecretSql = `
        CREATE OR REPLACE SECRET azure_temp_secret (
          TYPE AZURE,
          CONNECTION_STRING 'AccountName=${storageAccount};SharedAccessSignature=${sasToken}'
        )
      `;
      await this.conn.run(azureSecretSql);
      
      console.log('INFO: Azure secret azure_temp_secret created with SAS token');
    } catch (error) {
      console.error('Error configuring Azure credentials:', error);
      throw new Error(`Failed to configure Azure credentials: ${error}`);
    }
  }

  /**
   * Execute a SQL query on a Delta table
   * @param {string} tablePath - Path to the Delta table
   * @param {string} sql - SQL query to execute (optional)
   * @param {object} options - Additional options
   * @returns {Promise<object>} - Query results
   */
  async query(tablePath, sql = null, options = {}) {
    await this.initialize();
    
    if (!this.conn) {
      throw new Error('DuckDB connection not initialized');
    }
    
    // Check if this is a Unity Catalog table (format: catalog.schema.table)
    const isUnityCatalogTable = tablePath.split('.').length === 3 && 
                               !tablePath.startsWith('abfss://') && 
                               !tablePath.startsWith('s3://') &&
                               !tablePath.startsWith('/');
    
    // Check if this is an ABFSS path that might need SAS token
    const isAbfssPath = tablePath.startsWith('abfss://') || tablePath.includes('dfs.core.windows.net');
    
    // If Unity Catalog table, get the ABFSS path
    let deltaPath = tablePath;
    
    // Handle Unity Catalog tables
    if (isUnityCatalogTable && this.databricksHost && this.databricksToken && fetch) {
      try {
        // Get table info
        const tableInfo = await this.getTableInfo(tablePath);
        
        // Generate temporary credentials
        const credentials = await this.generateTempCredentials(tableInfo.table_id);
        
        // Configure Azure credentials
        await this.configureAzureCredentials(credentials);
        
        // Use the ABFSS path
        deltaPath = credentials.delta_path;
      } catch (error) {
        console.error(`Error setting up Unity Catalog table ${tablePath}:`, error);
        throw new Error(`Failed to set up Unity Catalog table: ${error}`);
      }
    }
    // Handle direct ABFSS paths with SAS token from environment
    else if (isAbfssPath && process.env.AZURE_STORAGE_SAS_TOKEN) {
      try {
        // Extract storage account from ABFSS path
        const abfssMatch = tablePath.match(/abfss:\/\/([^@]+)@([^.]+)\.dfs\.core\.windows\.net/);
        let storageAccount;
        
        if (abfssMatch && abfssMatch.length >= 3) {
          storageAccount = abfssMatch[2];
        } else if (process.env.AZURE_STORAGE_ACCOUNT_NAME) {
          storageAccount = process.env.AZURE_STORAGE_ACCOUNT_NAME;
        } else {
          throw new Error('Could not determine Azure storage account from ABFSS path or environment');
        }
        
        console.log(`INFO: Setting up Azure credentials for direct ABFSS path with storage account: ${storageAccount}`);
        
        // Set up Azure credentials in DuckDB
        await this.conn.run(`
          SET azure_storage_connection_string='DefaultEndpointsProtocol=https;AccountName=${storageAccount};SharedAccessSignature=${process.env.AZURE_STORAGE_SAS_TOKEN}';
        `);
        
        console.log('INFO: Azure credentials configured for direct ABFSS path');
        
        // Keep the original path
        deltaPath = tablePath;
      } catch (error) {
        console.error(`Error setting up Azure credentials for ABFSS path ${tablePath}:`, error);
        throw new Error(`Failed to set up Azure credentials for ABFSS path: ${error}`);
      }
    }

    // If no custom SQL provided, build a simple SELECT query
    if (!sql) {
      const limit = options.limit ? parseInt(options.limit, 10) : 10;
      sql = `SELECT * FROM delta_scan('${deltaPath}') LIMIT ${limit}`;
    } else if (sql.includes('$TABLE')) {
      // Replace $TABLE placeholder with delta_scan function
      console.log(`INFO: Replacing $TABLE placeholder with delta_scan('${deltaPath}')`);
      sql = sql.replace(/\$TABLE/g, `delta_scan('${deltaPath}')`);
    }
    
    try {
      console.log(`INFO: Executing query: ${sql}`);
      const reader = await this.conn.runAndReadAll(sql);
      const rows = reader.getRowObjects();
      console.log(`INFO: Query returned ${rows.length} rows`);
      // Use serializeBigInt to handle BigInt values before returning
      return serializeBigInt(rows);
    } catch (error) {
      console.error(`Error querying Delta table ${tablePath}:`, error);
      throw new Error(`Failed to query Delta table: ${error}`);
    }
  }

  /**
   * Get table statistics (row count)
   * @param {string} tablePath - Path to the Delta table
   * @returns {Promise<object>} - Table statistics
   */
  async getTableStats(tablePath) {
    await this.initialize();
    
    if (!this.conn) {
      throw new Error('DuckDB connection not initialized');
    }
    
    // Check if this is a Unity Catalog table (format: catalog.schema.table)
    const isUnityCatalogTable = tablePath.split('.').length === 3 && 
                               !tablePath.startsWith('abfss://') && 
                               !tablePath.startsWith('s3://') &&
                               !tablePath.startsWith('/');
    
    // Check if this is an ABFSS path that might need SAS token
    const isAbfssPath = tablePath.startsWith('abfss://') || tablePath.includes('dfs.core.windows.net');
    
    // If Unity Catalog table, get the ABFSS path
    let deltaPath = tablePath;
    
    // Handle Unity Catalog tables
    if (isUnityCatalogTable && this.databricksHost && this.databricksToken && fetch) {
      try {
        // Get table info
        const tableInfo = await this.getTableInfo(tablePath);
        
        // Generate temporary credentials
        const credentials = await this.generateTempCredentials(tableInfo.table_id);
        
        // Configure Azure credentials
        await this.configureAzureCredentials(credentials);
        
        // Use the ABFSS path
        deltaPath = credentials.delta_path;
      } catch (error) {
        console.error(`Error setting up Unity Catalog table ${tablePath}:`, error);
        throw new Error(`Failed to set up Unity Catalog table: ${error}`);
      }
    }
    // Handle direct ABFSS paths with SAS token from environment
    else if (isAbfssPath && process.env.AZURE_STORAGE_SAS_TOKEN) {
      try {
        // Extract storage account from ABFSS path
        const abfssMatch = tablePath.match(/abfss:\/\/([^@]+)@([^.]+)\.dfs\.core\.windows\.net/);
        let storageAccount;
        
        if (abfssMatch && abfssMatch.length >= 3) {
          storageAccount = abfssMatch[2];
        } else if (process.env.AZURE_STORAGE_ACCOUNT_NAME) {
          storageAccount = process.env.AZURE_STORAGE_ACCOUNT_NAME;
        } else {
          throw new Error('Could not determine Azure storage account from ABFSS path or environment');
        }
        
        console.log(`INFO: Setting up Azure credentials for direct ABFSS path with storage account: ${storageAccount}`);
        
        // Set up Azure credentials in DuckDB
        await this.conn.run(`
          SET azure_storage_connection_string='DefaultEndpointsProtocol=https;AccountName=${storageAccount};SharedAccessSignature=${process.env.AZURE_STORAGE_SAS_TOKEN}';
        `);
        
        console.log('INFO: Azure credentials configured for direct ABFSS path');
        
        // Keep the original path
        deltaPath = tablePath;
      } catch (error) {
        console.error(`Error setting up Azure credentials for ABFSS path ${tablePath}:`, error);
        throw new Error(`Failed to set up Azure credentials for ABFSS path: ${error}`);
      }
    }
    
    const countSql = `SELECT COUNT(*) as count FROM delta_scan('${deltaPath}')`;
    
    try {
      console.log(`INFO: Getting table statistics for: ${tablePath}`);
      const reader = await this.conn.runAndReadAll(countSql);
      const rows = reader.getRowObjects();
      
      // Handle BigInt or number result from DuckDB
      const countValue = rows[0]?.count;
      let count = 0;
      
      if (typeof countValue === 'bigint') {
        count = Number(countValue);
      } else if (typeof countValue === 'number') {
        count = countValue;
      } else if (countValue !== undefined && countValue !== null) {
        count = Number(countValue) || 0;
      }
      
      return { count };
    } catch (error) {
      console.error(`Error getting table stats for ${tablePath}:`, error);
      throw new Error(`Failed to get table stats: ${error}`);
    }
  }

  /**
   * Get table schema information
   * @param {string} tablePath - Path to the Delta table
   * @returns {Promise<object>} - Table schema information
   */
  async getTableSchema(tablePath) {
    await this.initialize();
    
    if (!this.conn) {
      throw new Error('DuckDB connection not initialized');
    }
    
    // Check if this is a Unity Catalog table (format: catalog.schema.table)
    const isUnityCatalogTable = tablePath.split('.').length === 3 && 
                               !tablePath.startsWith('abfss://') && 
                               !tablePath.startsWith('s3://') &&
                               !tablePath.startsWith('/');
    
    // If Unity Catalog table, get the ABFSS path
    let deltaPath = tablePath;
    
    // Handle Unity Catalog tables
    if (isUnityCatalogTable && this.databricksHost && this.databricksToken && fetch) {
      try {
        // Get table info
        const tableInfo = await this.getTableInfo(tablePath);
        
        // Generate temporary credentials
        const credentials = await this.generateTempCredentials(tableInfo.table_id);
        
        // Configure Azure credentials
        await this.configureAzureCredentials(credentials);
        
        // Use the ABFSS path
        deltaPath = credentials.delta_path;
      } catch (error) {
        console.error(`Error setting up Unity Catalog table ${tablePath}:`, error);
        throw new Error(`Failed to set up Unity Catalog table: ${error}`);
      }
    }
    
    try {
      console.log(`INFO: Getting table schema for: ${tablePath}`);
      
      // First, create a view to make it easier to query schema
      await this.conn.run(`CREATE OR REPLACE VIEW temp_delta_view AS SELECT * FROM delta_scan('${deltaPath}') LIMIT 0`);
      
      // Query the schema information
      const reader = await this.conn.runAndReadAll(`
        DESCRIBE temp_delta_view
      `);
      
      const schema = reader.getRowObjects();
      return { schema };
    } catch (error) {
      console.error(`Error getting table schema for ${tablePath}:`, error);
      throw new Error(`Failed to get table schema: ${error}`);
    }
  }

  /**
   * Close the database connection
   */
  async close() {
    if (this.conn) {
      this.conn.disconnectSync();
      this.conn = null;
    }
    
    this.initialized = false;
    console.log('INFO: DuckDB connection closed');
  }
}

/**
 * Helper function to serialize BigInt values in JSON
 */
function serializeBigInt(data) {
  return JSON.parse(JSON.stringify(data, (key, value) => 
    typeof value === 'bigint' ? value.toString() : value
  ));
}

/**
 * CLI handler function
 */
async function handleCli() {
  const args = process.argv.slice(2);
  const options = {};
  
  // Parse command line arguments
  args.forEach(arg => {
    if (arg.startsWith('--')) {
      const equalIndex = arg.indexOf('=');
      if (equalIndex !== -1) {
        const key = arg.substring(2, equalIndex);
        const value = arg.substring(equalIndex + 1);
        options[key] = value;
      } else {
        const key = arg.substring(2);
        options[key] = true;
      }
    }
  });
  
  // Override environment variables with CLI arguments if provided
  if (options.databricks_host) {
    process.env.DATABRICKS_WORKSPACE_URL = options.databricks_host;
  }
  if (options.databricks_token) {
    process.env.DATABRICKS_TOKEN = options.databricks_token;
  }
  if (options.azure_storage_account) {
    process.env.AZURE_STORAGE_ACCOUNT_NAME = options.azure_storage_account;
  }
  
  if (!options.table) {
    console.error('Error: No table path specified');
    console.log('Usage: delta-scanner --table=<table_path> [options]');
    console.log('Options:');
    console.log('  --table=<path>              Unity Catalog table name or ABFSS path');
    console.log('  --query=<sql>               Custom SQL query (use $TABLE as placeholder)');
    console.log('  --limit=<number>            Limit number of rows returned (default: 10)');
    console.log('  --databricks_host=<url>     Databricks workspace URL');
    console.log('  --databricks_token=<token>  Databricks access token');
    console.log('  --azure_storage_account=<n> Azure storage account name');
    console.log('  --format=table              Display results as table (default: JSON)');
    console.log('  --schema                    Show table schema information');
    process.exit(1);
  }
  
  const scanner = new DeltaScanner();
  
  try {
    // Get table stats first
    const stats = await scanner.getTableStats(options.table);
    console.log(`Table row count: ${stats.count}`);
    
    // Get schema information if requested
    if (options.schema) {
      const schemaInfo = await scanner.getTableSchema(options.table);
      console.log('\nTable Schema:');
      console.table(schemaInfo.schema);
    }
    
    // Execute query
    let customQuery = options.query || null;
    
    // IMPORTANT: Don't try to replace $TABLE in the CLI handler
    // Let the query method handle it since it already has the logic for both
    // Unity Catalog tables and direct paths
    if (customQuery && customQuery.includes('$TABLE')) {
      console.log(`INFO: Custom query with $TABLE placeholder detected - will be handled by query method`);
    }
    
    const rows = await scanner.query(
      options.table, 
      customQuery, 
      { limit: options.limit || 10 }
    );
    
    // Print results with BigInt handling
    console.log('\nQuery Results:');
    if (rows.length > 0) {
      // Print column names
      console.log('Columns:', Object.keys(rows[0]));
      
      // Print the data with BigInt handling
      const serializedRows = serializeBigInt(rows);
      
      if (options.format === 'table') {
        console.table(serializedRows);
      } else {
        console.log(JSON.stringify(serializedRows, null, 2));
      }
    } else {
      console.log('No results returned');
    }
  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    await scanner.close();
    console.log('INFO: Connections closed');
  }
}

// Run CLI handler if script is executed directly
if (require.main === module) {
  handleCli().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

// Export the DeltaScanner class for use as a module
module.exports = {
  DeltaScanner,
  serializeBigInt
};