# Delta Scanner

A standalone Delta Table Scanner using DuckDB for querying Delta tables through Unity Catalog from Node.js (designed with Azure Databricks in mind).

## Features

- Query Delta tables using DuckDB through Unity Catalog without a Databricks cluster
- Support for Unity Catalog tables with automatic credential handling
- ABFSS path support with SAS token authentication
- Use as a CLI tool or import as a module in your Node.js applications
- Custom SQL queries with table placeholder support

## Requirements

- Node.js 14 or higher
- Internet access to Databricks Unity Catalog API (for UC tables)
- No Azure credentials needed for UC tables (temporary SAS is requested automatically)
- For direct ABFSS access only: an Azure Storage account name and a valid SAS token

## Installation

### Global Installation

```bash
npm install -g delta-unity-duckdb
```

### Local Installation

```bash
npm install delta-unity-duckdb
```

## Configuration

Delta Scanner can be configured using environment variables or CLI arguments:

### Environment Variables

Create a `.env` file in your project root with the minimal variables for Unity Catalog tables (recommended):

```
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token-here
```

Direct ABFSS access (no Unity Catalog):

```
AZURE_STORAGE_ACCOUNT_NAME=your-storage-account-name
AZURE_STORAGE_SAS_TOKEN=your-sas-token
```

Notes for ABFSS:
- These are only required if you pass an `abfss://` table path directly and are NOT using Unity Catalog.
- Provide the SAS token string itself (usually starts with `sv=...`), without the leading `?`.

### CLI Arguments

CLI arguments will override environment variables:

```bash
delta-unity-duckdb --table="catalog.schema.table_name" --databricks_host="https://your-workspace.cloud.databricks.com" --databricks_token="your-token"
```

ABFSS example (requires AZURE_STORAGE_* envs):

```bash
export AZURE_STORAGE_ACCOUNT_NAME=your-account
export AZURE_STORAGE_SAS_TOKEN='sv=...&ss=...&srt=...&sp=rl&se=...&st=...&spr=https&sig=...'
delta-unity-duckdb --table="abfss://container@your-account.dfs.core.windows.net/path/to/delta-table" --limit=5 --format=table
```

## Quickstart

1. Create a `.env` file with the minimal required variables for Unity Catalog tables:

```
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-databricks-personal-access-token
```

Tip: export your `.env` into the shell (mac/Linux):

```bash
set -a; source .env; set +a
```

2. Run a query against a Unity Catalog table:

```bash
delta-unity-duckdb --table="$TABLE" --limit=5 --format=table
```

Example output (table format):

```text
Table row count: 12345

Query Results:
Columns: [ 'id', 'name', 'value' ]
┌─────────┬─────┬───────────┬───────┐
│ (index) │ id  │   name    │ value │
├─────────┼─────┼───────────┼───────┤
│    0    │  1  │ 'alpha'   │  3.14 │
│    1    │  2  │ 'beta'    │  2.71 │
│    2    │  3  │ 'gamma'   │  1.62 │
│    3    │  4  │ 'delta'   │  0.58 │
│    4    │  5  │ 'epsilon' │  4.20 │
└─────────┴─────┴───────────┴───────┘
```

3. Inspect schema (optional):

```bash
delta-unity-duckdb --table="catalog.schema.table_name" --schema
```

## Usage

### As a CLI Tool

After installing globally, you can use the `delta-unity-duckdb` command directly:

```bash
# Basic usage with .env file configuration (uses TABLE variable from .env)
delta-unity-duckdb --table="$TABLE" --limit=10

# Using explicit table name with CLI arguments for Databricks credentials
delta-unity-duckdb --table="catalog.schema.table_name" --databricks_host="https://your-workspace.cloud.databricks.com" --databricks_token="your-token" --limit=5

# Custom SQL query with explicit table name (IMPORTANT: use actual table name in --table parameter)
delta-unity-duckdb --table="catalog.schema.table_name" --query="SELECT column1, COUNT(*) as count FROM \$TABLE GROUP BY column1 ORDER BY count DESC LIMIT 5"

# Display results as a table instead of JSON
delta-unity-duckdb --table="catalog.schema.table_name" --format=table

# IMPORTANT: Table Parameter vs $TABLE Placeholder
# --table="actual.table.name"  <- Specifies which table to query
# \$TABLE in --query           <- Placeholder that gets replaced with delta_scan() function
```

### How it works

Delta Scanner:

- Loads DuckDB with the `httpfs`, `delta`, and `azure` extensions.
- Looks up your Unity Catalog table and requests temporary credentials from the Databricks Unity Catalog API.
- Creates a temporary DuckDB secret with the retrieved SAS token to securely access Azure Storage.
- Resolves the Delta table location and runs your query via DuckDB (e.g., `delta_scan(...)`).
- Closes connections when finished.



### As a Module

```javascript
const { DeltaScanner } = require('delta-unity-duckdb');

async function queryDeltaTable() {
  const scanner = new DeltaScanner();
  
  try {
    // Get table statistics
    const stats = await scanner.getTableStats('catalog.schema.table_name');
    console.log(`Table row count: ${stats.count}`);
    
    // Get schema information
    const schemaInfo = await scanner.getTableSchema('catalog.schema.table_name');
    console.log('Table Schema:', schemaInfo.schema);
    
    // Execute a simple query
    const rows = await scanner.query('catalog.schema.table_name', null, { limit: 5 });
    console.log('Query Results:', rows);
    
    // Execute a custom query
    const customResults = await scanner.query(
      'catalog.schema.table_name',
      'SELECT column1, COUNT(*) as count FROM $TABLE GROUP BY column1 ORDER BY count DESC LIMIT 5'
    );
    console.log('Custom Query Results:', customResults);
  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    await scanner.close();
  }
}

queryDeltaTable();
```



## API Reference

### DeltaScanner Class

#### Constructor

```javascript
const scanner = new DeltaScanner(options);
```

Options:
- `memory`: Boolean, whether to use in-memory database (default: true)
- `extensions`: Array of extensions to load (default: ['httpfs', 'delta', 'azure'])

#### Methods

- `async initialize()`: Initialize DuckDB connection and load required extensions
- `async query(tablePath, sql = null, options = {})`: Execute a SQL query on a Delta table
- `async getTableStats(tablePath)`: Get table statistics (row count)
- `async getTableSchema(tablePath)`: Get table schema information
- `async close()`: Close the database connection

## License

MIT