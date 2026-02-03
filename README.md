# SQLFluff PySpark

A wrapper library for sqlfluff that analyzes SQL files in temp directories. This library provides a programmatic interface to sqlfluff's parsing and fixing capabilities.

## Installation

```bash
pip install -e .
```

Or install from source:
```bash
pip install .
```

## Usage

### As a Library

Import and use the wrapped sqlfluff commands:

```python
from sqlfluff_pyspark import analyze_temp_directory, parse_sql, fix_sql

# Analyze SQL files in a temp directory
results = analyze_temp_directory(
    config_path="/path/to/.sqlfluff",
    fix_sql=False  # Set to True to apply fixes
)

# Parse a SQL string
parsed = parse_sql(
    sql="SELECT * FROM table",
    config_path="/path/to/.sqlfluff"
)

# Fix a SQL string
fixed_sql = fix_sql(
    sql="SeLEct * from table",
    config_path="/path/to/.sqlfluff"
)

# Extract and reformat SQL strings in Python files
from sqlfluff_pyspark import reformat_sql_in_python_file, extract_sql_strings

# Reformat SQL strings in a Python file
result = reformat_sql_in_python_file(
    file_path="my_script.py",
    config_path="/path/to/.sqlfluff",
    dry_run=False
)

# Or extract SQL strings without reformatting
code = '''
def get_users():
    result = spark.sql("SELECT id, name FROM users")
    return result
'''
sql_strings = extract_sql_strings(code)
# Only extracts SQL from spark.sql() calls
```

### Command-Line Interface

After installation, use the CLI:

```bash
# Analyze SQL files (no fixing)
sqlfluff-pyspark /path/to/.sqlfluff

# Analyze and fix SQL files
sqlfluff-pyspark /path/to/.sqlfluff --fix
```

## API Reference

### `analyze_temp_directory(config_path: str, fix_sql: bool = False) -> List[Dict[str, Any]]`

Analyzes all SQL files found in a temporary directory created by `tempfile.mkdtemp()`.

**Parameters:**
- `config_path`: Required path to an existing .sqlfluff config file
- `fix_sql`: If True, apply sqlfluff fixes to the SQL files (default: False)

**Returns:**
List of dictionaries containing analysis results for each SQL file, including:
- `file`: Filename
- `path`: Full path to the file
- `parsed`: Parsed SQL structure
- `fixed`: Boolean indicating if the file was fixed
- `original_content`: Original SQL content
- `fixed_content`: Fixed SQL content (if fixing was enabled)

### `parse_sql(sql: str, config_path: str) -> Dict[str, Any]`

Parse a SQL string and return the parsed structure.

**Parameters:**
- `sql`: SQL string to parse
- `config_path`: Path to an existing .sqlfluff config file

**Returns:**
Dictionary containing the parsed SQL structure

### `fix_sql(sql: str, config_path: str) -> str`

Fix a SQL string according to sqlfluff rules.

**Parameters:**
- `sql`: SQL string to fix
- `config_path`: Path to an existing .sqlfluff config file

**Returns:**
Fixed SQL string

### `extract_sql_strings(source_code: str) -> List[Dict[str, Any]]`

Extract SQL strings from `spark.sql()` calls in Python source code using AST parsing.

**Note:** Only extracts SQL strings that are passed as arguments to `spark.sql()` calls. Other SQL strings in the code are ignored.

**Parameters:**
- `source_code`: Python source code as a string

**Returns:**
List of dictionaries containing SQL string information with keys:
- `sql`: The SQL string content
- `lineno`: Line number where the string starts
- `col_offset`: Column offset where the string starts
- `end_lineno`: Line number where the string ends
- `col_end_offset`: Column offset where the string ends
- `node`: The AST node representing the string

### `reformat_sql_in_python_file(file_path: str, config_path: str, dry_run: bool = False) -> Dict[str, Any]`

Extract SQL strings from `spark.sql()` calls in a Python file, reformat them using sqlfluff, and replace them in the file.

**Note:** Only processes SQL strings found in `spark.sql()` calls. Other SQL strings in the code are ignored.

This function:
1. Parses the Python file using AST
2. Extracts SQL strings from `spark.sql()` calls
3. Writes each SQL string to a temporary StringIO buffer (as a "partial file")
4. Reforms the SQL using sqlfluff
5. Replaces the original SQL strings with reformatted versions

**Parameters:**
- `file_path`: Path to Python file to process
- `config_path`: Path to sqlfluff config file
- `dry_run`: If True, don't write changes back to file (default: False)

**Returns:**
Dictionary with results:
- `file`: Path to the processed file
- `sql_strings_found`: Number of SQL strings found
- `sql_strings_reformatted`: Number of SQL strings that were reformatted
- `replacements`: List of replacement operations performed
- `dry_run`: Whether this was a dry run
- `modified_source`: Modified source code (only if dry_run=True)

**Example:**
```python
from sqlfluff_pyspark import reformat_sql_in_python_file

# Reformat SQL strings in a Python file
result = reformat_sql_in_python_file(
    file_path="my_script.py",
    config_path="/path/to/.sqlfluff",
    dry_run=False  # Set to True to preview changes
)

print(f"Found {result['sql_strings_found']} SQL strings")
print(f"Reformatted {result['sql_strings_reformatted']} SQL strings")
```

## Development

### Running Tests

Install development dependencies:
```bash
pip install -e ".[dev]"
```

Run tests:
```bash
pytest
```

Run tests with coverage:
```bash
pytest --cov=sqlfluff_pyspark --cov-report=html
```

## Requirements

- Python >= 3.12
- sqlfluff >= 3.0.0

## Configuration

This library requires an existing sqlfluff configuration file. The config file path must be provided to all functions. See [sqlfluff documentation](https://docs.sqlfluff.com/) for configuration options.
