# SQLFluff PySpark Templater - Usage Guide

## Overview

The SQLFluff PySpark Templater allows you to use SQLFluff to lint, fix, and format SQL code embedded in PySpark f-string blocks within Python files.

## Installation

The templater is already installed in your project. You can use it with SQLFluff commands.

## Usage

### Direct File Processing (Recommended)

With the project's `.sqlfluff` configuration, you can process Python files directly:

```bash
# Lint SQL in PySpark files
uv run sqlfluff lint --templater pyspark your_file.py

# Fix SQL issues
uv run sqlfluff fix --templater pyspark your_file.py

# Format SQL
uv run sqlfluff format --templater pyspark your_file.py
```

### Stdin Processing (Alternative)

You can also use stdin for processing:

```bash
cat your_file.py | uv run sqlfluff lint --templater pyspark --dialect sparksql --stdin-filename your_file.py -
```

## Supported Patterns

The templater recognizes the following PySpark SQL patterns:

### 1. Triple-quoted f-strings
```python
df = spark.sql(f"""
    SELECT 
        user_id,
        COUNT(*) as order_count
    FROM orders 
    WHERE order_date >= '2023-01-01'
    GROUP BY user_id
    ORDER BY order_count DESC
""")
```

### 2. Single-quoted f-strings
```python
df = spark.sql(f"SELECT * FROM users WHERE age > 25")
```

### 3. Both single and double quotes
```python
df = spark.sql(f'SELECT * FROM users')
df = spark.sql(f"SELECT * FROM users")
```

## Examples

### Example 1: Linting a Python file with SQL

```bash
# Create a test file
cat > test.py << 'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

# SQL with formatting issues
df = spark.sql(f"""
    select id,name,age
    from employees 
    where age>25
    order by name
""")
EOF

# Lint the SQL (direct file processing)
uv run sqlfluff lint --templater pyspark test.py
```

Output:
```
== [test.py] FAIL
L:   1 | P:   1 | CP01 | Keywords must be upper case. [capitalisation.keywords]
L:   1 | P:   1 | LT09 | Select targets should be on a new line unless there is only one select target. [layout.select_targets]
L:   2 | P:   1 | LT02 | Line should not be indented. [layout.indent]
L:   2 | P:   5 | CP01 | Keywords must be upper case. [capitalisation.keywords]
L:   3 | P:   1 | LT02 | Line should not be indented. [layout.indent]
L:   3 | P:   5 | CP01 | Keywords must be upper case. [capitalisation.keywords]
L:   4 | P:   1 | LT02 | Line should not be indented. [layout.indent]
L:   4 | P:   5 | CP01 | Keywords must be upper case. [capitalisation.keywords]
L:   4 | P:  11 | CP01 | Keywords must be upper case. [capitalisation.keywords]
L:   4 | P:  18 | LT12 | Files must end with a single trailing newline. [layout.end_of_file]
```

### Example 2: Fixing SQL issues

```bash
# Fix the SQL automatically
uv run sqlfluff fix --templater pyspark test.py
```

Output:
```
SELECT
    id,
    name,
    age
FROM employees 
WHERE age>25
ORDER BY name
```

## Variable Substitution

The templater now supports variable substitution in f-strings! It automatically:

1. **Extracts variables** from Python code (assignments and function parameters)
2. **Substitutes variables** in SQL before processing
3. **Handles different data types** (strings, numbers, booleans, None)

### Supported Variable Types

- **Simple assignments**: `database_name = "my_db"`
- **Function parameters**: `def query(db_name, table_name):`
- **String literals**: Automatically quoted in SQL
- **Numbers**: Used as-is in SQL
- **None values**: Converted to `NULL` in SQL

### Example with Variables

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

# Variables
database_name = "my_database"
table_name = "users"
start_date = "2023-01-01"

# SQL with variables - automatically substituted!
df = spark.sql(f"""
    SELECT user_id, COUNT(*) as order_count
    FROM {database_name}.orders 
    WHERE order_date >= '{start_date}'
    GROUP BY user_id
""")
```

## Current Limitations

1. **Block Separation**: Multiple SQL blocks are concatenated with semicolon separators for processing
2. **Complex Expressions**: Only handles simple variable assignments, not complex expressions

## Configuration

You can create a `.sqlfluff` configuration file to set default settings:

```ini
[sqlfluff]
templater = pyspark
dialect = sparksql

[templater:pyspark]
type = pyspark
```

## Integration with IDEs

### VS Code

You can integrate this with VS Code by:

1. Installing the SQLFluff extension
2. Configuring it to use the pyspark templater
3. Setting up a custom command to process Python files

### Command Line Integration

Create a shell script for easy usage:

```bash
#!/bin/bash
# pyspark-sqlfluff.sh

if [ $# -eq 0 ]; then
    echo "Usage: $0 <python_file> [sqlfluff_command]"
    echo "Commands: lint, fix, format (default: lint)"
    exit 1
fi

FILE="$1"
COMMAND="${2:-lint}"

cat "$FILE" | uv run sqlfluff "$COMMAND" --templater pyspark --dialect sparksql --stdin-filename "$FILE" -
```

Make it executable:
```bash
chmod +x pyspark-sqlfluff.sh
```

Usage:
```bash
./pyspark-sqlfluff.sh my_file.py lint
./pyspark-sqlfluff.sh my_file.py fix
./pyspark-sqlfluff.sh my_file.py format
```

## Troubleshooting

### No SQL blocks found
If you see "DEBUG: No SQL blocks found", make sure your Python file contains `spark.sql(f"...")` patterns.

### Parsing errors with template variables
If you see parsing errors with `{variable_name}` patterns, make sure the variables are defined in the Python code as simple assignments or function parameters.

### File not processed
Make sure your `.sqlfluff` configuration includes `.py` in the `sql_file_exts` setting.

## Future Enhancements

- Better error reporting with line number mapping
- Integration with more IDEs and editors
- Individual SQL block processing (instead of concatenation)
- Support for complex expressions in variable substitution
- Better handling of nested function calls and imports