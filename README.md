# SQLFluff PySpark Templater

A SQLFluff templater that extracts and analyzes SQL code from PySpark f-string blocks.

## Overview

This templater allows you to use SQLFluff to analyze SQL code that's embedded in PySpark f-strings, such as:

```python
df = spark.sql(f"""
    SELECT 
        id,
        name,
        age
    FROM employees 
    WHERE age > 25
    ORDER BY name
""")
```

## Installation

### From Source

```bash
# Clone or download this repository
cd sqlfluff-pyspark-templater

# Install in development mode
pip install -e .

# Or install with uv
uv add .
```

### Dependencies

- Python 3.9+
- SQLFluff 3.4.2+
- PySpark 4.0.1+

## Usage

### Command Line

#### Extract SQL blocks only:
```bash
sqlfluff-pyspark --extract-only examples/example_pyspark.py
```

#### Extract to files:
```bash
sqlfluff-pyspark --extract-only -o extracted.sql examples/example_pyspark.py
```

### With SQLFluff

Once installed, you can use the templater with SQLFluff:

```bash
# Lint SQL in PySpark files
sqlfluff lint --templater pyspark examples/example_pyspark.py

# Fix SQL in PySpark files
sqlfluff fix --templater pyspark examples/example_pyspark.py

# Format SQL in PySpark files
sqlfluff format --templater pyspark examples/example_pyspark.py
```

### Configuration

Add to your `.sqlfluff` configuration file:

```ini
[templater:pyspark]
type = pyspark
```

## Supported Patterns

The templater recognizes the following PySpark SQL patterns:

1. **Triple-quoted f-strings:**
   ```python
   df = spark.sql(f"""
       SELECT * FROM table
   """)
   ```

2. **Single-quoted f-strings:**
   ```python
   df = spark.sql(f"SELECT * FROM table")
   ```

3. **Both single and double quotes:**
   ```python
   df = spark.sql(f'SELECT * FROM table')
   df = spark.sql(f"SELECT * FROM table")
   ```

## Features

- ✅ Extracts SQL from PySpark f-string blocks
- ✅ Supports both single and triple-quoted strings
- ✅ Maps error positions back to original Python file
- ✅ Works with all SQLFluff rules and features
- ✅ Command-line tool for extraction
- ✅ Handles multiple SQL blocks in one file

## Limitations

- Currently processes only the first SQL block found in a file
- Does not handle variable interpolation in f-strings
- Does not support complex Python expressions in f-strings

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Project Structure

```
sqlfluff-pyspark-templater/
├── sqlfluff_pyspark_templater/
│   ├── __init__.py
│   ├── templater.py           # Main templater implementation
│   ├── cli.py                 # Command-line interface
│   └── sqlfluff_wrapper.py    # SQLFluff integration
├── tests/
│   ├── __init__.py
│   └── test_templater.py
├── pyproject.toml
├── setup.py
├── LICENSE
└── README.md
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.