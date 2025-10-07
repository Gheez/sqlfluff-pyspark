# Installation Guide

## Quick Start

1. **Install the package:**
   ```bash
   pip install -e .
   # or with uv
   uv add .
   ```

2. **Use with SQLFluff:**
   ```bash
   sqlfluff lint --templater pyspark your_pyspark_file.py
   sqlfluff format --templater pyspark your_pyspark_file.py
   sqlfluff fix --templater pyspark your_pyspark_file.py
   ```

3. **Use the CLI tool:**
   ```bash
   sqlfluff-pyspark --extract-only your_pyspark_file.py
   ```

## Requirements

- Python 3.9+
- SQLFluff 2.0+

## Configuration

Add to your `.sqlfluff` configuration file:

```ini
[templater:pyspark]
type = pyspark
```

## Supported Patterns

The templater recognizes these PySpark SQL patterns:

- `spark.sql(f"""...""")` - Triple-quoted f-strings
- `spark.sql(f'...')` - Single-quoted f-strings  
- `spark.sql(f"..."")` - Double-quoted f-strings
- `df = spark.sql(f"""...""")` - Assignment with f-strings

## Examples

### Linting
```bash
sqlfluff lint --templater pyspark --dialect sparksql examples/example_pyspark.py
```

### Formatting
```bash
sqlfluff format --templater pyspark examples/example_pyspark.py
```

### Extraction
```bash
sqlfluff-pyspark --extract-only -o extracted.sql examples/example_pyspark.py
```