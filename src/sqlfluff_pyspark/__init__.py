"""
SQLFluff PySpark - A wrapper library for sqlfluff that analyzes SQL files in temp directories.
"""

from sqlfluff_pyspark.core import analyze_temp_directory, lint_sql, parse_sql, fix_sql
from sqlfluff_pyspark.ast_extract import (
    extract_sql_strings,
    reformat_sql_in_python_file,
    replace_sql_in_source,
)

__version__ = "0.1.0"
__all__ = [
    "analyze_temp_directory",
    "lint_sql",
    "parse_sql",
    "fix_sql",
    "extract_sql_strings",
    "reformat_sql_in_python_file",
    "replace_sql_in_source",
]
