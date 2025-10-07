"""
AST-based SQL string extraction and replacement functionality.
Uses AST to extract SQL strings from Python code, reformat them with sqlfluff,
and replace them in the original source.
"""

import ast
import io
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from sqlfluff_pyspark.core import fix_sql

logger = logging.getLogger(__name__)


class SQLStringExtractor(ast.NodeVisitor):
    """AST visitor to extract SQL strings from spark.sql() calls."""

    def __init__(self):
        self.sql_strings: List[Dict[str, any]] = []
        self.current_file: Optional[str] = None

    def visit_Call(self, node: ast.Call):
        """Visit function call nodes to find spark.sql() calls."""
        # Check if this is a spark.sql() call
        if self._is_spark_sql_call(node):
            # Extract string arguments from the call
            for arg in node.args:
                sql_text = self._extract_string_from_node(arg)
                if sql_text is not None:
                    self._record_sql_string(arg, sql_text)
        self.generic_visit(node)

    def _is_spark_sql_call(self, node: ast.Call) -> bool:
        """Check if a call node is a spark.sql() call."""
        # Check if it's an attribute access: spark.sql
        if isinstance(node.func, ast.Attribute):
            # Check if attribute name is "sql"
            if node.func.attr == "sql":
                # Check if the value is "spark" (could be a Name or another Attribute)
                if isinstance(node.func.value, ast.Name):
                    return node.func.value.id == "spark"
                elif isinstance(node.func.value, ast.Attribute):
                    # Handle cases like self.spark.sql() or df.spark.sql()
                    # We'll accept any chain ending with spark.sql
                    return self._is_spark_attribute(node.func.value)
        return False

    def _is_spark_attribute(self, node: ast.Attribute) -> bool:
        """Recursively check if an attribute chain ends with 'spark'."""
        if isinstance(node.value, ast.Name):
            return node.value.id == "spark"
        elif isinstance(node.value, ast.Attribute):
            return self._is_spark_attribute(node.value)
        return False

    def _extract_string_from_node(self, node: ast.AST) -> Optional[str]:
        """Extract string value from an AST node."""
        # Handle Python 3.8+ Constant nodes
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                return node.value

        # Handle Python <3.8 Str nodes
        if isinstance(node, ast.Str):
            return node.s

        # Handle JoinedStr (f-strings) - extract the format string parts
        if isinstance(node, ast.JoinedStr):
            # For f-strings, we might want to skip them or handle them specially
            # For now, we'll try to extract static parts
            parts = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                elif isinstance(value, ast.Str):
                    parts.append(value.s)
            if parts:
                return "".join(parts)

        return None

    def _record_sql_string(self, node: ast.AST, sql_text: str):
        """Record a SQL string found in the AST."""
        self.sql_strings.append(
            {
                "node": node,
                "sql": sql_text,
                "lineno": node.lineno,
                "col_offset": node.col_offset,
                "end_lineno": getattr(node, "end_lineno", node.lineno),
                "col_end_offset": getattr(
                    node, "col_end_offset", node.col_offset + len(sql_text)
                ),
            }
        )


def extract_sql_strings(source_code: str) -> List[Dict[str, any]]:
    """
    Extract SQL strings from spark.sql() calls in Python source code using AST.

    Only extracts SQL strings that are passed as arguments to spark.sql() calls.
    Other SQL strings in the code are ignored.

    Args:
        source_code: Python source code as a string

    Returns:
        List of dictionaries containing SQL string information
    """
    try:
        tree = ast.parse(source_code)
        extractor = SQLStringExtractor()
        extractor.visit(tree)
        return extractor.sql_strings
    except SyntaxError as e:
        logger.error(f"Failed to parse Python code: {e}")
        return []


def replace_sql_in_source(
    source_code: str,
    sql_replacements: List[Tuple[int, int, int, int, str]],
) -> str:
    """
    Replace SQL strings in source code with reformatted versions using line/column positions.

    Args:
        source_code: Original Python source code
        sql_replacements: List of (start_line, start_col, end_line, end_col, new_sql) tuples

    Returns:
        Modified source code with SQL strings replaced
    """
    # Sort replacements by line number (reverse order to maintain indices)
    sql_replacements.sort(key=lambda x: (x[0], x[1]), reverse=True)

    lines = source_code.splitlines(keepends=True)
    if not lines:
        return source_code

    # Ensure all lines end with newline for proper reconstruction
    for i, line in enumerate(lines):
        if not line.endswith(("\n", "\r\n", "\r")):
            lines[i] = line + "\n"

    for start_line, start_col, end_line, end_col, new_sql in sql_replacements:
        if start_line == end_line:
            # Single line replacement
            line = lines[start_line]
            # Handle case where line might not have newline
            line_content = line.rstrip("\n\r")
            newline = line[len(line_content) :]
            lines[start_line] = (
                line_content[:start_col] + new_sql + line_content[end_col:] + newline
            )
        else:
            # Multi-line replacement
            start_line_content = lines[start_line][:start_col]
            end_line_content = lines[end_line][end_col:]
            lines[start_line] = start_line_content + new_sql + end_line_content
            # Remove lines in between
            for i in range(start_line + 1, end_line):
                lines[i] = ""

    return "".join(lines)


def reformat_sql_in_python_file(
    file_path: str,
    config_path: str,
    dry_run: bool = False,
) -> Dict[str, any]:
    """
    Extract SQL strings from spark.sql() calls in a Python file, reformat them, and replace them.

    Only processes SQL strings found in spark.sql() calls. Uses AST to extract SQL strings,
    writes them to temporary StringIO objects for processing, then replaces them in the original source.

    Args:
        file_path: Path to Python file to process
        config_path: Path to sqlfluff config file
        dry_run: If True, don't write changes back to file

    Returns:
        Dictionary with results including replacements made
    """
    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        raise FileNotFoundError(f"Python file not found: {file_path}")

    # Read source code
    source_code = file_path_obj.read_text(encoding="utf-8")

    # Extract SQL strings using AST
    sql_strings = extract_sql_strings(source_code)

    if not sql_strings:
        logger.info(f"No SQL strings found in {file_path}")
        return {
            "file": file_path,
            "sql_strings_found": 0,
            "sql_strings_reformatted": 0,
            "replacements": [],
        }

    logger.info(f"Found {len(sql_strings)} SQL string(s) in {file_path}")

    # Reformat each SQL string
    replacements = []
    reformatted_count = 0

    for sql_info in sql_strings:
        original_sql = sql_info["sql"]

        try:
            # Use StringIO as a "partial file" to hold the SQL string
            # This allows us to work with it as a file-like object if needed
            sql_buffer = io.StringIO(original_sql)
            sql_content = sql_buffer.read()
            sql_buffer.close()

            # Fix the SQL using sqlfluff
            fixed_sql = fix_sql(sql_content, config_path)

            # Only replace if it changed
            if fixed_sql.strip() != original_sql.strip():
                # Get line/column positions from AST node
                # The col_offset points to the start of the string literal (including opening quote)
                start_line = sql_info["lineno"] - 1  # Convert to 0-based
                start_col = sql_info["col_offset"]

                # Find the actual string literal in source to get exact positions and quote style
                lines = source_code.splitlines(keepends=True)
                if start_line < len(lines):
                    line = lines[start_line]

                    # Find the quote style and full string literal
                    quote_chars = ['"""', "'''", '"', "'"]  # Check triple quotes first
                    quote_style = None
                    quote_start = None

                    for quote in quote_chars:
                        if start_col + len(quote) <= len(line):
                            if line[start_col : start_col + len(quote)] == quote:
                                quote_style = quote
                                quote_start = start_col
                                break

                    if quote_style:
                        # Find the end of the string literal (closing quote)
                        # Search forward from the opening quote
                        search_start = quote_start + len(quote_style)
                        quote_end = None

                        # Handle multi-line strings
                        if quote_style in ['"""', "'''"]:
                            # Search across multiple lines if needed
                            current_line = start_line
                            search_pos = (
                                search_start if current_line == start_line else 0
                            )

                            while current_line < len(lines):
                                line_text = lines[current_line]
                                # Look for closing quote
                                pos = line_text.find(quote_style, search_pos)
                                if pos != -1:
                                    quote_end = pos + len(quote_style)
                                    end_line = current_line
                                    break
                                current_line += 1
                                search_pos = 0
                        else:
                            # Single-line string - find closing quote on same line
                            pos = line.find(quote_style, search_start)
                            if pos != -1:
                                quote_end = pos + len(quote_style)
                                end_line = start_line

                        if quote_end is not None:
                            # Determine if we should use triple quotes for the fixed SQL
                            use_triple_quotes = "\n" in fixed_sql or len(fixed_sql) > 80

                            if use_triple_quotes and quote_style in ['"', "'"]:
                                # Switch to triple quotes
                                new_quote_style = quote_style * 3
                                fixed_sql_quoted = (
                                    f"{new_quote_style}{fixed_sql}{new_quote_style}"
                                )
                            else:
                                # Keep original quote style
                                fixed_sql_quoted = (
                                    f"{quote_style}{fixed_sql}{quote_style}"
                                )

                            # Replace the entire string literal (including quotes)
                            replacements.append(
                                (
                                    start_line,
                                    quote_start,
                                    end_line,
                                    quote_end,
                                    fixed_sql_quoted,
                                )
                            )
                            reformatted_count += 1
                            logger.info(
                                f"Reformatted SQL string at line {sql_info['lineno']}"
                            )
                        else:
                            logger.warning(
                                f"Could not find closing quote for SQL at line {sql_info['lineno']}"
                            )
                    else:
                        logger.warning(
                            f"Could not determine quote style for SQL at line {sql_info['lineno']}"
                        )
        except Exception as e:
            logger.warning(f"Failed to reformat SQL at line {sql_info['lineno']}: {e}")

    # Apply replacements
    modified_source = source_code
    if replacements:
        modified_source = replace_sql_in_source(source_code, replacements)
        if not dry_run:
            file_path_obj.write_text(modified_source, encoding="utf-8")
            logger.info(f"Wrote reformatted code to {file_path}")
        else:
            logger.info(f"Dry run: would write reformatted code to {file_path}")

    return {
        "file": file_path,
        "sql_strings_found": len(sql_strings),
        "sql_strings_reformatted": reformatted_count,
        "replacements": replacements,
        "dry_run": dry_run,
        "modified_source": modified_source if dry_run else None,
    }
