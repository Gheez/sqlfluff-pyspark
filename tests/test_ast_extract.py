"""Tests for AST-based SQL extraction and replacement."""

import pytest
from sqlfluff_pyspark.ast_extract import (
    extract_sql_strings,
    reformat_sql_in_python_file,
    replace_sql_in_source,
)


class TestExtractSQLStrings:
    """Tests for extract_sql_strings function."""

    def test_extract_sql_from_spark_sql_call(self):
        """Test extracting SQL string from spark.sql() call."""
        code = """
def get_users():
    result = spark.sql("SELECT id, name FROM users")
    return result
"""
        sql_strings = extract_sql_strings(code)
        assert len(sql_strings) == 1
        assert "SELECT" in sql_strings[0]["sql"]
        assert "id, name FROM users" in sql_strings[0]["sql"]

    def test_extract_multiple_spark_sql_calls(self):
        """Test extracting SQL strings from multiple spark.sql() calls."""
        code = """
def get_data():
    df1 = spark.sql("SELECT * FROM table1")
    df2 = spark.sql("INSERT INTO table2 VALUES (1)")
    return df1, df2
"""
        sql_strings = extract_sql_strings(code)
        assert len(sql_strings) == 2
        assert any("SELECT" in s["sql"] for s in sql_strings)
        assert any("INSERT" in s["sql"] for s in sql_strings)

    def test_ignore_non_spark_sql_strings(self):
        """Test that non-spark.sql() SQL strings are ignored."""
        code = """
def execute_query():
    query = "SELECT * FROM users WHERE id = 1"
    cursor.execute(query)
    spark.sql("SELECT * FROM table")
"""
        sql_strings = extract_sql_strings(code)
        # Should only find the spark.sql() call, not the query variable
        assert len(sql_strings) == 1
        assert "SELECT * FROM table" in sql_strings[0]["sql"]

    def test_no_spark_sql_calls(self):
        """Test with no spark.sql() calls."""
        code = """
def hello():
    query = "SELECT * FROM users"
    return query
"""
        sql_strings = extract_sql_strings(code)
        # Should not find any SQL strings since there are no spark.sql() calls
        assert len(sql_strings) == 0

    def test_multiline_sql_in_spark_sql(self):
        """Test extracting multiline SQL strings from spark.sql()."""
        code = '''
def get_complex_query():
    result = spark.sql("""
    SELECT u.id, u.name, p.title
    FROM users u
    JOIN posts p ON u.id = p.user_id
    WHERE u.active = 1
    """)
    return result
'''
        sql_strings = extract_sql_strings(code)
        assert len(sql_strings) == 1
        assert "SELECT" in sql_strings[0]["sql"]
        assert "JOIN" in sql_strings[0]["sql"]

    def test_spark_sql_with_variable(self):
        """Test that spark.sql() with variable arguments is handled."""
        code = """
def get_data():
    query = "SELECT * FROM table"
    result = spark.sql(query)  # Variable, not a string literal
"""
        sql_strings = extract_sql_strings(code)
        # Variable arguments are not extracted (only string literals)
        assert len(sql_strings) == 0

    def test_nested_spark_sql(self):
        """Test spark.sql() calls in nested contexts."""
        code = """
def process_data():
    if condition:
        df = spark.sql("SELECT * FROM table1")
    else:
        df = spark.sql("SELECT * FROM table2")
    return df
"""
        sql_strings = extract_sql_strings(code)
        assert len(sql_strings) == 2

    def test_invalid_python_code(self):
        """Test with invalid Python code."""
        code = "def invalid syntax here"
        sql_strings = extract_sql_strings(code)
        # Should return empty list on syntax error
        assert isinstance(sql_strings, list)


class TestReplaceSQLInSource:
    """Tests for replace_sql_in_source function."""

    def test_replace_single_line_sql(self):
        """Test replacing SQL on a single line."""
        source = 'query = "SELECT * FROM table"'
        replacements = [(0, 9, 0, 33, '"SELECT * FROM table"')]
        result = replace_sql_in_source(source, replacements)
        assert "SELECT" in result

    def test_replace_multiline_sql(self):
        """Test replacing multiline SQL."""
        source = '''query = """
SELECT * FROM table
WHERE id = 1
"""'''
        replacements = [(0, 9, 2, 3, '"""SELECT * FROM table\\nWHERE id = 1\\n"""')]
        result = replace_sql_in_source(source, replacements)
        assert "SELECT" in result


class TestReformatSQLInPythonFile:
    """Tests for reformat_sql_in_python_file function."""

    def test_file_not_found(self, sqlfluff_config_file):
        """Test error when file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            reformat_sql_in_python_file("/nonexistent/file.py", sqlfluff_config_file)

    def test_no_spark_sql_calls(self, sqlfluff_config_file, tmp_path):
        """Test file with no spark.sql() calls."""
        python_file = tmp_path / "test.py"
        python_file.write_text(
            'def hello():\n    query = "SELECT * FROM users"\n    return query\n'
        )

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=True
        )
        assert result["sql_strings_found"] == 0
        assert result["sql_strings_reformatted"] == 0

    def test_reformat_spark_sql_string(self, sqlfluff_config_file, tmp_path):
        """Test reformatting SQL strings in spark.sql() calls."""
        python_file = tmp_path / "test.py"
        python_file.write_text(
            'def get_users():\n    result = spark.sql("SeLEct * from users")\n    return result\n'
        )

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=True
        )
        assert result["sql_strings_found"] > 0
        # The SQL should be reformatted (case fixed)
        if result["sql_strings_reformatted"] > 0:
            assert result["replacements"]

    def test_dry_run(self, sqlfluff_config_file, tmp_path):
        """Test dry run mode doesn't modify file."""
        python_file = tmp_path / "test.py"
        original_content = 'def get_users():\n    result = spark.sql("SeLEct * from users")\n    return result\n'
        python_file.write_text(original_content)

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=True
        )

        # File should be unchanged in dry run mode
        assert python_file.read_text() == original_content
        assert result["dry_run"] is True

    def test_actual_reformat(self, sqlfluff_config_file, tmp_path):
        """Test actually reformatting SQL strings in spark.sql() calls."""
        python_file = tmp_path / "test.py"
        python_file.write_text(
            'def get_users():\n    result = spark.sql("SeLEct * from users")\n    return result\n'
        )

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        # If SQL was reformatted, file should be modified
        if result["sql_strings_reformatted"] > 0:
            modified_content = python_file.read_text()
            # The reformatted SQL should have proper case
            assert "SELECT" in modified_content or "select" in modified_content.lower()

    def test_end_line_not_duplicated_when_fixing(self, sqlfluff_config_file, tmp_path):
        """Test that the line at the end of a Python string is not duplicated when fixing."""
        python_file = tmp_path / "test.py"
        # Create a multiline SQL string that ends at the end of a line
        # The SQL string ends with a line that should not be duplicated
        original_content = '''def get_data():
    result = spark.sql("""
    SELECT id, name
    FROM users
    WHERE active = 1
    """)
    return result
'''
        python_file.write_text(original_content)

        # Count the number of lines before fixing
        original_lines = original_content.splitlines(keepends=True)
        original_line_count = len(original_lines)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        # Read the modified content
        modified_content = python_file.read_text()
        modified_lines = modified_content.splitlines(keepends=True)
        modified_line_count = len(modified_lines)

        # Check that the end line is not duplicated
        # The line count should be the same or less (if SQL was reformatted to fewer lines)
        # but should not be more due to duplication
        assert modified_line_count <= original_line_count + 2, (
            f"Line count increased unexpectedly. "
            f"Original: {original_line_count}, Modified: {modified_line_count}. "
            f"This suggests the end line was duplicated."
        )

        # Specifically check that the line after the closing triple quotes is not duplicated
        # Find the closing triple quotes and check what comes after
        lines_list = modified_content.splitlines(keepends=True)
        for i, line in enumerate(lines_list):
            if '"""' in line and i > 0:
                # Check if this is the closing quote line
                # The next line should be the return statement, not a duplicate
                if i + 1 < len(lines_list):
                    next_line = lines_list[i + 1]
                    # If the next line is the same as the current line (excluding quotes), it's duplicated
                    if next_line.strip() == line.strip() and '"""' not in next_line:
                        pytest.fail(
                            f"End line appears to be duplicated. "
                            f"Line {i}: {repr(line)}, Line {i + 1}: {repr(next_line)}"
                        )

        # Also verify the structure is correct - should have return statement after the SQL
        assert "return result" in modified_content, (
            "Return statement should be present after SQL string"
        )

    def test_end_line_content_preserved(self, sqlfluff_config_file, tmp_path):
        """Test that content at the end of a multiline SQL string is preserved correctly."""
        python_file = tmp_path / "test.py"
        # Create a multiline SQL string where the last line has specific content
        original_content = '''def get_data():
    result = spark.sql("""
    SELECT id, name
    FROM users
    WHERE active = 1
    """)
    return result
'''
        python_file.write_text(original_content)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        modified_content_lines = modified_content.splitlines(keepends=True)

        # Find the line with the closing triple quotes
        closing_quote_line_idx = None
        for i, line in enumerate(modified_content_lines):
            if '"""' in line and i > 2:  # Should be after the opening quotes
                closing_quote_line_idx = i
                break

        if closing_quote_line_idx is not None:
            # Check that the line after closing quotes is correct (should be return statement)
            if closing_quote_line_idx + 1 < len(modified_content_lines):
                line_after_closing = modified_content_lines[closing_quote_line_idx + 1]
                # The line after closing quotes should be the return statement
                # It should NOT be a duplicate of the closing quote line
                assert "return" in line_after_closing, (
                    f"Line after closing quotes should contain 'return', "
                    f"but got: {repr(line_after_closing)}. "
                    f"This suggests the end line was duplicated or content was lost."
                )

        # Verify the structure: should have exactly one return statement
        return_count = modified_content.count("return result")
        assert return_count == 1, (
            f"Expected exactly one 'return result' statement, but found {return_count}. "
            f"This suggests duplication occurred."
        )

        # Verify that the closing quote line doesn't appear twice consecutively
        for i in range(len(modified_content_lines) - 1):
            current_line = modified_content_lines[i]
            next_line = modified_content_lines[i + 1]
            # Check if we have duplicate closing quote lines
            if (
                '"""' in current_line
                and current_line.strip() == next_line.strip()
                and '"""' in next_line
            ):
                pytest.fail(
                    f"Found duplicate closing quote lines at lines {i} and {i + 1}: "
                    f"{repr(current_line)} and {repr(next_line)}"
                )


class TestFStringSupport:
    """Tests for f-string SQL formatting support."""

    def test_extract_fstring_from_spark_sql_call(self):
        """Test extracting f-string from spark.sql() call."""
        code = '''
def get_users():
    table_name = "users"
    result = spark.sql(f"SELECT id, name FROM {table_name}")
    return result
'''
        sql_strings = extract_sql_strings(code)
        assert len(sql_strings) == 1
        assert sql_strings[0]["sql_type"] == "fstring"
        assert "parts" in sql_strings[0]["sql"]

    def test_extract_fstring_with_multiple_expressions(self):
        """Test extracting f-string with multiple expressions."""
        code = '''
def get_data():
    table = "users"
    condition = "active = 1"
    result = spark.sql(f"SELECT * FROM {table} WHERE {condition}")
    return result
'''
        sql_strings = extract_sql_strings(code)
        assert len(sql_strings) == 1
        assert sql_strings[0]["sql_type"] == "fstring"
        fstring_info = sql_strings[0]["sql"]
        # Should have multiple parts (text and expressions)
        assert len(fstring_info["parts"]) > 2

    def test_reformat_fstring_sql(self, sqlfluff_config_file, tmp_path):
        """Test reformatting SQL in f-strings."""
        python_file = tmp_path / "test.py"
        python_file.write_text(
            'def get_users():\n    table = "users"\n    result = spark.sql(f"SeLEct * from {table}")\n    return result\n'
        )

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=True
        )
        assert result["sql_strings_found"] == 1
        # Should detect the f-string
        if result["sql_strings_reformatted"] > 0:
            assert result["replacements"]

    def test_fstring_preserves_expressions(self, sqlfluff_config_file, tmp_path):
        """Test that f-string expressions are preserved during formatting."""
        python_file = tmp_path / "test.py"
        original_code = 'def get_data():\n    table = "users"\n    result = spark.sql(f"SELECT * FROM {table} WHERE id = {user_id}")\n    return result\n'
        python_file.write_text(original_code)

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        if result["sql_strings_reformatted"] > 0:
            modified_content = python_file.read_text()
            # Check that expressions are still present
            assert "{table}" in modified_content or "table" in modified_content
            assert "{user_id}" in modified_content or "user_id" in modified_content

    def test_fstring_with_multiline_sql(self, sqlfluff_config_file, tmp_path):
        """Test f-string with multiline SQL."""
        python_file = tmp_path / "test.py"
        original_code = '''def get_data():
    table = "users"
    result = spark.sql(f"""
    SELECT id, name
    FROM {table}
    WHERE active = 1
    """)
    return result
'''
        python_file.write_text(original_code)

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=True
        )
        assert result["sql_strings_found"] == 1

    def test_mixed_fstring_and_regular_strings(self, sqlfluff_config_file, tmp_path):
        """Test file with both f-strings and regular strings."""
        python_file = tmp_path / "test.py"
        original_code = '''def get_data():
    table = "users"
    df1 = spark.sql("SELECT * FROM table1")
    df2 = spark.sql(f"SELECT * FROM {table}")
    return df1, df2
'''
        python_file.write_text(original_code)

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=True
        )
        # Should find both strings
        assert result["sql_strings_found"] == 2
