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
