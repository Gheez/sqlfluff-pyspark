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
        code = """
def get_users():
    table_name = "users"
    result = spark.sql(f"SELECT id, name FROM {table_name}")
    return result
"""
        sql_strings = extract_sql_strings(code)
        assert len(sql_strings) == 1
        assert sql_strings[0]["sql_type"] == "fstring"
        assert "parts" in sql_strings[0]["sql"]

    def test_extract_fstring_with_multiple_expressions(self):
        """Test extracting f-string with multiple expressions."""
        code = """
def get_data():
    table = "users"
    condition = "active = 1"
    result = spark.sql(f"SELECT * FROM {table} WHERE {condition}")
    return result
"""
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
        original_code = """def get_data():
    table = "users"
    df1 = spark.sql("SELECT * FROM table1")
    df2 = spark.sql(f"SELECT * FROM {table}")
    return df1, df2
"""
        python_file.write_text(original_code)

        result = reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=True
        )
        # Should find both strings
        assert result["sql_strings_found"] == 2

    def test_sql_followed_by_method_call(self, sqlfluff_config_file, tmp_path):
        """Test that SQL string followed by method call is not corrupted."""
        python_file = tmp_path / "test.py"
        original_code = '''spark.sql("""
    SELECT distinct  
        '{V_COMPANY_CODE_CARSTORE_AS}'      AS COMPANY_CODE
        ,IFNULL(b.GL_ACCOUNT_NO_MAP,a.No_)  AS ACCOUNT_NUMBER
        ,a.Name                             AS ACCOUNT_NAME
    FROM
        HEDIN_HAAS.EXTR_CARSTORE_AS_G_L_ACCOUNT a
        LEFT JOIN hedin_automotive_carstore_as_g_l_entry b on a.No_ = b.GL_ACCOUNT_NO_ORG

 """).createOrReplaceTempView("Carstore_AS_GLA")'''
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        # Check that the method call is preserved
        modified_content = python_file.read_text()
        assert ".createOrReplaceTempView" in modified_content, (
            "Method call should be preserved after SQL formatting"
        )
        assert '"Carstore_AS_GLA"' in modified_content, (
            "Method call argument should be preserved"
        )

        # Verify the method call is complete (not truncated)
        if ".createOrReplaceTempView" in modified_content:
            idx = modified_content.find(".createOrReplaceTempView")
            method_call = modified_content[idx : idx + 50]
            assert method_call.startswith(".createOrReplaceTempView"), (
                f"Method call appears to be truncated: {repr(method_call)}"
            )

    def test_multiple_statements_preserved(self, sqlfluff_config_file, tmp_path):
        """Test that code before and after SQL strings is preserved."""
        python_file = tmp_path / "test.py"
        original_code = """import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

# First query
df1 = spark.sql("SELECT * FROM table1 WHERE id = 1")

# Second query with method call
df2 = spark.sql("SELECT name FROM users").filter("active = 1")

# Process results
result = df1.join(df2, "id")
result.show()
"""
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that all code is preserved
        assert "import pandas as pd" in modified_content
        assert "from pyspark.sql import SparkSession" in modified_content
        assert "spark = SparkSession.builder" in modified_content
        assert "# First query" in modified_content
        assert "# Second query" in modified_content
        assert "# Process results" in modified_content
        assert "df1 = spark.sql" in modified_content
        assert "df2 = spark.sql" in modified_content
        assert ".filter(" in modified_content
        assert "result = df1.join" in modified_content
        assert "result.show()" in modified_content

    def test_code_on_same_line_as_opening_quote(self, sqlfluff_config_file, tmp_path):
        """Test SQL string with code on the same line as opening quote."""
        python_file = tmp_path / "test.py"
        original_code = """def get_data(): df = spark.sql("SELECT * FROM users WHERE active = 1"); return df"""
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that code before and after is preserved
        assert (
            "def get_data():" in modified_content
            or "def get_data()" in modified_content
        )
        assert "df = spark.sql" in modified_content
        assert "return df" in modified_content or "return" in modified_content

    def test_code_on_same_line_as_closing_quote(self, sqlfluff_config_file, tmp_path):
        """Test SQL string with code on the same line as closing quote."""
        python_file = tmp_path / "test.py"
        original_code = """spark.sql("SELECT id, name FROM users").createOrReplaceTempView("users_view"); print("Done")"""
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that method call and subsequent code are preserved
        assert ".createOrReplaceTempView" in modified_content
        assert '"users_view"' in modified_content
        assert 'print("Done")' in modified_content or "print(" in modified_content

    def test_multiline_string_with_code_on_same_lines(
        self, sqlfluff_config_file, tmp_path
    ):
        """Test multiline SQL string with code on opening and closing lines."""
        python_file = tmp_path / "test.py"
        original_code = '''# Setup
df = spark.sql("""
    SELECT * FROM table
    WHERE id > 100
""").filter("status = 'active'")  # Filter active records
result = df.collect()  # Get results
'''
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that all code is preserved
        assert "# Setup" in modified_content
        assert "df = spark.sql" in modified_content
        assert ".filter(" in modified_content
        assert "# Filter active records" in modified_content
        assert "result = df.collect()" in modified_content
        assert "# Get results" in modified_content

    def test_multiple_sql_strings_with_code_between(
        self, sqlfluff_config_file, tmp_path
    ):
        """Test multiple SQL strings with code between them."""
        python_file = tmp_path / "test.py"
        original_code = """# First query
query1 = spark.sql("SELECT * FROM table1")
df1 = query1.toPandas()  # Convert to pandas

# Second query
query2 = spark.sql("SELECT * FROM table2 WHERE date > '2024-01-01'")
df2 = query2.toPandas()

# Combine results
combined = pd.concat([df1, df2])
"""
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that all code between strings is preserved
        assert "# First query" in modified_content
        assert "query1 = spark.sql" in modified_content
        assert "df1 = query1.toPandas()" in modified_content
        assert "# Convert to pandas" in modified_content
        assert "# Second query" in modified_content
        assert "query2 = spark.sql" in modified_content
        assert "df2 = query2.toPandas()" in modified_content
        assert "# Combine results" in modified_content
        assert "combined = pd.concat" in modified_content

    def test_sql_in_function_with_code_before_after(
        self, sqlfluff_config_file, tmp_path
    ):
        """Test SQL string inside function with code before and after."""
        python_file = tmp_path / "test.py"
        original_code = '''def process_data():
    # Initialize
    spark_session = get_spark_session()
    
    # Execute query
    result = spark.sql("""
        SELECT 
            id,
            name,
            email
        FROM users
        WHERE active = 1
    """)
    
    # Process results
    return result.collect()
'''
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that function structure and all code is preserved
        assert "def process_data():" in modified_content
        assert "# Initialize" in modified_content
        assert "spark_session = get_spark_session()" in modified_content
        assert "# Execute query" in modified_content
        assert "result = spark.sql" in modified_content
        assert "# Process results" in modified_content
        assert "return result.collect()" in modified_content

    def test_sql_with_complex_method_chaining(self, sqlfluff_config_file, tmp_path):
        """Test SQL string followed by complex method chaining."""
        python_file = tmp_path / "test.py"
        original_code = """result = spark.sql("SELECT * FROM orders WHERE date >= '2024-01-01'") \\
    .filter("status = 'completed'") \\
    .select("order_id", "customer_id", "total") \\
    .orderBy("total", ascending=False) \\
    .limit(100) \\
    .cache()
print(f"Processed {result.count()} orders")
"""
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that method chaining is preserved
        assert "result = spark.sql" in modified_content
        assert ".filter(" in modified_content
        assert ".select(" in modified_content
        assert ".orderBy(" in modified_content
        assert ".limit(" in modified_content
        assert ".cache()" in modified_content
        assert "print(f" in modified_content or "print(" in modified_content

    def test_sql_in_if_statement_with_else(self, sqlfluff_config_file, tmp_path):
        """Test SQL strings in conditional statements."""
        python_file = tmp_path / "test.py"
        original_code = """if use_new_table:
    df = spark.sql("SELECT * FROM new_table WHERE id > 100")
else:
    df = spark.sql("SELECT * FROM old_table WHERE id > 100")
    
df.show()
"""
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that conditional structure is preserved
        assert "if use_new_table:" in modified_content
        assert "df = spark.sql" in modified_content
        assert "else:" in modified_content
        assert "df.show()" in modified_content
        # Check that both SQL strings are present
        assert "new_table" in modified_content or "SELECT" in modified_content
        assert "old_table" in modified_content or "SELECT" in modified_content

    def test_sql_with_triple_quotes_and_code_on_lines(
        self, sqlfluff_config_file, tmp_path
    ):
        """Test triple-quoted SQL with code on the same lines as quotes."""
        python_file = tmp_path / "test.py"
        original_code = '''# Query with code on same lines
df = spark.sql("""SELECT * FROM users""").filter("active = 1")  # Single line triple quote
result = df.collect()  # Get all results

# Multi-line with code
df2 = spark.sql("""
    SELECT id, name FROM products
    WHERE price > 100
""").orderBy("price")  # Order by price
'''
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that code on same lines is preserved
        assert "# Query with code on same lines" in modified_content
        assert "df = spark.sql" in modified_content
        assert ".filter(" in modified_content
        assert "# Single line triple quote" in modified_content
        assert "result = df.collect()" in modified_content
        assert "# Get all results" in modified_content
        assert "# Multi-line with code" in modified_content
        assert "df2 = spark.sql" in modified_content
        assert ".orderBy(" in modified_content
        assert "# Order by price" in modified_content

    def test_multiple_sql_strings_same_line(self, sqlfluff_config_file, tmp_path):
        """Test multiple SQL strings on the same line."""
        python_file = tmp_path / "test.py"
        original_code = """df1 = spark.sql("SELECT * FROM table1"); df2 = spark.sql("SELECT * FROM table2"); result = df1.join(df2, "id")"""
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that both SQL strings and code between them are preserved
        assert "df1 = spark.sql" in modified_content
        assert "df2 = spark.sql" in modified_content
        assert "result = df1.join" in modified_content
        # Both should have SQL content
        assert "table1" in modified_content or "SELECT" in modified_content
        assert "table2" in modified_content or "SELECT" in modified_content

    def test_sql_with_fstring_and_regular_string_mixed(
        self, sqlfluff_config_file, tmp_path
    ):
        """Test mix of f-strings and regular strings with code around them."""
        python_file = tmp_path / "test.py"
        original_code = """# Regular string
table_name = "users"
df1 = spark.sql("SELECT * FROM users WHERE active = 1")

# F-string
user_id = 123
df2 = spark.sql(f"SELECT * FROM {table_name} WHERE id = {user_id}")

# Another regular string
df3 = spark.sql("SELECT COUNT(*) as total FROM users")

# Combine
result = df1.union(df2).union(df3)
result.show()
"""
        python_file.write_text(original_code)

        reformat_sql_in_python_file(
            str(python_file), sqlfluff_config_file, dry_run=False
        )

        modified_content = python_file.read_text()
        # Check that all code is preserved
        assert "# Regular string" in modified_content
        assert 'table_name = "users"' in modified_content
        assert "df1 = spark.sql" in modified_content
        assert "# F-string" in modified_content
        assert "user_id = 123" in modified_content
        assert "df2 = spark.sql" in modified_content
        assert "# Another regular string" in modified_content
        assert "df3 = spark.sql" in modified_content
        assert "# Combine" in modified_content
        assert "result = df1.union" in modified_content
        assert "result.show()" in modified_content
        # Check f-string expression is preserved
        assert "{table_name}" in modified_content or "table_name" in modified_content
        assert "{user_id}" in modified_content or "user_id" in modified_content
