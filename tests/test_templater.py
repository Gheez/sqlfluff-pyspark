"""
Tests for the PySpark templater.
"""

import pytest
from sqlfluff_pyspark_templater.templater import PySparkTemplater
from sqlfluff.core.config import FluffConfig


class TestPySparkTemplater:
    """Test cases for PySparkTemplater."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.templater = PySparkTemplater()
        self.config = FluffConfig(overrides={"dialect": "databricks"})
    
    def test_process_python_file_with_single_sql_block(self):
        """Test processing a Python file with a single SQL block."""
        python_code = '''from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

# Test SQL
df = spark.sql(f"select user_id,count(*) from users where id=123 group by user_id")
'''
        
        result, errors = self.templater.process(
            fname="test.py",
            in_str=python_code,
            config=self.config
        )
        
        # Should extract and format the SQL
        assert "select" in result.source_str.lower()
        assert "user_id" in result.source_str
        assert "count(*)" in result.source_str
        assert "from users" in result.source_str.lower()
        assert "where id = 123" in result.source_str.lower()
        assert "group by user_id" in result.source_str.lower()
        # Should have proper formatting with line breaks
        assert "\n    user_id," in result.source_str
        assert "\n    count(*)" in result.source_str
        assert "\nfrom users" in result.source_str
        assert "\nwhere id = 123" in result.source_str
        assert "\ngroup by user_id" in result.source_str
        assert len(errors) == 0
    
    def test_process_python_file_with_multiple_sql_blocks(self):
        """Test processing a Python file with multiple SQL blocks."""
        python_code = '''from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

# First SQL block
df1 = spark.sql(f"select user_id,count(*) from users where id=123 group by user_id")

# Second SQL block
df2 = spark.sql(f"""
    select name,email from users where active=true
""")
'''
        
        result, errors = self.templater.process(
            fname="test.py",
            in_str=python_code,
            config=self.config
        )
        
        # Should format both SQL blocks
        assert "select" in result.source_str.lower()
        assert "user_id" in result.source_str
        assert "name" in result.source_str
        assert "email" in result.source_str
        # Should have proper formatting with line breaks for both blocks
        assert "\n    user_id," in result.source_str
        assert "\n    count(*)" in result.source_str
        assert "\n    name," in result.source_str
        assert "\n    email" in result.source_str
        assert len(errors) == 0
    
    def test_process_python_file_with_variables(self):
        """Test processing a Python file with variables in SQL."""
        python_code = '''from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

# Variables
database_name = "my_database"
table_name = "users"
user_id = 12345

# SQL with variables
df = spark.sql(f"select * from {database_name}.{table_name} where id={user_id}")
'''
        
        result, errors = self.templater.process(
            fname="test.py",
            in_str=python_code,
            config=self.config
        )
        
        # Should substitute variables and format SQL
        assert "select" in result.source_str.lower()
        assert "from" in result.source_str.lower()
        assert "where" in result.source_str.lower()
        # Variables should be substituted
        assert "my_database" in result.source_str
        assert "users" in result.source_str
        assert "12345" in result.source_str
        # Should have proper formatting
        assert "\nwhere" in result.source_str
        assert len(errors) == 0
    
    def test_process_python_file_with_function_parameters(self):
        """Test processing a Python file with function parameters as variables."""
        python_code = '''from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

def query_data(sourcedatabase, sourcetablename):
    df = spark.sql(f"SELECT * FROM {sourcedatabase}.{sourcetablename}")
    return df
'''
        
        result, errors = self.templater.process(
            fname="test.py",
            in_str=python_code,
            config=self.config
        )
        
        # Should format SQL and use placeholders for function parameters
        assert "SELECT" in result.source_str
        assert "FROM" in result.source_str
        # Function parameters should be replaced with placeholders
        assert "__SOURCEDATABASE__" in result.source_str
        assert "__SOURCETABLENAME__" in result.source_str
        assert len(errors) == 0
    
    def test_process_python_file_with_no_sql(self):
        """Test processing a Python file with no SQL blocks."""
        python_code = '''from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

# No SQL here
print("Hello, World!")
'''
        
        result, errors = self.templater.process(
            fname="test.py",
            in_str=python_code,
            config=self.config
        )
        
        # Should return the original file unchanged
        assert result.source_str == python_code
        assert len(errors) == 0
    
    def test_process_sql_file_delegates_to_jinja(self):
        """Test that SQL files are delegated to Jinja templater."""
        sql_code = "select user_id,count(*) from users where id=123 group by user_id"
        
        result, errors = self.templater.process(
            fname="test.sql",
            in_str=sql_code,
            config=self.config
        )
        
        # Should format the SQL
        assert "select" in result.source_str.lower()
        assert "user_id" in result.source_str
        assert "count(*)" in result.source_str
        assert "from users" in result.source_str.lower()
        assert "where id = 123" in result.source_str.lower()
        assert "group by user_id" in result.source_str.lower()
        # Should have proper formatting with line breaks
        assert "\n    user_id," in result.source_str
        assert "\n    count(*)" in result.source_str
        assert "\nfrom users" in result.source_str
        assert "\nwhere id = 123" in result.source_str
        assert "\ngroup by user_id" in result.source_str
        assert len(errors) == 0
    
    def test_extract_sql_blocks_single_quotes(self):
        """Test extracting SQL from single-quoted f-strings."""
        python_code = 'df = spark.sql(f"select * from users")'
        
        sql_blocks = self.templater._extract_sql_blocks(python_code)
        
        assert len(sql_blocks) == 1
        sql_content, source_info = sql_blocks[0]
        assert sql_content == "select * from users"
        assert source_info['start_pos'] == 5  # Position of spark.sql(f"...")
        assert source_info['end_pos'] == len(python_code)
        assert 'original_f_string' in source_info
    
    def test_extract_sql_blocks_triple_quotes(self):
        """Test extracting SQL from triple-quoted f-strings."""
        python_code = '''df = spark.sql(f"""
    select * from users
    where active = true
""")'''
        
        sql_blocks = self.templater._extract_sql_blocks(python_code)
        
        assert len(sql_blocks) == 1
        sql_content, source_info = sql_blocks[0]
        assert "select * from users" in sql_content
        assert "where active = true" in sql_content
        assert source_info['start_pos'] == 5  # Position of spark.sql(f"""...""")
        assert source_info['end_pos'] == len(python_code)
        assert 'original_f_string' in source_info
    
    def test_extract_sql_blocks_multiple_blocks(self):
        """Test extracting multiple SQL blocks."""
        python_code = '''df1 = spark.sql(f"select * from users")
df2 = spark.sql(f"select * from orders")'''
        
        sql_blocks = self.templater._extract_sql_blocks(python_code)
        
        assert len(sql_blocks) == 2
        assert sql_blocks[0][0] == "select * from users"
        assert sql_blocks[1][0] == "select * from orders"
    
    def test_extract_variables_simple_assignments(self):
        """Test extracting variables from simple assignments."""
        python_code = '''database_name = "my_database"
table_name = "users"
user_id = 12345
is_active = True
count = None'''
        
        variables = self.templater._extract_variables(python_code)
        
        assert variables['database_name'] == "my_database"
        assert variables['table_name'] == "users"
        assert variables['user_id'] == 12345
        assert variables['is_active'] == True
        assert variables['count'] == None
    
    def test_extract_variables_function_parameters(self):
        """Test extracting variables from function parameters."""
        python_code = '''def query_data(sourcedatabase, sourcetablename, user_id):
    return spark.sql(f"SELECT * FROM {sourcedatabase}.{sourcetablename} WHERE id = {user_id}")'''
        
        variables = self.templater._extract_variables(python_code)
        
        assert variables['sourcedatabase'] == "__SOURCEDATABASE__"
        assert variables['sourcetablename'] == "__SOURCETABLENAME__"
        assert variables['user_id'] == "__USER_ID__"
    
    def test_substitute_variables_strings(self):
        """Test substituting string variables."""
        sql_content = "SELECT * FROM {database_name}.{table_name} WHERE name = '{user_name}'"
        variables = {
            'database_name': 'my_db',
            'table_name': 'users',
            'user_name': 'john'
        }
        
        result = self.templater._substitute_variables(sql_content, variables)
        
        assert result == "SELECT * FROM 'my_db'.'users' WHERE name = 'john'"
    
    def test_substitute_variables_numbers(self):
        """Test substituting numeric variables."""
        sql_content = "SELECT * FROM users WHERE id = {user_id} AND age > {min_age}"
        variables = {
            'user_id': 12345,
            'min_age': 18
        }
        
        result = self.templater._substitute_variables(sql_content, variables)
        
        assert result == "SELECT * FROM users WHERE id = 12345 AND age > 18"
    
    def test_substitute_variables_none(self):
        """Test substituting None variables."""
        sql_content = "SELECT * FROM users WHERE deleted_at IS {is_deleted}"
        variables = {
            'is_deleted': None
        }
        
        result = self.templater._substitute_variables(sql_content, variables)
        
        assert result == "SELECT * FROM users WHERE deleted_at IS NULL"
    
    def test_revert_variable_substitution(self):
        """Test reverting variable substitution."""
        sql_content = "SELECT * FROM 'my_db'.'users' WHERE id = 12345"
        variables = {
            'database_name': 'my_db',
            'table_name': 'users',
            'user_id': 12345
        }
        
        result = self.templater._revert_variable_substitution(sql_content, variables)
        
        assert result == "SELECT * FROM {database_name}.{table_name} WHERE id = {user_id}"
    
    def test_create_f_string_with_sql_single_quotes(self):
        """Test creating f-string with single quotes."""
        sql_content = "SELECT * FROM users"
        original_f_string = 'spark.sql(f"select * from users")'
        
        result = self.templater._create_f_string_with_sql(sql_content, original_f_string)
        
        assert result == 'spark.sql(f"SELECT * FROM users")'
    
    def test_create_f_string_with_sql_triple_quotes(self):
        """Test creating f-string with triple quotes."""
        sql_content = "SELECT\n    *\nFROM users"
        original_f_string = 'spark.sql(f"""select * from users""")'
        
        result = self.templater._create_f_string_with_sql(sql_content, original_f_string)
        
        assert result == 'spark.sql(f"""SELECT\n    *\nFROM users""")'
    
    def test_preserve_python_structure(self):
        """Test that Python file structure is preserved."""
        python_code = '''from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Test").getOrCreate()

# Configuration
database_name = "my_database"

# Main query
df = spark.sql(f"select user_id,count(*) from {database_name}.users where id=123 group by user_id")

# Process results
print("Query completed")
'''
        
        result, errors = self.templater.process(
            fname="test.py",
            in_str=python_code,
            config=self.config
        )
        
        # Should preserve Python structure
        assert "from pyspark.sql import SparkSession" in result.source_str
        assert "import os" in result.source_str
        assert "spark = SparkSession.builder" in result.source_str
        assert "# Configuration" in result.source_str
        assert "database_name = \"my_database\"" in result.source_str
        assert "# Main query" in result.source_str
        assert "# Process results" in result.source_str
        assert "print(\"Query completed\")" in result.source_str
        
        # SQL should be formatted
        assert "select" in result.source_str.lower()
        assert "user_id" in result.source_str
        assert "count(*)" in result.source_str
        assert "from" in result.source_str.lower()
        assert "where id = 123" in result.source_str.lower()
        assert "group by user_id" in result.source_str.lower()
        # Should have proper formatting with line breaks
        assert "\n    user_id," in result.source_str
        assert "\n    count(*)" in result.source_str
        assert "\nfrom" in result.source_str
        assert "\nwhere id = 123" in result.source_str
        assert "\ngroup by user_id" in result.source_str
        
        assert len(errors) == 0