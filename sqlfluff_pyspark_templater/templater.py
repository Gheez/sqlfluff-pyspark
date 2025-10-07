"""
PySpark Templater for SQLFluff

This templater extracts SQL code from PySpark f-string blocks like:
    df = spark.sql(f\"\"\"SELECT * FROM table\"\"\")
"""

import re
import ast
from typing import List, Optional, Tuple, Any, Dict, TYPE_CHECKING

from sqlfluff.core.templaters.base import RawTemplater, TemplatedFile, RawFileSlice, TemplatedFileSlice
from sqlfluff.core.templaters.jinja import JinjaTemplater
from sqlfluff.core.errors import SQLTemplaterError
from sqlfluff.core.helpers.slice import offset_slice

if TYPE_CHECKING:  # pragma: no cover
    from sqlfluff.cli.formatters import OutputStreamFormatter
    from sqlfluff.core import FluffConfig


class PySparkTemplater(JinjaTemplater):
    """
    A templater that extracts SQL from PySpark f-string blocks.
    
    This templater looks for patterns like:
    - spark.sql(f\"\"\"...\"\"\")
    - spark.sql(f'...')
    - df = spark.sql(f\"\"\"...\"\"\")
    - spark.sql(f\"\"\"...\"\"\", ...)
    """
    
    name = "pyspark"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._jinja_templater = JinjaTemplater(**kwargs)
    
    def process(
        self,
        *,
        fname: str,
        in_str: Optional[str] = None,
        config: Optional["FluffConfig"] = None,
        formatter: Optional["OutputStreamFormatter"] = None,
    ) -> tuple[TemplatedFile, list[SQLTemplaterError]]:
        """
        Process the input string to extract SQL from PySpark f-strings or regular SQL.
        
        For Python files: extracts SQL from f-strings and processes them
        For SQL files: delegates to the parent Jinja templater
        
        Args:
            fname: The filename being processed
            in_str: The input string containing SQL or Python code
            config: SQLFluff configuration
            formatter: SQLFluff formatter
            
        Returns:
            Tuple of (TemplatedFile, list[SQLTemplaterError])
        """
        if in_str is None:
            return TemplatedFile("", []), []
        
        # Check if this is a Python file
        if fname and fname.endswith('.py'):
            # This is a Python file - extract SQL from f-strings
            return self._process_python_file(in_str, fname, config, formatter)
        else:
            # This is a regular SQL file - delegate to Jinja templater
            # Use subprocess to ensure consistent formatting
            import tempfile
            import subprocess
            import os
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
                temp_file.write(in_str)
                temp_file_path = temp_file.name
            
            try:
                # Use subprocess to run SQLFluff fix
                result = subprocess.run([
                    'uv', 'run', 'sqlfluff', 'fix', '--dialect', 'databricks', temp_file_path
                ], capture_output=True, text=True, cwd='/home/th/repos/bi-datalake-hdl')
                
                print(f"DEBUG: SQLFluff return code: {result.returncode}")
                print(f"DEBUG: SQLFluff stdout: {result.stdout}")
                print(f"DEBUG: SQLFluff stderr: {result.stderr}")
                
                # Read the fixed SQL
                with open(temp_file_path, 'r') as f:
                    fixed_sql = f.read()
                
                print(f"DEBUG: Fixed SQL: {repr(fixed_sql)}")
                
                # Create proper slices for the TemplatedFile
                raw_slices = [RawFileSlice(
                    raw=fixed_sql,
                    slice_type="literal",
                    source_idx=0
                )]
                
                templated_slices = [TemplatedFileSlice(
                    slice_type="literal",
                    source_slice=slice(0, len(fixed_sql)),
                    templated_slice=slice(0, len(fixed_sql))
                )]
                
                return TemplatedFile(
                    source_str=fixed_sql,
                    fname=fname,
                    templated_str=fixed_sql,
                    sliced_file=templated_slices,
                    raw_sliced=raw_slices
                ), []
                
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
    
    def _process_python_file(
        self, 
        in_str: str, 
        fname: str, 
        config: Optional["FluffConfig"], 
        formatter: Optional["OutputStreamFormatter"]
    ) -> tuple[TemplatedFile, list[SQLTemplaterError]]:
        """
        Process a Python file to extract and process SQL from f-strings.
        """
        import tempfile
        import os
        
        # Extract variables from the Python code
        variables = self._extract_variables(in_str)
        
        # Extract SQL from PySpark f-strings
        sql_blocks = self._extract_sql_blocks(in_str)
        
        if not sql_blocks:
            # No SQL blocks found, return the original file
            raw_slices = [RawFileSlice(
                raw=in_str,
                slice_type="literal",
                source_idx=0
            )]
            
            templated_slices = [TemplatedFileSlice(
                slice_type="literal",
                source_slice=slice(0, len(in_str)),
                templated_slice=slice(0, len(in_str))
            )]
            
            return TemplatedFile(
                source_str=in_str,
                fname=fname,
                templated_str=in_str,
                sliced_file=templated_slices,
                raw_sliced=raw_slices
            ), []
        
        # Sort SQL blocks by position to process them in order
        sql_blocks.sort(key=lambda x: x[1]['start_pos'])
        
        # Process each SQL block and replace it in the original Python code
        modified_content = in_str
        offset = 0  # Track cumulative offset changes
        
        for i, (sql_content, source_info) in enumerate(sql_blocks):
            # Substitute variables in the SQL content
            substituted_sql = self._substitute_variables(sql_content, variables)
            
            # Create a temporary file with the SQL
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
                temp_file.write(substituted_sql)
                temp_file_path = temp_file.name
            
            try:
                # Use subprocess to run SQLFluff fix
                import subprocess
                result = subprocess.run([
                    'uv', 'run', 'sqlfluff', 'fix', '--dialect', 'databricks', temp_file_path
                ], capture_output=True, text=True, cwd='/home/th/repos/bi-datalake-hdl')
                
                # Read the fixed SQL
                with open(temp_file_path, 'r') as f:
                    fixed_sql = f.read()
                
                # Revert variable substitution to get back original variable names
                reverted_sql = self._revert_variable_substitution(fixed_sql, variables)
                
                # Create the new f-string with fixed SQL
                original_f_string = source_info.get('original_f_string', '')
                new_f_string = self._create_f_string_with_sql(reverted_sql, original_f_string)
                
                # Replace the original f-string with the fixed one
                start_pos = source_info['start_pos'] + offset
                end_pos = source_info['end_pos'] + offset
                
                # Update the content
                modified_content = modified_content[:start_pos] + new_f_string + modified_content[end_pos:]
                
                # Update offset for next replacement
                offset += len(new_f_string) - (end_pos - start_pos)
                
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
        
        # Process each SQL block and replace it in the original Python code
        modified_content = in_str
        offset = 0  # Track cumulative offset changes
        
        for i, (sql_content, source_info) in enumerate(sql_blocks):
            # Substitute variables in the SQL content
            substituted_sql = self._substitute_variables(sql_content, variables)
            
            # Create a temporary file with the SQL
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
                temp_file.write(substituted_sql)
                temp_file_path = temp_file.name
            
            try:
                # Use subprocess to run SQLFluff fix
                import subprocess
                result = subprocess.run([
                    'uv', 'run', 'sqlfluff', 'fix', '--dialect', 'databricks', temp_file_path
                ], capture_output=True, text=True, cwd='/home/th/repos/bi-datalake-hdl')
                
                # Read the fixed SQL
                with open(temp_file_path, 'r') as f:
                    fixed_sql = f.read()
                
                # Revert variable substitution to get back original variable names
                reverted_sql = self._revert_variable_substitution(fixed_sql, variables)
                
                # Create the new f-string with fixed SQL
                original_f_string = source_info.get('original_f_string', '')
                new_f_string = self._create_f_string_with_sql(reverted_sql, original_f_string)
                
                # Replace the original f-string with the fixed one
                start_pos = source_info['start_pos'] + offset
                end_pos = source_info['end_pos'] + offset
                
                # Update the content
                modified_content = modified_content[:start_pos] + new_f_string + modified_content[end_pos:]
                
                # Update offset for next replacement
                offset += len(new_f_string) - (end_pos - start_pos)
                
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
        
        # Create proper slices for the TemplatedFile
        raw_slices = [RawFileSlice(
            raw=modified_content,
            slice_type="literal",
            source_idx=0
        )]
        
        templated_slices = [TemplatedFileSlice(
            slice_type="literal",
            source_slice=slice(0, len(modified_content)),
            templated_slice=slice(0, len(modified_content))
        )]
        
        # Create a TemplatedFile with the modified Python content
        return TemplatedFile(
            source_str=modified_content,
            fname=fname,
            templated_str=modified_content,
            sliced_file=templated_slices,
            raw_sliced=raw_slices
        ), []
    
    def _extract_sql_blocks(self, content: str) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Extract SQL blocks from PySpark f-string patterns.
        
        Returns:
            List of tuples containing (sql_content, source_info)
        """
        sql_blocks = []
        
        # Pattern to match spark.sql(f"""...""") or spark.sql(f'...')
        # This handles both triple-quoted and single-quoted f-strings
        patterns = [
            # Triple-quoted f-strings
            r'spark\.sql\s*\(\s*f\s*"""\s*(.*?)\s*"""\s*\)',
            r'spark\.sql\s*\(\s*f\s*\'\'\'\s*(.*?)\s*\'\'\'\s*\)',
            # Single-quoted f-strings (but be careful with nested quotes)
            r'spark\.sql\s*\(\s*f\s*"([^"]*)"\s*\)',
            r'spark\.sql\s*\(\s*f\s*\'([^\']*)\'\s*\)',
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, content, re.DOTALL | re.MULTILINE)
            for match in matches:
                sql_content = match.group(1).strip()
                if sql_content:
                    # Calculate line number
                    line_no = content[:match.start()].count('\n') + 1
                    
                    sql_blocks.append((
                        sql_content,
                        {
                            'start_line': line_no,
                            'start_pos': match.start(),
                            'end_pos': match.end(),
                            'pattern': pattern,
                            'original_f_string': match.group(0)
                        }
                    ))
        
        return sql_blocks
    
    def _extract_variables(self, content: str) -> Dict[str, Any]:
        """
        Extract variable definitions from Python code.
        
        This method parses the Python code to find variable assignments
        and function parameters that could be used in f-strings.
        """
        variables = {}
        
        try:
            # Parse the Python code
            tree = ast.parse(content)
            
            # Walk through the AST to find variable assignments and function parameters
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    # Handle simple assignments like: variable = value
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            var_name = target.id
                            
                            # Try to evaluate the value
                            try:
                                # Handle string literals
                                if isinstance(node.value, ast.Str):
                                    variables[var_name] = node.value.s
                                elif isinstance(node.value, ast.Constant):
                                    variables[var_name] = node.value.value
                                # Handle numbers
                                elif isinstance(node.value, ast.Num):
                                    variables[var_name] = node.value.n
                                # Handle boolean values
                                elif isinstance(node.value, ast.NameConstant):
                                    variables[var_name] = node.value.value
                                # Handle None
                                elif isinstance(node.value, ast.Constant) and node.value.value is None:
                                    variables[var_name] = None
                                # For complex expressions, use a placeholder
                                else:
                                    variables[var_name] = f"__{var_name.upper()}__"
                            except:
                                # If we can't evaluate, use a placeholder
                                variables[var_name] = f"__{var_name.upper()}__"
                
                elif isinstance(node, ast.FunctionDef):
                    # Handle function parameters
                    for arg in node.args.args:
                        if isinstance(arg, ast.arg):
                            var_name = arg.arg
                            # Use a placeholder for function parameters
                            variables[var_name] = f"__{var_name.upper()}__"
                                
        except SyntaxError:
            # If parsing fails, return empty variables
            pass
            
        return variables
    
    def _revert_variable_substitution(self, sql_content: str, variables: Dict[str, Any]) -> str:
        """
        Revert variable substitution to get back original variable names.
        """
        reverted_sql = sql_content
        
        # Sort by length (longest first) to avoid partial replacements
        sorted_vars = sorted(variables.items(), key=lambda x: len(str(x[1])), reverse=True)
        
        for var_name, var_value in sorted_vars:
            if var_value is None:
                substituted_value = "NULL"
            elif isinstance(var_value, str):
                substituted_value = f"'{var_value}'"
            else:
                substituted_value = str(var_value)
            
            # Replace back to variable name
            pattern = f"{{{var_name}}}"
            reverted_sql = reverted_sql.replace(substituted_value, pattern)
        
        return reverted_sql
    
    def _create_f_string_with_sql(self, sql_content: str, original_f_string: str) -> str:
        """
        Create a new f-string with the fixed SQL content.
        """
        # Clean up the SQL content - remove extra whitespace but preserve structure
        sql_content = sql_content.strip()
        
        # Determine the quote style from the original f-string
        if '"""' in original_f_string:
            quote_start = 'f"""'
            quote_end = '"""'
        elif "'''" in original_f_string:
            quote_start = "f'''"
            quote_end = "'''"
        elif '"' in original_f_string:
            quote_start = 'f"'
            quote_end = '"'
        else:
            quote_start = "f'"
            quote_end = "'"
        
        # Extract the function call part
        if 'spark.sql(' in original_f_string:
            prefix = 'spark.sql('
            suffix = ')'
        else:
            # Fallback - just wrap in quotes
            return f'{quote_start}{sql_content}{quote_end}'
        
        return f'{prefix}{quote_start}{sql_content}{quote_end}{suffix}'
    
    def _substitute_variables(self, sql_content: str, variables: Dict[str, Any]) -> str:
        """
        Substitute variables in SQL content.
        
        This method replaces {variable_name} patterns with their values.
        """
        substituted_sql = sql_content
        
        for var_name, var_value in variables.items():
            # Replace {variable_name} with the actual value
            pattern = f"{{{var_name}}}"
            
            if var_value is None:
                substituted_value = "NULL"
                substituted_sql = substituted_sql.replace(pattern, substituted_value)
            elif isinstance(var_value, str):
                # Check if the variable is already quoted in the SQL
                quoted_pattern = f"'{pattern}'"
                double_quoted_pattern = f'"{pattern}"'
                
                if quoted_pattern in sql_content:
                    # Variable is already in single quotes, just replace the variable
                    substituted_sql = substituted_sql.replace(quoted_pattern, f"'{var_value}'")
                elif double_quoted_pattern in sql_content:
                    # Variable is already in double quotes, just replace the variable
                    substituted_sql = substituted_sql.replace(double_quoted_pattern, f'"{var_value}"')
                else:
                    # Variable is not quoted, add quotes
                    substituted_sql = substituted_sql.replace(pattern, f"'{var_value}'")
            else:
                # Convert other types to string
                substituted_sql = substituted_sql.replace(pattern, str(var_value))
        
        return substituted_sql
    
    def get_vars(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get variables from the context for templating.
        
        This is a placeholder - in a real implementation, you might want to
        extract variables from the Python code context.
        """
        return context.get('vars', {})
    
    def slice_file(self, raw_str: str, templated_str: str) -> List[Tuple[int, int, int, int]]:
        """
        Slice the file to map between raw and templated positions.
        
        This is used for error reporting and mapping back to original positions.
        """
        # For now, return a simple 1:1 mapping
        # In a more sophisticated implementation, you'd want to map
        # the extracted SQL positions back to the original Python file
        return [(0, len(raw_str), 0, len(templated_str))]