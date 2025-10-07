"""
SQLFluff wrapper for PySpark templater.

This module provides a custom SQLFluff wrapper that uses our PySpark templater
to extract and analyze SQL from PySpark f-string blocks.
"""

import os
import tempfile
from pathlib import Path
from typing import List, Optional, Dict, Any
from sqlfluff.api.simple import lint, fix
from sqlfluff.core.config import FluffConfig
from sqlfluff.core.linter import Linter
from sqlfluff.core.templaters import RawTemplater
from .templater import PySparkTemplater


class PySparkSQLFluffWrapper:
    """
    A wrapper that combines PySpark templater with SQLFluff functionality.
    
    This class extracts SQL from PySpark f-strings and then runs SQLFluff
    analysis on the extracted SQL.
    """
    
    def __init__(self, config: Optional[FluffConfig] = None, dialect: str = "sparksql"):
        """
        Initialize the wrapper.
        
        Args:
            config: Optional SQLFluff configuration. If None, uses default config.
            dialect: SQL dialect to use. Defaults to "sparksql" for PySpark.
        """
        if config is None:
            # Try to load from current directory, fallback to default with dialect
            try:
                config = FluffConfig.from_path(".")
            except:
                config = FluffConfig(overrides={"core": {"dialect": dialect}})
        
        self.config = config
        self.templater = PySparkTemplater()
    
    def extract_sql_blocks(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract SQL blocks from a PySpark Python file.
        
        Args:
            file_path: Path to the Python file containing PySpark SQL
            
        Returns:
            List of dictionaries containing SQL blocks and metadata
        """
        with open(file_path, 'r') as f:
            content = f.read()
        
        sql_blocks = self.templater._extract_sql_blocks(content)
        
        result = []
        for i, (sql_content, source_info) in enumerate(sql_blocks):
            result.append({
                'index': i,
                'sql': sql_content,
                'start_line': source_info['start_line'],
                'start_pos': source_info['start_pos'],
                'end_pos': source_info['end_pos'],
                'pattern': source_info['pattern']
            })
        
        return result
    
    def lint_file(self, file_path: str) -> Dict[str, Any]:
        """
        Lint SQL blocks in a PySpark Python file.
        
        Args:
            file_path: Path to the Python file containing PySpark SQL
            
        Returns:
            Dictionary containing linting results for each SQL block
        """
        sql_blocks = self.extract_sql_blocks(file_path)
        
        if not sql_blocks:
            return {
                'file': file_path,
                'blocks': [],
                'summary': {
                    'total_blocks': 0,
                    'total_violations': 0
                }
            }
        
        results = {
            'file': file_path,
            'blocks': [],
            'summary': {
                'total_blocks': len(sql_blocks),
                'total_violations': 0
            }
        }
        
        for block in sql_blocks:
            # Lint the extracted SQL directly
            violations = lint(block['sql'], config=self.config)
            
            # Adjust line numbers to match original file
            adjusted_violations = []
            for violation in violations:
                # Convert violation to dict if it's an object
                if hasattr(violation, '__dict__'):
                    violation_dict = violation.__dict__.copy()
                else:
                    violation_dict = violation.copy() if isinstance(violation, dict) else {'description': str(violation)}
                
                # Adjust line number
                if 'line_no' in violation_dict:
                    violation_dict['line_no'] = block['start_line'] + violation_dict['line_no'] - 1
                elif hasattr(violation, 'line_no'):
                    violation_dict['line_no'] = block['start_line'] + violation.line_no - 1
                
                adjusted_violations.append(violation_dict)
            
            block_result = {
                'index': block['index'],
                'start_line': block['start_line'],
                'sql': block['sql'],
                'violations': adjusted_violations,
                'violation_count': len(adjusted_violations)
            }
            
            results['blocks'].append(block_result)
            results['summary']['total_violations'] += len(adjusted_violations)
        
        return results
    
    def fix_file(self, file_path: str, output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Fix SQL blocks in a PySpark Python file.
        
        Args:
            file_path: Path to the Python file containing PySpark SQL
            output_path: Optional output path. If None, overwrites the original file.
            
        Returns:
            Dictionary containing fixing results
        """
        sql_blocks = self.extract_sql_blocks(file_path)
        
        if not sql_blocks:
            return {
                'file': file_path,
                'blocks': [],
                'summary': {
                    'total_blocks': 0,
                    'fixed_blocks': 0
                }
            }
        
        # Read the original file
        with open(file_path, 'r') as f:
            original_content = f.read()
        
        lines = original_content.split('\n')
        fixed_blocks = 0
        
        # Process each SQL block
        for block in sql_blocks:
            # Create a temporary file with the extracted SQL
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
                temp_file.write(block['sql'])
                temp_file_path = temp_file.name
            
            try:
                # Fix the extracted SQL
                fixed_sql = fix(temp_file_path, config=self.config)
                
                if fixed_sql != block['sql']:
                    # Replace the SQL in the original content
                    # This is a simplified approach - in practice, you'd want more sophisticated replacement
                    start_line_idx = block['start_line'] - 1
                    end_line_idx = start_line_idx + len(block['sql'].split('\n'))
                    
                    # Find the actual SQL block in the lines
                    sql_lines = block['sql'].split('\n')
                    for i, line in enumerate(lines[start_line_idx:start_line_idx + len(sql_lines)]):
                        if sql_lines[i] in line:
                            # Replace this line with the fixed version
                            lines[start_line_idx + i] = fixed_sql.split('\n')[i]
                    
                    fixed_blocks += 1
                
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
        
        # Write the fixed content
        output_file = output_path or file_path
        with open(output_file, 'w') as f:
            f.write('\n'.join(lines))
        
        return {
            'file': file_path,
            'output_file': output_file,
            'blocks': sql_blocks,
            'summary': {
                'total_blocks': len(sql_blocks),
                'fixed_blocks': fixed_blocks
            }
        }
    
    def format_file(self, file_path: str, output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Format SQL blocks in a PySpark Python file.
        
        Args:
            file_path: Path to the Python file containing PySpark SQL
            output_path: Optional output path. If None, overwrites the original file.
            
        Returns:
            Dictionary containing formatting results
        """
        # Similar to fix_file but uses format instead of fix
        sql_blocks = self.extract_sql_blocks(file_path)
        
        if not sql_blocks:
            return {
                'file': file_path,
                'blocks': [],
                'summary': {
                    'total_blocks': 0,
                    'formatted_blocks': 0
                }
            }
        
        # Read the original file
        with open(file_path, 'r') as f:
            original_content = f.read()
        
        lines = original_content.split('\n')
        formatted_blocks = 0
        
        # Process each SQL block
        for block in sql_blocks:
            # Create a temporary file with the extracted SQL
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
                temp_file.write(block['sql'])
                temp_file_path = temp_file.name
            
            try:
                # Format the extracted SQL (using fix as a formatter)
                formatted_sql = fix(block['sql'], config=self.config)
                
                if formatted_sql != block['sql']:
                    # Replace the SQL in the original content
                    start_line_idx = block['start_line'] - 1
                    sql_lines = block['sql'].split('\n')
                    
                    # Find and replace the SQL block
                    for i, line in enumerate(lines[start_line_idx:start_line_idx + len(sql_lines)]):
                        if i < len(formatted_sql.split('\n')):
                            lines[start_line_idx + i] = formatted_sql.split('\n')[i]
                    
                    formatted_blocks += 1
                
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
        
        # Write the formatted content
        output_file = output_path or file_path
        with open(output_file, 'w') as f:
            f.write('\n'.join(lines))
        
        return {
            'file': file_path,
            'output_file': output_file,
            'blocks': sql_blocks,
            'summary': {
                'total_blocks': len(sql_blocks),
                'formatted_blocks': formatted_blocks
            }
        }