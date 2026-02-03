"""Integration tests for sqlfluff-pyspark."""

import tempfile
import shutil
from pathlib import Path
import pytest
from sqlfluff_pyspark import parse_sql, fix_sql


@pytest.mark.integration
class TestIntegration:
    """Integration tests that test the full workflow."""

    def test_full_workflow_with_temp_files(self, sqlfluff_config_file):
        """Test the full workflow with actual temp files."""
        # Create a temporary directory
        temp_dir = Path(tempfile.mkdtemp())
        try:
            # Create SQL files in the temp directory
            sql_file1 = temp_dir / "test1.sql"
            sql_file1.write_text("SELECT id, name FROM users;\n")

            sql_file2 = temp_dir / "test2.sql"
            sql_file2.write_text("SeLEct * from table\n")

            # Note: analyze_temp_directory creates its own temp directory,
            # so we can't directly test with our files. Instead, we test
            # the individual functions that work with SQL strings.

            # Test parsing
            parsed1 = parse_sql(sql_file1.read_text(), sqlfluff_config_file)
            assert isinstance(parsed1, dict)

            # Test fixing
            fixed2 = fix_sql(sql_file2.read_text(), sqlfluff_config_file)
            assert isinstance(fixed2, str)
            # Fixed SQL should be different from original (case fixed)
            assert fixed2 != sql_file2.read_text()

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_parse_fix_chain(self, sqlfluff_config_file):
        """Test chaining parse and fix operations."""
        sql = "SeLEct id, name from users where active=1"

        # Parse
        parsed = parse_sql(sql, sqlfluff_config_file)
        assert isinstance(parsed, dict)

        # Fix
        fixed = fix_sql(sql, sqlfluff_config_file)
        assert isinstance(fixed, str)

        # Fixed SQL should be different from original
        assert fixed != sql
