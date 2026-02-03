"""Tests for CLI functionality."""

from unittest.mock import patch
import pytest
from sqlfluff_pyspark.cli import main


class TestCLI:
    """Tests for command-line interface."""

    def test_cli_without_config_path(self, capsys):
        """Test CLI without required config path."""
        with patch("sys.argv", ["sqlfluff-pyspark"]):
            with pytest.raises(SystemExit):
                main()
        # Check that error message is printed
        # Note: argparse will handle this and exit

    def test_cli_with_config_path(self, sqlfluff_config_file, capsys):
        """Test CLI with config path."""
        with patch("sys.argv", ["sqlfluff-pyspark", sqlfluff_config_file]):
            with patch("sqlfluff_pyspark.cli.analyze_temp_directory") as mock_analyze:
                mock_analyze.return_value = []
                main()
                mock_analyze.assert_called_once_with(
                    config_path=sqlfluff_config_file, fix_sql=False
                )

    def test_cli_with_fix_flag(self, sqlfluff_config_file, capsys):
        """Test CLI with --fix flag."""
        with patch("sys.argv", ["sqlfluff-pyspark", sqlfluff_config_file, "--fix"]):
            with patch("sqlfluff_pyspark.cli.analyze_temp_directory") as mock_analyze:
                mock_analyze.return_value = []
                main()
                mock_analyze.assert_called_once_with(
                    config_path=sqlfluff_config_file, fix_sql=True
                )

    def test_cli_with_results(self, sqlfluff_config_file, caplog):
        """Test CLI with results."""
        mock_results = [
            {
                "file": "test.sql",
                "path": "/tmp/test.sql",
                "parsed": {},
                "fixed": False,
            }
        ]
        with patch("sys.argv", ["sqlfluff-pyspark", sqlfluff_config_file]):
            with patch("sqlfluff_pyspark.cli.analyze_temp_directory") as mock_analyze:
                mock_analyze.return_value = mock_results
                with caplog.at_level("INFO"):
                    main()
                assert "Analysis complete" in caplog.text

    def test_cli_with_fixed_files(self, sqlfluff_config_file, caplog):
        """Test CLI reporting fixed files."""
        mock_results = [
            {
                "file": "test.sql",
                "path": "/tmp/test.sql",
                "parsed": {},
                "fixed": True,
            }
        ]
        with patch("sys.argv", ["sqlfluff-pyspark", sqlfluff_config_file, "--fix"]):
            with patch("sqlfluff_pyspark.cli.analyze_temp_directory") as mock_analyze:
                mock_analyze.return_value = mock_results
                with caplog.at_level("INFO"):
                    main()
                assert "Fixed 1 file(s)" in caplog.text

    def test_cli_error_handling(self, caplog):
        """Test CLI error handling."""
        with patch("sys.argv", ["sqlfluff-pyspark", "/nonexistent/config"]):
            with pytest.raises(SystemExit):
                with caplog.at_level("ERROR"):
                    main()
            # Error is logged via logger.error
            assert "Configuration file not found" in caplog.text
