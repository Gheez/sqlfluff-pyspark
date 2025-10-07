"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def sqlfluff_config_file(tmp_path):
    """Create a temporary sqlfluff config file."""
    config_content = """[sqlfluff]
dialect = ansi
templater = raw

[sqlfluff:rules]
max_line_length = 80
"""
    config_file = tmp_path / ".sqlfluff"
    config_file.write_text(config_content)
    return str(config_file)


@pytest.fixture
def temp_sql_files(tmp_path):
    """Create temporary SQL files for testing."""
    # Create a temporary directory with SQL files
    sql_dir = tmp_path / "sql_files"
    sql_dir.mkdir()

    # Create a valid SQL file
    valid_sql = sql_dir / "valid.sql"
    valid_sql.write_text("SELECT id, name FROM users WHERE active = 1;\n")

    # Create an invalid SQL file (with violations)
    invalid_sql = sql_dir / "invalid.sql"
    invalid_sql.write_text("SeLEct * from table\n")

    # Create an empty SQL file
    empty_sql = sql_dir / "empty.sql"
    empty_sql.write_text("")

    return {
        "directory": sql_dir,
        "valid": valid_sql,
        "invalid": invalid_sql,
        "empty": empty_sql,
    }


@pytest.fixture
def sample_sql():
    """Sample SQL string for testing."""
    return "SELECT id, name FROM users WHERE active = 1"


@pytest.fixture
def invalid_sql():
    """Invalid SQL string with violations."""
    return "SeLEct * from table"
