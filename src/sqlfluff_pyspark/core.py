"""
Core functionality for wrapping sqlfluff commands.
"""

import logging
import tempfile
from pathlib import Path
from typing import List, Dict, Any
from sqlfluff import lint, parse, fix
from sqlfluff.core import FluffConfig

# Configure logging
logger = logging.getLogger(__name__)


def analyze_temp_directory(
    config_path: str, fix_sql: bool = False
) -> List[Dict[str, Any]]:
    """
    Wrapper function that uses sqlfluff Python library to analyze a temp directory.
    Uses tempfile module to create a temporary directory.

    Args:
        config_path: Required path to an existing .sqlfluff config file
        fix_sql: If True, apply sqlfluff fixes to the SQL files (default: False)

    Returns:
        List of dictionaries containing analysis results for each SQL file

    Raises:
        FileNotFoundError: If the config file does not exist
        ValueError: If the config file cannot be loaded
    """
    # Validate that config file exists
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    # Create a temporary directory using tempfile module
    temp_dir = Path(tempfile.mkdtemp())

    logger.info(f"Analyzing SQL files in: {temp_dir}")

    # Find all SQL files in the temp directory
    sql_files = list(temp_dir.glob("*.sql"))

    if not sql_files:
        logger.info(f"No SQL files found in {temp_dir}")
        return []

    # Load sqlfluff config from the required config file
    try:
        sqlfluff_config = FluffConfig.from_path(config_path)
        logger.info(f"Loaded config from: {config_path}")
    except Exception as e:
        raise ValueError(f"Failed to load configuration from {config_path}: {e}") from e

    results = []
    for sql_file in sql_files:
        logger.info(f"Analyzing: {sql_file.name}")
        try:
            # Read the SQL file content
            with open(sql_file, "r") as f:
                original_sql = f.read()

            sql_content = original_sql

            # Fix SQL if requested
            fixed_sql = None
            if fix_sql:
                logger.info(f"Applying fixes to {sql_file.name}")
                try:
                    fixed_sql = fix(sql_content, config=sqlfluff_config)
                    if fixed_sql != original_sql:
                        # Write the fixed content back to the file
                        with open(sql_file, "w") as f:
                            f.write(fixed_sql)
                        logger.info(f"Fixed {sql_file.name} - changes applied")
                        sql_content = fixed_sql  # Use fixed content for linting
                    else:
                        logger.info(f"No fixes needed for {sql_file.name}")
                except Exception as fix_error:
                    logger.warning(f"Failed to fix {sql_file.name}: {fix_error}")
                    # Continue with original content if fixing fails

            # Parse the SQL file
            parsed = parse(sql_content, config=sqlfluff_config)

            # Lint the SQL file (after fixing if applicable)
            # Returns List[Dict[str, Any]] - structured information as dictionaries
            lint_result = lint(sql_content, config=sqlfluff_config)

            results.append(
                {
                    "file": sql_file.name,
                    "path": str(sql_file),
                    "parsed": parsed,
                    "violations": lint_result,
                    "fixed": fixed_sql is not None and fixed_sql != original_sql
                    if fix_sql
                    else False,
                    "original_content": original_sql,
                    "fixed_content": fixed_sql if fix_sql else None,
                }
            )

            if lint_result:
                logger.warning(f"Found {len(lint_result)} violation(s) in {sql_file}")
                for violation in lint_result:
                    # lint_result returns structured dictionaries with keys:
                    # - "code": rule code (e.g., "CP01")
                    # - "line_no": line number (sqlfluff < 4.0)
                    # - "start_line_no"/"end_line_no": line numbers (sqlfluff >= 4.0)
                    # - "line_pos": position on the line (sqlfluff < 4.0)
                    # - "start_line_pos"/"end_line_pos": positions (sqlfluff >= 4.0)
                    # - "description": description of the violation
                    line_no = (
                        violation.get("line_no")
                        or violation.get("start_line_no")
                        or violation.get("end_line_no")
                        or "?"
                    )
                    line_pos = (
                        violation.get("line_pos")
                        or violation.get("start_line_pos")
                        or violation.get("end_line_pos")
                        or "?"
                    )
                    code = violation.get("code", "UNKNOWN")
                    description = violation.get("description", "No description")

                    logger.error(
                        f"Error at {sql_file}:{line_no}:{line_pos} - {code}: {description}"
                    )
            else:
                logger.info(f"No violations found in {sql_file.name}")

        except Exception as e:
            logger.error(f"Error analyzing {sql_file}: {e}", exc_info=True)
            results.append(
                {"file": sql_file.name, "path": str(sql_file), "error": str(e)}
            )

    return results


def lint_sql(sql: str, config_path: str) -> List[Dict[str, Any]]:
    """
    Lint a SQL string using sqlfluff.

    Args:
        sql: SQL string to lint
        config_path: Path to an existing .sqlfluff config file

    Returns:
        List of violation dictionaries

    Raises:
        FileNotFoundError: If the config file does not exist
        ValueError: If the config file cannot be loaded
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        sqlfluff_config = FluffConfig.from_path(config_path)
    except Exception as e:
        raise ValueError(f"Failed to load configuration from {config_path}: {e}") from e

    return lint(sql, config=sqlfluff_config)


def parse_sql(sql: str, config_path: str) -> Dict[str, Any]:
    """
    Parse a SQL string using sqlfluff.

    Args:
        sql: SQL string to parse
        config_path: Path to an existing .sqlfluff config file

    Returns:
        Dictionary containing the parsed structure

    Raises:
        FileNotFoundError: If the config file does not exist
        ValueError: If the config file cannot be loaded
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        sqlfluff_config = FluffConfig.from_path(config_path)
    except Exception as e:
        raise ValueError(f"Failed to load configuration from {config_path}: {e}") from e

    return parse(sql, config=sqlfluff_config)


def fix_sql(sql: str, config_path: str) -> str:
    """
    Fix a SQL string using sqlfluff.

    Args:
        sql: SQL string to fix
        config_path: Path to an existing .sqlfluff config file

    Returns:
        Fixed SQL string

    Raises:
        FileNotFoundError: If the config file does not exist
        ValueError: If the config file cannot be loaded
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        sqlfluff_config = FluffConfig.from_path(config_path)
    except Exception as e:
        raise ValueError(f"Failed to load configuration from {config_path}: {e}") from e

    return fix(sql, config=sqlfluff_config)
