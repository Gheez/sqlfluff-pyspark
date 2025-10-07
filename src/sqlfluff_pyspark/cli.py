"""
Command-line interface for sqlfluff-pyspark.
"""

import argparse
import logging
import sys
from sqlfluff_pyspark.core import analyze_temp_directory

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point."""
    logger.info("SQLFluff PySpark - Analyzing temp directory")
    logger.info("=" * 50)

    parser = argparse.ArgumentParser(
        description="Analyze SQL files in a temp directory using sqlfluff"
    )
    parser.add_argument(
        "config_path",
        help="Path to an existing .sqlfluff config file (required)",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Apply sqlfluff fixes to SQL files",
    )

    args = parser.parse_args()

    try:
        results = analyze_temp_directory(config_path=args.config_path, fix_sql=args.fix)

        if results:
            logger.info("=" * 50)
            fixed_count = sum(1 for r in results if r.get("fixed", False))
            logger.info(f"Analysis complete. Processed {len(results)} file(s).")
            if args.fix and fixed_count > 0:
                logger.info(f"Fixed {fixed_count} file(s).")
    except (FileNotFoundError, ValueError) as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
