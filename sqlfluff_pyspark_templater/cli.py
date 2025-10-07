"""
Command-line interface for SQLFluff PySpark Templater
"""

import sys
import argparse
import json
from pathlib import Path
from sqlfluff.core.config import FluffConfig
from .sqlfluff_wrapper import PySparkSQLFluffWrapper


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="SQLFluff PySpark Templater - Extract and analyze SQL from PySpark f-strings"
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Python files to analyze"
    )
    parser.add_argument(
        "--extract-only",
        action="store_true",
        help="Only extract SQL blocks without running SQLFluff"
    )
    parser.add_argument(
        "--lint",
        action="store_true",
        help="Lint SQL blocks using SQLFluff"
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Fix SQL blocks using SQLFluff"
    )
    parser.add_argument(
        "--format",
        action="store_true",
        help="Format SQL blocks using SQLFluff"
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file for extracted SQL or fixed code"
    )
    parser.add_argument(
        "--config",
        "-c",
        help="Path to SQLFluff config file"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    if not args.files:
        print("No files specified. Use --help for usage information.")
        sys.exit(1)
    
    # Load SQLFluff config
    config = None
    if args.config:
        config = FluffConfig.from_file(args.config)
    
    # Initialize wrapper (it will create default config with sparksql dialect if needed)
    wrapper = PySparkSQLFluffWrapper(config)
    
    results = []
    
    for file_path in args.files:
        if not Path(file_path).exists():
            print(f"File not found: {file_path}")
            continue
        
        if args.verbose:
            print(f"Processing: {file_path}")
        
        try:
            if args.extract_only:
                result = wrapper.extract_sql_blocks(file_path)
                if not args.json:
                    print(f"\n=== {file_path} ===")
                    if not result:
                        print("No SQL blocks found")
                    else:
                        for block in result:
                            print(f"\nSQL Block {block['index']+1} (line {block['start_line']}):")
                            print("-" * 50)
                            print(block['sql'])
                            print("-" * 50)
                            
                            if args.output:
                                output_path = Path(args.output)
                                if len(result) > 1:
                                    output_path = output_path.with_suffix(f".{block['index']+1}.sql")
                                
                                with open(output_path, 'w') as f:
                                    f.write(block['sql'])
                                print(f"Extracted to: {output_path}")
                
                results.append({
                    'file': file_path,
                    'action': 'extract',
                    'blocks': result
                })
            
            elif args.lint:
                result = wrapper.lint_file(file_path)
                if not args.json:
                    print(f"\n=== {file_path} ===")
                    print(f"Total SQL blocks: {result['summary']['total_blocks']}")
                    print(f"Total violations: {result['summary']['total_violations']}")
                    
                    for block in result['blocks']:
                        print(f"\nSQL Block {block['index']+1} (line {block['start_line']}):")
                        if block['violation_count'] == 0:
                            print("  ✅ No violations found")
                        else:
                            print(f"  ❌ {block['violation_count']} violations found:")
                            for violation in block['violations']:
                                line_no = getattr(violation, 'line_no', violation.get('line_no', 'unknown'))
                                description = getattr(violation, 'description', violation.get('description', str(violation)))
                                print(f"    Line {line_no}: {description}")
                
                results.append(result)
            
            elif args.fix:
                result = wrapper.fix_file(file_path, args.output)
                if not args.json:
                    print(f"\n=== {file_path} ===")
                    print(f"Total SQL blocks: {result['summary']['total_blocks']}")
                    print(f"Fixed blocks: {result['summary']['fixed_blocks']}")
                    if args.output:
                        print(f"Output written to: {result['output_file']}")
                
                results.append(result)
            
            elif args.format:
                result = wrapper.format_file(file_path, args.output)
                if not args.json:
                    print(f"\n=== {file_path} ===")
                    print(f"Total SQL blocks: {result['summary']['total_blocks']}")
                    print(f"Formatted blocks: {result['summary']['formatted_blocks']}")
                    if args.output:
                        print(f"Output written to: {result['output_file']}")
                
                results.append(result)
            
            else:
                # Default: just extract
                result = wrapper.extract_sql_blocks(file_path)
                if not args.json:
                    print(f"\n=== {file_path} ===")
                    if not result:
                        print("No SQL blocks found")
                    else:
                        for block in result:
                            print(f"\nSQL Block {block['index']+1} (line {block['start_line']}):")
                            print("-" * 50)
                            print(block['sql'])
                            print("-" * 50)
                
                results.append({
                    'file': file_path,
                    'action': 'extract',
                    'blocks': result
                })
        
        except Exception as e:
            error_msg = f"Error processing {file_path}: {str(e)}"
            if args.verbose:
                import traceback
                error_msg += f"\n{traceback.format_exc()}"
            
            if not args.json:
                print(error_msg)
            
            results.append({
                'file': file_path,
                'error': error_msg
            })
    
    if args.json:
        print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()