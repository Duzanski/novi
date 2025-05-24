#!/usr/bin/env python3
"""
Main entry point for the Oil & Gas Wells ETL Pipeline.

This script processes raw well and production data from CSV/XML files
and loads the cleaned data into a PostgreSQL database.

Usage:
    python main.py
"""

import sys
import argparse
from src.pipeline import ETLPipeline


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Oil & Gas Wells ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py                    # Run full pipeline
    python main.py --log-level DEBUG  # Run with debug logging
        """,
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()

    try:
        # Initialize and run pipeline
        pipeline = ETLPipeline()
        stats = pipeline.run()

        # Print results
        print("\n" + "=" * 50)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        print(f"Total wells processed: {stats.get('total_wells', 0)}")
        print(f"Horizontal wells: {stats.get('horizontal_wells', 0)}")
        print(
            f"Average lateral length: {stats.get('avg_lateral_length_horizontal', 0):.0f} ft"
        )
        print(f"Wells with production data: {stats.get('wells_with_production', 0)}")
        print(f"Total cumulative oil: {stats.get('total_cumulative_oil', 0):,.0f} bbls")
        print(f"Total cumulative gas: {stats.get('total_cumulative_gas', 0):,.0f} mcf")
        print("=" * 50)

        return 0

    except Exception as e:
        print(f"ERROR: Pipeline failed - {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
