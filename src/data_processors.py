"""Data processing classes for the ETL pipeline."""

import logging
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .config import config
from .utils import distance_in_feet

logger = logging.getLogger(__name__)


class WellsProcessor:
    """Process wells data from CSV files."""

    def __init__(self):
        self.wells_df: Optional[pd.DataFrame] = None
        self.bottom_hole_df: Optional[pd.DataFrame] = None

    def load_wells_data(self) -> pd.DataFrame:
        """Load and process wells data from CSV."""
        logger.info(f"Loading wells data from {config.file_paths.wells_file}")

        try:
            self.wells_df = pd.read_csv(
                config.file_paths.wells_file,
                encoding="ISO-8859-1",
                skiprows=1,
                dtype={"API": str},
            )

            # Data cleaning and transformations
            self.wells_df["API10"] = self.wells_df["API"].str[:10]
            self.wells_df["Operator"] = self.wells_df["Operator"].str.replace(
                r"[±]", "a", regex=True
            )
            self.wells_df["IsHorizontalWell"] = self.wells_df["Dir_Horiz"] == "Y"
            self.wells_df["State"] = "Utah"

            # Add PLSS column
            self._add_plss_column()

            # Select relevant columns
            self.wells_df = self.wells_df[
                [
                    "State",
                    "API10",
                    "Operator",
                    "IsHorizontalWell",
                    "Latitude",
                    "Longitude",
                    "PLSS",
                ]
            ]

            logger.info(f"Loaded {len(self.wells_df)} wells")
            return self.wells_df

        except Exception as e:
            logger.error(f"Error loading wells data: {e}")
            raise

    def _add_plss_column(self) -> None:
        """Add PLSS column in format 'SWNE 24 3S 6W'."""
        self.wells_df["PLSS"] = (
            self.wells_df["QuarterQuarter"].astype(str)
            + " "
            + self.wells_df["Sec"].astype(str)
            + " "
            + self.wells_df["Township"].astype(str)
            + self.wells_df["TownshipDir"].astype(str)
            + " "
            + self.wells_df["Range"].astype(str)
            + self.wells_df["RangeDir"].astype(str)
        )

    def load_bottom_hole_data(self) -> pd.DataFrame:
        """Load bottom hole locations data."""
        logger.info(
            f"Loading bottom hole data from {config.file_paths.bottom_hole_file}"
        )

        try:
            self.bottom_hole_df = pd.read_csv(
                config.file_paths.bottom_hole_file,
                sep="\t",
                on_bad_lines="skip",
                dtype={"API": str},
                index_col=None,
            )

            # Drop index column and process
            self.bottom_hole_df = self.bottom_hole_df.iloc[:, 1:]
            self.bottom_hole_df["API10"] = self.bottom_hole_df["API"].str[:10]

            # Filter to only wells we have in wells data
            if self.wells_df is not None:
                self.bottom_hole_df = self.bottom_hole_df[
                    self.bottom_hole_df["API10"].isin(self.wells_df["API10"])
                ]

            logger.info(f"Loaded {len(self.bottom_hole_df)} bottom hole locations")
            return self.bottom_hole_df

        except Exception as e:
            logger.error(f"Error loading bottom hole data: {e}")
            raise

    def merge_and_calculate_lateral_length(self) -> pd.DataFrame:
        """Merge wells with bottom hole data and calculate lateral lengths."""
        if self.wells_df is None or self.bottom_hole_df is None:
            raise ValueError("Must load wells and bottom hole data first")

        logger.info("Merging wells with bottom hole locations")

        # Rename columns to distinguish surface hole vs bottom hole
        wells_renamed = self.wells_df.rename(
            columns={"Latitude": "SHLLatitude", "Longitude": "SHLLongitude"}
        )

        bottom_hole_renamed = self.bottom_hole_df[
            ["API10", "Latitude", "Longitude"]
        ].rename(columns={"Latitude": "BHLatitude", "Longitude": "BHLongitude"})

        # Merge data
        merged_df = pd.merge(wells_renamed, bottom_hole_renamed, on="API10", how="left")

        # Calculate lateral lengths
        logger.info("Calculating lateral lengths")
        merged_df["LateralLength"] = merged_df.apply(
            self._calculate_lateral_length_row, axis=1
        )

        # Apply data quality filters
        self._apply_lateral_length_filters(merged_df)

        self.wells_df = merged_df
        return self.wells_df

    def _calculate_lateral_length_row(self, row: pd.Series) -> Optional[int]:
        """Calculate lateral length for a single row."""
        coords = [row.SHLLatitude, row.SHLLongitude, row.BHLatitude, row.BHLongitude]

        if pd.notnull(coords).all():
            try:
                return distance_in_feet(
                    row.SHLLatitude, row.SHLLongitude, row.BHLatitude, row.BHLongitude
                )
            except ValueError as e:
                logger.warning(f"Invalid coordinates for API {row.API10}: {e}")
                return None
        return None

    def _apply_lateral_length_filters(self, df: pd.DataFrame) -> None:
        """Apply data quality filters to lateral lengths."""
        # Remove unrealistic lateral lengths
        df.loc[
            df["LateralLength"] > config.data_limits.max_lateral_length, "LateralLength"
        ] = None

        # Remove horizontal wells with too short lateral lengths
        df.loc[
            df["IsHorizontalWell"]
            & (df["LateralLength"] < config.data_limits.min_lateral_length),
            "LateralLength",
        ] = None


class ProductionProcessor:
    """Process production data from XML files."""

    def __init__(self):
        self.production_df: Optional[pd.DataFrame] = None

    def load_and_process_production(self, valid_api10s: set) -> pd.DataFrame:
        """Load and process production data from XML."""
        logger.info(f"Loading production data from {config.file_paths.production_file}")

        try:
            self.production_df = pd.read_xml(
                config.file_paths.production_file,
                dtype={
                    c: str
                    for c in [
                        "API",
                        "api_state_code",
                        "api_county_code",
                        "api_well_code",
                    ]
                },
            )

            # Process dates and API
            for column in ["ReportPeriod", "Received"]:
                self.production_df[column] = pd.to_datetime(self.production_df[column])

            self.production_df["API10"] = (
                self.production_df["api_state_code"]
                + self.production_df["api_county_code"]
                + self.production_df["api_well_code"]
            )

            # Use ReportPeriod as Date (fixes the bug mentioned in Task 1)
            self.production_df["Date"] = self.production_df["ReportPeriod"]

            # Filter to valid wells
            self.production_df = self.production_df[
                self.production_df["API10"].isin(valid_api10s)
            ]

            # Select relevant columns
            self.production_df = self.production_df[
                ["API10", "Date", "Oil", "Gas", "Water"]
            ]

            # Apply data quality filters
            self._apply_production_filters()

            logger.info(f"Processed {len(self.production_df)} production records")
            return self.production_df

        except Exception as e:
            logger.error(f"Error loading production data: {e}")
            raise

    def _apply_production_filters(self) -> None:
        """Apply data quality filters to production data."""
        for (
            product,
            max_production,
        ) in config.data_limits.monthly_production_limits.items():
            # Remove negative values
            self.production_df.loc[self.production_df[product] < 0, product] = np.nan
            # Remove unrealistic high values
            self.production_df.loc[
                self.production_df[product] > max_production, product
            ] = np.nan

    def calculate_cumulative_production(self) -> pd.DataFrame:
        """Calculate cumulative production by well."""
        if self.production_df is None:
            raise ValueError("Must load production data first")

        logger.info("Calculating cumulative production")

        cumulative_df = (
            self.production_df.groupby("API10")[["Oil", "Gas", "Water"]]
            .sum()
            .reset_index()
        )

        # Rename columns
        cumulative_df = cumulative_df.rename(
            columns={
                "Oil": "CumulativeOil_Calculated",
                "Gas": "CumulativeGas_Calculated",
                "Water": "CumulativeWater_Calculated",
            }
        )

        return cumulative_df


class DatabaseLoader:
    """Handle database operations."""

    def __init__(self):
        self.engine: Optional[Engine] = None

    def connect(self) -> Engine:
        """Create database connection."""
        if not config.database.connection_string:
            raise ValueError("Database connection string not configured")

        try:
            self.engine = create_engine(config.database.connection_string)
            logger.info("Connected to database")
            return self.engine
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def load_data(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "replace"
    ) -> None:
        """Load dataframe to database table."""
        if self.engine is None:
            self.connect()

        try:
            df.to_sql(table_name, con=self.engine, index=False, if_exists=if_exists)
            logger.info(f"Loaded {len(df)} records to table {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to {table_name}: {e}")
            raise
