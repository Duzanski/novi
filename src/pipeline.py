"""Main ETL pipeline for oil and gas well data processing."""

import logging
import pandas as pd
from typing import Dict, Any

from .config import config
from .utils import setup_logging
from .data_processors import WellsProcessor, ProductionProcessor, DatabaseLoader
from .well_spacing import WellSpacingAnalyzer

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL pipeline coordinator."""

    def __init__(self):
        """Initialize the ETL pipeline."""
        setup_logging()
        self._setup_pandas_options()

        self.wells_processor = WellsProcessor()
        self.production_processor = ProductionProcessor()
        self.db_loader = DatabaseLoader()

        self.stats: Dict[str, Any] = {}

    def _setup_pandas_options(self) -> None:
        """Configure pandas display options."""
        for option, value in config.pandas_display_options.items():
            pd.set_option(option, value)

    def run(self) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.

        Returns:
            Dictionary containing pipeline statistics and results
        """
        logger.info("Starting ETL pipeline")

        try:
            # Step 1: Process wells data
            wells_df = self._process_wells_data()

            # Step 2: Process production data
            production_df = self._process_production_data(wells_df["API10"].tolist())

            # Step 3: Calculate cumulative production and merge with wells
            wells_final = self._merge_cumulative_production(wells_df, production_df)

            # Step 4: Add well spacing features (Task 3)
            wells_final = self._add_well_spacing_features(wells_final)

            # Step 5: Load data to database
            self._load_to_database(wells_final, production_df)

            # Step 6: Calculate and log statistics
            self._calculate_statistics(wells_final)

            logger.info("ETL pipeline completed successfully")
            return self.stats

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

    def _process_wells_data(self) -> pd.DataFrame:
        """Process wells and bottom hole location data."""
        logger.info("Processing wells data")

        # Load wells data
        wells_df = self.wells_processor.load_wells_data()

        # Load bottom hole data
        self.wells_processor.load_bottom_hole_data()

        # Merge and calculate lateral lengths
        wells_df = self.wells_processor.merge_and_calculate_lateral_length()

        return wells_df

    def _process_production_data(self, valid_api10s: list) -> pd.DataFrame:
        """Process production data."""
        logger.info("Processing production data")

        production_df = self.production_processor.load_and_process_production(
            set(valid_api10s)
        )

        return production_df

    def _merge_cumulative_production(
        self, wells_df: pd.DataFrame, production_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Calculate cumulative production and merge with wells data."""
        logger.info("Calculating and merging cumulative production")

        # Calculate cumulative production
        cumulative_df = self.production_processor.calculate_cumulative_production()

        # Merge with wells data
        wells_final = pd.merge(wells_df, cumulative_df, on="API10", how="left")

        return wells_final

    def _add_well_spacing_features(self, wells_df: pd.DataFrame) -> pd.DataFrame:
        """Add well spacing features for Task 3."""
        logger.info("Adding well spacing features")

        spacing_analyzer = WellSpacingAnalyzer(wells_df)
        wells_with_spacing = spacing_analyzer.add_spacing_features()

        return wells_with_spacing

    def _load_to_database(
        self, wells_df: pd.DataFrame, production_df: pd.DataFrame
    ) -> None:
        """Load processed data to database."""
        logger.info("Loading data to database")

        # Connect to database
        self.db_loader.connect()

        # Load wells data
        self.db_loader.load_data(wells_df, "Wells")

        # Load production data
        self.db_loader.load_data(production_df, "Production")

    def _calculate_statistics(self, wells_df: pd.DataFrame) -> None:
        """Calculate pipeline statistics."""
        self.stats = {
            "total_wells": len(wells_df),
            "horizontal_wells": int(wells_df["IsHorizontalWell"].sum()),
            "avg_lateral_length_horizontal": wells_df[wells_df["IsHorizontalWell"]][
                "LateralLength"
            ].mean(),
            "wells_with_production": len(
                wells_df.dropna(subset=["CumulativeOil_Calculated"])
            ),
            "total_cumulative_oil": wells_df["CumulativeOil_Calculated"].sum(),
            "total_cumulative_gas": wells_df["CumulativeGas_Calculated"].sum(),
            "total_cumulative_water": wells_df["CumulativeWater_Calculated"].sum(),
            "avg_well_density": wells_df["WellDensityScore"].mean(),
            "avg_distance_to_nearest_horizontal": wells_df[
                "DistanceToNearestHorizontalWell"
            ].mean(),
        }

        logger.info(f"Pipeline statistics: {self.stats}")


def main():
    """Main entry point for the ETL pipeline."""
    pipeline = ETLPipeline()
    stats = pipeline.run()
    print("ETL Pipeline completed successfully!")
    print(f"Statistics: {stats}")


if __name__ == "__main__":
    main()
