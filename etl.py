# %%
import math
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.expand_frame_repr", False)

logger = logging.getLogger(__name__)


def distance_in_feet(lat1: float, lng1: float, lat2: float, lng2: float) -> int:
    """https://en.wikipedia.org/wiki/Geographical_distance"""

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)

    a = math.pow(math.sin(delta_phi / 2), 2) + math.cos(phi1) * math.cos(
        phi2
    ) * math.pow(math.sin(delta_lambda / 2), 2)

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    radius_earth_feet = 6371000 * 3.2808399

    return round(radius_earth_feet * c)


# Task 2: Add PLSS column in format "SWNE 24 3S 6W"
def add_plss_column(wells_df):
    """Add PLSS column in format 'SWNE 24 3S 6W'."""
    wells_df["PLSS"] = (
        wells_df["QuarterQuarter"].astype(str)
        + " "
        + wells_df["Sec"].astype(str)
        + " "
        + wells_df["Township"].astype(str)
        + wells_df["TownshipDir"].astype(str)
        + " "
        + wells_df["Range"].astype(str)
        + wells_df["RangeDir"].astype(str)
    )
    return wells_df


def load_data(
    df: pd.DataFrame, table_name: str, engine, if_exists: str = "append"
) -> None:
    """Load dataframe to database table. Logs success or failure of the operation."""

    try:
        df.to_sql(table_name, con=engine, index=False, if_exists=if_exists)
        logger.info(f"Loaded {len(df)} records to table {table_name}")
    except Exception as e:
        logger.error(f"Failed to load data to {table_name}: {e}")
        raise


def load_wells_data():
    """Load and process wells data from CSV."""
    logger.info("Loading wells data")

    wells = pd.read_csv(
        "input/Wells.csv", encoding="ISO-8859-1", skiprows=1, dtype={"API": str}
    )

    try:
        wells["API10"] = wells["API"].str[:10]
        # wells["Operator"] = wells["Operator"].replace("±", "a") - Replace was not working so I used regex
        wells["Operator"] = wells["Operator"].str.replace(r"[±]", "a", regex=True)
        wells["IsHorizontalWell"] = wells["Dir_Horiz"] == "Y"
        wells["State"] = "Utah"

        # Add PLSS column
        wells = add_plss_column(wells)

        # Select relevant columns
        wells = wells[
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

        logger.info(f"Loaded {len(wells)} wells")
        return wells

    except Exception as e:
        logger.error(f"Error loading wells data: {e}")
        raise


def load_bottom_hole_data(wells_df):
    """Load and process bottom hole locations data from CSV."""
    logger.info("Loading bottom hole locations data")

    try:
        bottom_hole_locations = pd.read_csv(
            "input/BottomholeLocations.csv",
            sep="\t",
            on_bad_lines="skip",
            dtype={"API": str},
            index_col=None,
        )
        bottom_hole_locations = bottom_hole_locations.iloc[:, 1:]
        bottom_hole_locations["API10"] = bottom_hole_locations["API"].str[:10]
        bottom_hole_locations = bottom_hole_locations[
            bottom_hole_locations["API10"].isin(wells_df["API10"])
        ]
        logger.info(f"Loaded {len(bottom_hole_locations)} bottom hole locations")
        return bottom_hole_locations

    except Exception as e:
        logger.error(f"Error loading bottom hole data: {e}")
        raise


def merge_wells_with_bottom_holes(wells, bottom_hole_locations):
    """Merge wells with bottom hole locations and calculate lateral lengths."""
    logger.info("Merging wells with bottom hole locations")

    wells_renamed = wells.rename(
        columns={"Latitude": "SHLLatitude", "Longitude": "SHLLongitude"}
    )

    bottom_hole_renamed = bottom_hole_locations[
        ["API10", "Latitude", "Longitude"]
    ].rename(columns={"Latitude": "BHLatitude", "Longitude": "BHLongitude"})

    wells_merged = pd.merge(wells_renamed, bottom_hole_renamed, on="API10", how="left")

    logger.info("Calculating lateral lengths")
    wells_merged["LateralLength"] = wells_merged.apply(
        lambda row: (
            distance_in_feet(
                row.SHLLatitude, row.SHLLongitude, row.BHLatitude, row.BHLongitude
            )
            if pd.notnull(
                [row.SHLLatitude, row.SHLLongitude, row.BHLatitude, row.BHLongitude]
            ).all()
            else None
        ),
        axis=1,
    )

    # Apply data quality filters to lateral lengths
    wells_merged = apply_lateral_length_filters(wells_merged)

    return wells_merged


def apply_lateral_length_filters(wells):
    """Apply data quality filters to lateral lengths."""
    max_lat_length = 22_000
    wells.loc[wells["LateralLength"] > max_lat_length, "LateralLength"] = None

    min_lat_length = 2_000
    wells.loc[
        wells["IsHorizontalWell"] & (wells["LateralLength"] < min_lat_length),
        "LateralLength",
    ] = None

    return wells


def get_wells_stats(wells):
    """Get statistics about wells."""
    return {
        "NumberOfHorizontalWells": int(wells.IsHorizontalWell.sum()),
        "AvgLateralLengthHorizontalWells": wells[
            wells.IsHorizontalWell
        ].LateralLength.mean(),
    }


def load_production_data(wells_df):
    """Process production data from XML files."""
    logger.info("Loading production data from XML")

    try:
        production = pd.read_xml(
            "input/Production.xml",
            dtype={
                c: str
                for c in ["API", "api_state_code", "api_county_code", "api_well_code"]
            },
        )

        for column in ["ReportPeriod", "Received"]:
            production[column] = pd.to_datetime(production[column])
        production["API10"] = (
            production["api_state_code"]
            + production["api_county_code"]
            + production["api_well_code"]
        )

        # Task 1- As per shared graph, the date should be the report period instead of received
        production["Date"] = production["ReportPeriod"]
        production = production[production["API10"].isin(wells_df["API10"])]

        production = production[["API10", "Date", "Oil", "Gas", "Water"]]
        logger.info(f"Processed {len(production)} production records")
        return production

    except Exception as e:
        logger.error(f"Error loading production data: {e}")
        raise


def apply_production_quality_filters(production):
    """Apply data quality filters to production data."""
    monthly_limits = {"Oil": 500_000, "Gas": 5_000_000, "Water": 20_000_000}

    for product, max_production in monthly_limits.items():
        production.loc[production[product] < 0, product] = np.nan
        production.loc[production[product] > max_production, product] = np.nan

    return production


# Task 2: Calculate lifetime cumulative production from actual production data
def calculate_cumulative_production(production):
    """Calculate cumulative production by well."""
    logger.info("Calculating cumulative production")

    cumulative_production = (
        production.groupby("API10")[["Oil", "Gas", "Water"]].sum().reset_index()
    )

    cumulative_production = cumulative_production.rename(
        columns={
            "Oil": "CumulativeOil_Calculated",
            "Gas": "CumulativeGas_Calculated",
            "Water": "CumulativeWater_Calculated",
        }
    )

    return cumulative_production


def main():
    """Main ETL pipeline function."""
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_CONNECTION"))

    # Load and process wells data
    wells = load_wells_data()

    # Load bottom hole data
    bottom_hole_locations = load_bottom_hole_data(wells)

    # Merge wells with bottom hole data and calculate lateral lengths
    wells = merge_wells_with_bottom_holes(wells, bottom_hole_locations)

    # Get wells statistics
    stats = get_wells_stats(wells)
    logger.info(f"Wells stats: {stats}")

    # Load and process production data
    production = load_production_data(wells)
    production = apply_production_quality_filters(production)

    # Load production data to database
    load_data(production, "Production", engine, if_exists="append")

    # Calculate cumulative production and merge with wells
    cumulative_production = calculate_cumulative_production(production)
    wells = pd.merge(wells, cumulative_production, on="API10", how="left")

    # Load wells data to database
    load_data(wells, "Wells", engine, if_exists="append")


if __name__ == "__main__":
    main()

# %%
