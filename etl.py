# %%
import math
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)  # Auto-detect terminal width
pd.set_option("display.max_colwidth", None)  # Don't truncate column contents
pd.set_option("display.expand_frame_repr", False)  # Don't wrap to multiple lines


# %%
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


# %%
if __name__ == "__main__":
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_CONNECTION"))

    wells = pd.read_csv(
        "input/Wells.csv", encoding="ISO-8859-1", skiprows=1, dtype={"API": str}
    )
    wells["API10"] = wells["API"].str[:10]
    # wells["Operator"] = wells["Operator"].replace("±", "a") - Replace was not working so I used regex
    wells["Operator"] = wells["Operator"].str.replace(r"[±]", "a", regex=True)
    wells["IsHorizontalWell"] = wells["Dir_Horiz"] == "Y"
    wells["State"] = "Utah"
    print(wells[wells["API10"] == "4301330372"])
    # %%
    # Task 2: Add PLSS column in format "SWNE 24 3S 6W"
    wells["PLSS"] = (
        wells["QuarterQuarter"].astype(str)
        + " "
        + wells["Sec"].astype(str)
        + " "
        + wells["Township"].astype(str)
        + wells["TownshipDir"].astype(str)
        + " "
        + wells["Range"].astype(str)
        + wells["RangeDir"].astype(str)
    )

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

    print(wells)

    # %%
    bottom_hole_locations = pd.read_csv(
        "input/BottomholeLocations.csv",
        sep="\t",
        on_bad_lines="skip",
        dtype={"API": str},
        index_col=None,
    )
    bottom_hole_locations = bottom_hole_locations.iloc[:, 1:]  # drop index column
    bottom_hole_locations["API10"] = bottom_hole_locations["API"].str[:10]
    bottom_hole_locations = bottom_hole_locations[
        bottom_hole_locations["API10"].isin(wells["API10"])
    ]
    print(bottom_hole_locations)
    # %%

    wells = pd.merge(
        wells.rename(columns={"Latitude": "SHLLatitude", "Longitude": "SHLLongitude"}),
        bottom_hole_locations[["API10", "Latitude", "Longitude"]].rename(
            columns={"Latitude": "BHLatitude", "Longitude": "BHLongitude"}
        ),
        on="API10",
        how="left",
    )

    print(wells)
    # %%
    wells["LateralLength"] = wells.apply(
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

    max_lat_length = 22_000
    wells.loc[wells["LateralLength"] > max_lat_length, "LateralLength"] = None

    min_lat_length = 2_000
    wells.loc[
        wells["IsHorizontalWell"] & (wells["LateralLength"] < min_lat_length),
        "LateralLength",
    ] = None

    stats = {
        "NumberOfHorizontalWells": int(wells.IsHorizontalWell.sum()),
        "AvgLateralLengthHorizontalWells": wells[
            wells.IsHorizontalWell
        ].LateralLength.mean(),
    }

    print(stats)

    # wells.to_sql("Wells", con=engine, index=False, if_exists="replace")  # Moved to end after cumulative calculation
    # %%
    production = pd.read_xml(
        "input/Production.xml",
        dtype={
            c: str
            for c in ["API", "api_state_code", "api_county_code", "api_well_code"]
        },
    )
    print(production)

    # %%
    for column in ["ReportPeriod", "Received"]:
        production[column] = pd.to_datetime(production[column])
    production["API10"] = (
        production["api_state_code"]
        + production["api_county_code"]
        + production["api_well_code"]
    )
    # Task 1- As per shared graph, the date should be the report period
    production["Date"] = production["ReportPeriod"]
    production = production[production["API10"].isin(wells["API10"])]

    production = production[["API10", "Date", "Oil", "Gas", "Water"]]

    monthly_limits = {"Oil": 500_000, "Gas": 5_000_000, "Water": 20_000_000}

    for product, max_production in monthly_limits.items():
        production.loc[production[product] < 0, product] = np.nan
        production.loc[production[product] > max_production, product] = np.nan

    production.to_sql("Production", con=engine, index=False, if_exists="replace")

    # %%
    # Task 2: Calculate lifetime cumulative production from actual production data
    cumulative_production = (
        production.groupby("API10")[["Oil", "Gas", "Water"]].sum().reset_index()
    )
    print(cumulative_production)
    cumulative_production = cumulative_production.rename(
        columns={
            "Oil": "CumulativeOil_Calculated",
            "Gas": "CumulativeGas_Calculated",
            "Water": "CumulativeWater_Calculated",
        }
    )

    # Merge cumulative production back to wells
    wells = pd.merge(wells, cumulative_production, on="API10", how="left")

    # Update the wells table in database with new columns
    wells.to_sql("Wells", con=engine, index=False, if_exists="replace")

# %%
