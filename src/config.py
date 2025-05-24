"""Configuration settings for the ETL pipeline."""

import os
from dataclasses import dataclass, field
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    connection_string: str = os.getenv("DATABASE_CONNECTION", "")


@dataclass
class DataLimits:
    """Data validation limits and thresholds."""

    max_lateral_length: int = 22_000
    min_lateral_length: int = 2_000
    monthly_production_limits: Dict[str, int] = None

    def __post_init__(self):
        if self.monthly_production_limits is None:
            self.monthly_production_limits = {
                "Oil": 500_000,
                "Gas": 5_000_000,
                "Water": 20_000_000,
            }


@dataclass
class FilePaths:
    """File paths for input and output data."""

    input_dir: str = "input"
    wells_file: str = "input/Wells.csv"
    bottom_hole_file: str = "input/BottomholeLocations.csv"
    production_file: str = "input/Production.xml"


@dataclass
class Config:
    """Main configuration class."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    data_limits: DataLimits = field(default_factory=DataLimits)
    file_paths: FilePaths = field(default_factory=FilePaths)

    # Pandas display options
    pandas_display_options: Dict[str, Any] = None

    def __post_init__(self):
        if self.pandas_display_options is None:
            self.pandas_display_options = {
                "display.max_columns": None,
                "display.width": None,
                "display.max_colwidth": None,
                "display.expand_frame_repr": False,
            }


# Global config instance
config = Config()
