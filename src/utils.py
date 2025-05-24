"""Utility functions for the ETL pipeline."""

import math
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def distance_in_feet(lat1: float, lng1: float, lat2: float, lng2: float) -> int:
    """
    Calculate distance between two geographic points in feet.

    Uses the haversine formula: https://en.wikipedia.org/wiki/Geographical_distance

    Args:
        lat1: Latitude of first point
        lng1: Longitude of first point
        lat2: Latitude of second point
        lng2: Longitude of second point

    Returns:
        Distance in feet (rounded to nearest integer)

    Raises:
        ValueError: If any coordinate is invalid
    """
    # Validate inputs
    for coord, name in [(lat1, "lat1"), (lng1, "lng1"), (lat2, "lat2"), (lng2, "lng2")]:
        if not isinstance(coord, (int, float)) or math.isnan(coord):
            raise ValueError(f"Invalid coordinate {name}: {coord}")

    # Check latitude bounds
    if not (-90 <= lat1 <= 90) or not (-90 <= lat2 <= 90):
        raise ValueError("Latitude must be between -90 and 90 degrees")

    # Check longitude bounds
    if not (-180 <= lng1 <= 180) or not (-180 <= lng2 <= 180):
        raise ValueError("Longitude must be between -180 and 180 degrees")

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


def setup_logging(level: str = "INFO") -> None:
    """
    Set up logging configuration.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()],
    )
