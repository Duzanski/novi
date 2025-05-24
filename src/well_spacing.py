"""
Well spacing analysis module.

This module implements various well spacing features to help analyze
the neighborhood characteristics of oil and gas wells.
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Tuple
from sklearn.neighbors import NearestNeighbors

from .utils import distance_in_feet

logger = logging.getLogger(__name__)


class WellSpacingAnalyzer:
    """Analyze well spacing and neighborhood characteristics."""

    def __init__(self, wells_df: pd.DataFrame):
        """
        Initialize with wells dataframe.

        Args:
            wells_df: DataFrame containing well locations and metadata
        """
        self.wells_df = wells_df.copy()
        self.horizontal_wells = wells_df[wells_df["IsHorizontalWell"]].copy()

    def add_spacing_features(self) -> pd.DataFrame:
        """
        Add well spacing features to the wells dataframe.

        Returns:
            Updated wells dataframe with spacing features
        """
        logger.info("Calculating well spacing features")

        # Feature 1: Distance to nearest horizontal well
        self.wells_df["DistanceToNearestHorizontalWell"] = (
            self._calculate_nearest_horizontal_distance()
        )

        # Feature 2: Number of horizontal wells within 1 mile
        self.wells_df["HorizontalWellsWithin1Mile"] = self._count_wells_within_radius(
            5280
        )  # 1 mile = 5280 feet

        # Feature 3: Average distance to 3 nearest horizontal wells
        self.wells_df["AvgDistanceTo3NearestHorizontal"] = (
            self._average_distance_to_k_nearest(3)
        )

        # Feature 4: Well density score (wells per square mile in 2-mile radius)
        self.wells_df["WellDensityScore"] = self._calculate_well_density_score()

        return self.wells_df

    def _calculate_nearest_horizontal_distance(self) -> pd.Series:
        """Calculate distance to nearest horizontal well for each well."""
        distances = []

        for idx, well in self.wells_df.iterrows():
            if pd.isna(well["SHLLatitude"]) or pd.isna(well["SHLLongitude"]):
                distances.append(np.nan)
                continue

            # Find nearest horizontal well (excluding self if it's horizontal)
            horizontal_candidates = self.horizontal_wells.copy()
            if well["IsHorizontalWell"]:
                horizontal_candidates = horizontal_candidates[
                    horizontal_candidates["API10"] != well["API10"]
                ]

            if len(horizontal_candidates) == 0:
                distances.append(np.nan)
                continue

            min_distance = float("inf")
            for _, other_well in horizontal_candidates.iterrows():
                if pd.notna(other_well["SHLLatitude"]) and pd.notna(
                    other_well["SHLLongitude"]
                ):
                    try:
                        dist = distance_in_feet(
                            well["SHLLatitude"],
                            well["SHLLongitude"],
                            other_well["SHLLatitude"],
                            other_well["SHLLongitude"],
                        )
                        min_distance = min(min_distance, dist)
                    except ValueError:
                        continue

            distances.append(min_distance if min_distance != float("inf") else np.nan)

        return pd.Series(distances, index=self.wells_df.index)

    def _count_wells_within_radius(self, radius_feet: float) -> pd.Series:
        """Count horizontal wells within specified radius."""
        counts = []

        for idx, well in self.wells_df.iterrows():
            if pd.isna(well["SHLLatitude"]) or pd.isna(well["SHLLongitude"]):
                counts.append(0)
                continue

            count = 0
            for _, other_well in self.horizontal_wells.iterrows():
                # Skip self if it's a horizontal well
                if well["API10"] == other_well["API10"]:
                    continue

                if pd.notna(other_well["SHLLatitude"]) and pd.notna(
                    other_well["SHLLongitude"]
                ):
                    try:
                        dist = distance_in_feet(
                            well["SHLLatitude"],
                            well["SHLLongitude"],
                            other_well["SHLLatitude"],
                            other_well["SHLLongitude"],
                        )
                        if dist <= radius_feet:
                            count += 1
                    except ValueError:
                        continue

            counts.append(count)

        return pd.Series(counts, index=self.wells_df.index)

    def _average_distance_to_k_nearest(self, k: int = 3) -> pd.Series:
        """Calculate average distance to k nearest horizontal wells."""
        avg_distances = []

        for idx, well in self.wells_df.iterrows():
            if pd.isna(well["SHLLatitude"]) or pd.isna(well["SHLLongitude"]):
                avg_distances.append(np.nan)
                continue

            distances = []
            for _, other_well in self.horizontal_wells.iterrows():
                # Skip self if it's a horizontal well
                if well["API10"] == other_well["API10"]:
                    continue

                if pd.notna(other_well["SHLLatitude"]) and pd.notna(
                    other_well["SHLLongitude"]
                ):
                    try:
                        dist = distance_in_feet(
                            well["SHLLatitude"],
                            well["SHLLongitude"],
                            other_well["SHLLatitude"],
                            other_well["SHLLongitude"],
                        )
                        distances.append(dist)
                    except ValueError:
                        continue

            if len(distances) >= k:
                distances.sort()
                avg_dist = np.mean(distances[:k])
                avg_distances.append(avg_dist)
            else:
                avg_distances.append(np.nan)

        return pd.Series(avg_distances, index=self.wells_df.index)

    def _calculate_well_density_score(self) -> pd.Series:
        """
        Calculate well density score (wells per square mile within 2-mile radius).

        This gives a measure of how crowded the area around each well is.
        """
        density_scores = []
        radius_miles = 2.0
        radius_feet = radius_miles * 5280
        area_sq_miles = np.pi * radius_miles**2

        for idx, well in self.wells_df.iterrows():
            if pd.isna(well["SHLLatitude"]) or pd.isna(well["SHLLongitude"]):
                density_scores.append(0.0)
                continue

            count = 0
            for _, other_well in self.wells_df.iterrows():
                # Skip self
                if well["API10"] == other_well["API10"]:
                    continue

                if pd.notna(other_well["SHLLatitude"]) and pd.notna(
                    other_well["SHLLongitude"]
                ):
                    try:
                        dist = distance_in_feet(
                            well["SHLLatitude"],
                            well["SHLLongitude"],
                            other_well["SHLLatitude"],
                            other_well["SHLLongitude"],
                        )
                        if dist <= radius_feet:
                            count += 1
                    except ValueError:
                        continue

            density = count / area_sq_miles
            density_scores.append(density)

        return pd.Series(density_scores, index=self.wells_df.index)
