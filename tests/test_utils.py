"""Tests for utility functions."""

import pytest
import math
from src.utils import distance_in_feet


class TestDistanceInFeet:
    """Test the distance_in_feet function."""

    def test_distance_same_point(self):
        """Test distance between same point should be 0."""
        result = distance_in_feet(40.0, -111.0, 40.0, -111.0)
        assert result == 0

    def test_distance_known_coordinates(self):
        """Test distance calculation with known coordinates."""
        # Salt Lake City to Provo (approximately 45 miles = ~237,600 feet)
        slc_lat, slc_lng = 40.7608, -111.8910
        provo_lat, provo_lng = 40.2338, -111.6585

        result = distance_in_feet(slc_lat, slc_lng, provo_lat, provo_lng)

        # Should be approximately 237,600 feet (allow 10% tolerance)
        expected = 237_600
        tolerance = 0.1
        assert abs(result - expected) <= expected * tolerance

    def test_invalid_latitude(self):
        """Test that invalid latitude raises ValueError."""
        with pytest.raises(ValueError, match="Latitude must be between"):
            distance_in_feet(91.0, -111.0, 40.0, -111.0)

        with pytest.raises(ValueError, match="Latitude must be between"):
            distance_in_feet(40.0, -111.0, -91.0, -111.0)

    def test_invalid_longitude(self):
        """Test that invalid longitude raises ValueError."""
        with pytest.raises(ValueError, match="Longitude must be between"):
            distance_in_feet(40.0, 181.0, 40.0, -111.0)

        with pytest.raises(ValueError, match="Longitude must be between"):
            distance_in_feet(40.0, -111.0, 40.0, -181.0)

    def test_nan_coordinates(self):
        """Test that NaN coordinates raise ValueError."""
        with pytest.raises(ValueError, match="Invalid coordinate"):
            distance_in_feet(float("nan"), -111.0, 40.0, -111.0)

    def test_none_coordinates(self):
        """Test that None coordinates raise ValueError."""
        with pytest.raises(ValueError, match="Invalid coordinate"):
            distance_in_feet(None, -111.0, 40.0, -111.0)
