"""
Boundary fetching utilities.
Consolidates the repeated boundary fetching logic across analyses.
"""
from __future__ import annotations

from typing import Optional, Dict, Any
import pandas as pd

from filters.region import get_region_boundary


def fetch_boundaries(
    state_code: Optional[str],
    county_code: Optional[str]
) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Fetch state and county boundaries, returning both plus the most specific one.

    Args:
        state_code: State FIPS code (e.g., "06" for California)
        county_code: County FIPS code (e.g., "06037" for Los Angeles County)

    Returns:
        Dictionary with keys:
        - 'state': State boundary DataFrame (or None)
        - 'county': County boundary DataFrame (or None)
        - 'region': The most specific boundary (county if available, else state)
    """
    state_boundary_df = (
        get_region_boundary(state_code) if state_code else None
    )
    county_boundary_df = (
        get_region_boundary(county_code) if county_code else None
    )

    # Use county boundary if available and not empty, otherwise fall back to state
    region_boundary_df = (
        county_boundary_df
        if (county_boundary_df is not None and not county_boundary_df.empty)
        else state_boundary_df
    )

    return {
        'state': state_boundary_df,
        'county': county_boundary_df,
        'region': region_boundary_df
    }
