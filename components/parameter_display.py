"""
Parameter display utilities.
Consolidates the repeated parameter table rendering across analyses.
"""
from __future__ import annotations

from typing import List, Dict, Any
import streamlit as st
import pandas as pd


def render_parameter_table(params: List[Dict[str, str]], title: str = "Selected Parameters (from executed query)") -> None:
    """
    Render a parameter summary table.

    Args:
        params: List of dicts with 'Parameter' and 'Value' keys
        title: Title for the section

    Example:
        render_parameter_table([
            {"Parameter": "Industry Type", "Value": "Sewage Treatment"},
            {"Parameter": "Geographic Region", "Value": "California > Los Angeles"},
        ])
    """
    if not params:
        return

    params_df = pd.DataFrame(params)
    st.markdown(f"### {title}")
    st.table(params_df)


def build_region_params(
    region_display: str,
    default_label: str = "All Regions"
) -> Dict[str, str]:
    """
    Build a parameter dict for region display.

    Args:
        region_display: The formatted region display string
        default_label: Default value if region_display is empty

    Returns:
        Dict with 'Parameter' and 'Value' keys
    """
    return {
        "Parameter": "Geographic Region",
        "Value": region_display or default_label
    }


def build_concentration_params(
    min_conc: float,
    max_conc: float,
    include_nondetects: bool = False
) -> Dict[str, str]:
    """
    Build a parameter dict for concentration range.

    Args:
        min_conc: Minimum concentration
        max_conc: Maximum concentration
        include_nondetects: Whether nondetects are included

    Returns:
        Dict with 'Parameter' and 'Value' keys
    """
    conc_text = f"{min_conc} - {max_conc} ng/L"
    if include_nondetects:
        conc_text += " (including nondetects)"

    return {
        "Parameter": "Detected Concentration",
        "Value": conc_text
    }


def build_industry_params(industry_display: str) -> Dict[str, str]:
    """
    Build a parameter dict for industry type.

    Args:
        industry_display: The formatted industry display string

    Returns:
        Dict with 'Parameter' and 'Value' keys
    """
    return {
        "Parameter": "Industry Type",
        "Value": industry_display or "All Industries"
    }
