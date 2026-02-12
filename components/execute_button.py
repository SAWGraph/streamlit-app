"""
Shared execute button component for analyses.
Consolidates the repeated execute query button pattern.
"""
from __future__ import annotations

from typing import List, Optional
import streamlit as st


def render_execute_button(
    disabled: bool = False,
    missing_fields: Optional[List[str]] = None,
    help_text: Optional[str] = None,
    key: Optional[str] = None,
    label: str = "Execute Query"
) -> bool:
    """
    Render a standardized execute query button in the sidebar.

    Args:
        disabled: Whether the button should be disabled
        missing_fields: List of missing required fields (used to build help text)
        help_text: Custom help text (overrides auto-generated from missing_fields)
        key: Unique key for the button
        label: Button label text

    Returns:
        True if button was clicked, False otherwise

    Example:
        clicked = render_execute_button(
            disabled=not has_county,
            missing_fields=["county"] if not has_county else None
        )
    """
    # Build help text from missing fields if not provided
    if help_text is None:
        if missing_fields:
            help_text = f"Select {', '.join(missing_fields)} first"
        else:
            help_text = "Execute the analysis"

    button_kwargs = {
        "label": label,
        "type": "primary",
        "use_container_width": True,
        "disabled": disabled,
        "help": help_text,
    }

    if key:
        button_kwargs["key"] = key

    return st.sidebar.button(**button_kwargs)


def check_required_fields(**fields) -> tuple[bool, List[str]]:
    """
    Check which required fields are missing.

    Args:
        **fields: Field names and their values (e.g., county=county_code, industry=naics_code)

    Returns:
        Tuple of (can_execute: bool, missing_fields: List[str])

    Example:
        can_execute, missing = check_required_fields(
            county=context.selected_county_code,
            industry=selected_naics_code
        )
    """
    missing = [name for name, value in fields.items() if not value]
    can_execute = len(missing) == 0
    return can_execute, missing
