"""
Session state management for analyses.
Consolidates the repeated session state patterns.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
import streamlit as st


class AnalysisState:
    """
    Manages session state for an analysis module.

    Provides consistent key naming and state management across analyses.

    Example:
        state = AnalysisState(context.analysis_key)

        # Store results
        state.set_results({
            "samples_df": samples_df,
            "facilities_df": facilities_df,
        })

        # Check and retrieve results
        if state.has_results:
            results = state.get_results()
    """

    def __init__(self, analysis_key: str):
        """
        Initialize state manager for an analysis.

        Args:
            analysis_key: The unique key for this analysis (e.g., "pfas_upstream")
        """
        self.analysis_key = analysis_key
        self._has_results_key = f"{analysis_key}_has_results"
        self._results_key = f"{analysis_key}_results"

    @property
    def has_results(self) -> bool:
        """Check if results exist in session state."""
        return st.session_state.get(self._has_results_key, False)

    def get_results(self) -> Dict[str, Any]:
        """Get stored results from session state."""
        return st.session_state.get(self._results_key, {})

    def set_results(self, results: Dict[str, Any]) -> None:
        """
        Store results in session state.

        Args:
            results: Dictionary of result data to store
        """
        st.session_state[self._results_key] = results
        st.session_state[self._has_results_key] = True

    def clear_results(self) -> None:
        """Clear stored results from session state."""
        if self._results_key in st.session_state:
            del st.session_state[self._results_key]
        if self._has_results_key in st.session_state:
            del st.session_state[self._has_results_key]

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a custom session state value for this analysis.

        Args:
            key: The key name (will be prefixed with analysis_key)
            default: Default value if key doesn't exist
        """
        full_key = f"{self.analysis_key}_{key}"
        return st.session_state.get(full_key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Set a custom session state value for this analysis.

        Args:
            key: The key name (will be prefixed with analysis_key)
            value: Value to store
        """
        full_key = f"{self.analysis_key}_{key}"
        st.session_state[full_key] = value

    def init_if_missing(self, key: str, default: Any) -> None:
        """
        Initialize a session state key if it doesn't exist.

        Args:
            key: The key name (will be prefixed with analysis_key)
            default: Default value to set if key doesn't exist
        """
        full_key = f"{self.analysis_key}_{key}"
        if full_key not in st.session_state:
            st.session_state[full_key] = default


def check_old_session_keys(old_keys: List[str], show_warning: bool = True) -> bool:
    """
    Check for old session state keys and optionally show migration notice.

    Args:
        old_keys: List of old session state key names to check
        show_warning: Whether to show a warning message

    Returns:
        True if old keys were found, False otherwise
    """
    has_old_keys = any(key in st.session_state for key in old_keys)

    if has_old_keys and show_warning:
        if 'migration_notice_shown' not in st.session_state:
            st.warning("Session state has been reset. Please reconfigure your analysis parameters.")
            st.session_state.migration_notice_shown = True

    return has_old_keys
