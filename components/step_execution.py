"""
Step execution component for multi-step analyses.
Consolidates the progress columns and spinner patterns.
"""
from __future__ import annotations

from typing import Callable, Any, Optional, List, Tuple
from dataclasses import dataclass
import streamlit as st


@dataclass
class StepResult:
    """Result from a single step execution."""
    success: bool
    data: Any
    error: Optional[str] = None
    message: Optional[str] = None


class StepExecutor:
    """
    Executes analysis steps with progress indicators.

    Example:
        executor = StepExecutor(num_steps=3)

        with executor.step(1, "Finding samples...") as step:
            samples_df, error = execute_sample_query()
            if error:
                step.error(f"Failed: {error}")
            elif samples_df.empty:
                step.warning("No samples found")
            else:
                step.success(f"Found {len(samples_df)} samples")
    """

    def __init__(self, num_steps: int = 3):
        """
        Initialize step executor.

        Args:
            num_steps: Number of steps (creates that many columns)
        """
        self.num_steps = num_steps
        self.columns = st.columns(num_steps)
        self._current_step = 0

    def step(self, step_num: int, spinner_text: str):
        """
        Context manager for executing a step.

        Args:
            step_num: Step number (1-indexed)
            spinner_text: Text to show in spinner

        Returns:
            StepContext for reporting results
        """
        return StepContext(self.columns[step_num - 1], spinner_text)


class StepContext:
    """Context manager for a single step execution."""

    def __init__(self, column, spinner_text: str):
        self.column = column
        self.spinner_text = spinner_text
        self._entered = False

    def __enter__(self):
        self._entered = True
        self.column.__enter__()
        self._spinner = st.spinner(self.spinner_text)
        self._spinner.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._spinner.__exit__(exc_type, exc_val, exc_tb)
        self.column.__exit__(exc_type, exc_val, exc_tb)
        return False

    def success(self, message: str) -> None:
        """Show success message for this step."""
        st.success(message)

    def error(self, message: str) -> None:
        """Show error message for this step."""
        st.error(message)

    def warning(self, message: str) -> None:
        """Show warning message for this step."""
        st.warning(message)

    def info(self, message: str) -> None:
        """Show info message for this step."""
        st.info(message)


def run_steps(
    steps: List[Tuple[str, Callable[[], Tuple[Any, Optional[str]]]]],
    skip_on_empty: bool = True
) -> List[StepResult]:
    """
    Run multiple steps with progress columns.

    Args:
        steps: List of (spinner_text, callable) tuples.
               Each callable should return (data, error_or_none)
        skip_on_empty: Whether to skip subsequent steps if a step returns empty data

    Returns:
        List of StepResult objects

    Example:
        results = run_steps([
            ("Finding facilities...", lambda: execute_facilities_query()),
            ("Finding samples...", lambda: execute_samples_query()),
        ])
    """
    executor = StepExecutor(num_steps=len(steps))
    results = []
    should_skip = False

    for i, (spinner_text, func) in enumerate(steps, 1):
        with executor.step(i, spinner_text) as step:
            if should_skip:
                step.info(f"Step {i}: Skipped")
                results.append(StepResult(success=False, data=None, message="Skipped"))
                continue

            try:
                data, error = func()

                if error:
                    step.error(f"Step {i} failed: {error}")
                    results.append(StepResult(success=False, data=data, error=error))
                    if skip_on_empty:
                        should_skip = True
                elif data is None or (hasattr(data, 'empty') and data.empty):
                    step.info(f"Step {i}: No results found")
                    results.append(StepResult(success=True, data=data, message="No results"))
                    if skip_on_empty:
                        should_skip = True
                else:
                    count = len(data) if hasattr(data, '__len__') else "some"
                    step.success(f"Step {i}: Found {count} results")
                    results.append(StepResult(success=True, data=data))

            except Exception as e:
                step.error(f"Step {i} error: {str(e)}")
                results.append(StepResult(success=False, data=None, error=str(e)))
                if skip_on_empty:
                    should_skip = True

    return results
