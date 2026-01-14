"""
Analysis Registry - Centralized configuration for all analyses
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional
import pandas as pd


@dataclass
class AnalysisContext:
    """Context object passed to each analysis with shared data and configuration"""
    # Data (loaded once, shared across all analyses)
    states_df: pd.DataFrame
    counties_df: pd.DataFrame
    subdivisions_df: pd.DataFrame
    substances_df: pd.DataFrame
    material_types_df: pd.DataFrame
    
    # Region selection (computed from sidebar selections)
    selected_state_code: Optional[str]
    selected_state_name: Optional[str]
    selected_county_code: Optional[str]
    selected_county_name: Optional[str]
    selected_subdivision_code: Optional[str]
    selected_subdivision_name: Optional[str]
    
    # Computed region code for queries
    region_code: str  # e.g., "23", "23005", or "2301104475"
    region_display: str  # e.g., "Maine" or "Penobscot County, Maine"
    
    # Configuration
    endpoints: dict  # ENDPOINT_URLS
    project_dir: str
    
    # Analysis metadata
    analysis_key: str  # "upstream", "downstream", "near_facilities", etc.
    query_number: int  # 1, 2, 3, 4, 5


@dataclass(frozen=True)
class AnalysisSpec:
    """Specification for an analysis type"""
    key: str
    label: str
    title: str
    description: str
    query: int
    enabled: bool
    runner: Callable[[AnalysisContext], None]


def _disabled_stub(key: str) -> Callable[[AnalysisContext], None]:
    """Create a stub function for disabled analyses"""
    def _run(context: AnalysisContext) -> None:
        import streamlit as st
        st.warning(f"'{key}' is currently disabled.")
    return _run


def build_registry() -> dict[str, AnalysisSpec]:
    """
    Build the analysis registry with lazy imports to avoid loading all modules at startup.
    """
    # Lazy imports prevent unnecessary module loading and side effects at import time
    from analyses.pfas_upstream import main as upstream_main
    from analyses.pfas_downstream import main as downstream_main
    from analyses.samples_near_facilities import main as near_facilities_main
    from analyses.regional_overview import main as regional_main
    from analyses.facility_risk import main as risk_main
    from analyses.sockg_sites import main as sockg_main
    
    specs = [
        AnalysisSpec(
            key="near_facilities",
            label="Samples Near Facilities",
            title="🏭 Samples Near Facilities",
            description="Explore PFAS samples located near facilities of specific industries.",
            query=2,
            enabled=True,
            runner=near_facilities_main,
        ),
        AnalysisSpec(
            key="downstream",
            label="PFAS Downstream Tracing",
            title="⬇️ PFAS Downstream Tracing",
            description="Explore PFAS samples that are downstream from facilities of specific industries.",
            query=5,
            enabled=True,
            runner=downstream_main,
        ),
        AnalysisSpec(
            key="upstream",
            label="PFAS Upstream Tracing",
            title="🌊 PFAS Upstream Tracing",
            description="Trace facilities that might be potential PFAS sources upstream from specific samples.",
            query=1,
            enabled=True,
            runner=upstream_main,
        ),
        AnalysisSpec(
            key="sockg_sites",
            label="SOCKG Sites & Facilities",
            title="🧪 SOCKG Sites & Facilities",
            description="View SOCKG locations and nearby facilities (optional state filter).",
            query=6,
            enabled=True,
            runner=sockg_main,
        ),
        AnalysisSpec(
            key="regional",
            label="Regional Contamination Overview",
            title="📊 Regional Contamination Overview",
            description="High-level regional summary of detections and hotspots.",
            query=3,
            enabled=False,
            runner=regional_main,
        ),
        AnalysisSpec(
            key="risk",
            label="Facility Risk Assessment",
            title="⚠️ Facility Risk Assessment",
            description="Assess facility risk based on proximity, detections, and indicators.",
            query=4,
            enabled=False,  # Currently a stub
            runner=risk_main,
        ),
    ]
    
    # Ensure unique keys
    registry = {s.key: s for s in specs}
    if len(registry) != len(specs):
        dupes = [s.key for s in specs if [x.key for x in specs].count(s.key) > 1]
        raise ValueError(f"Duplicate analysis keys found: {sorted(set(dupes))}")
    
    # Swap runner for disabled analyses (optional UX improvement)
    for k, spec in list(registry.items()):
        if not spec.enabled:
            registry[k] = AnalysisSpec(**{**spec.__dict__, "runner": _disabled_stub(spec.key)})
    
    return registry
