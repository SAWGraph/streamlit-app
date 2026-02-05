"""
NAICS helper utilities shared across analyses.

This module centralizes common logic for:
- Normalizing NAICS code inputs
- Determining NAICS level from code length
- Building VALUES / hierarchy fragments used in SPARQL queries
"""

from __future__ import annotations

from typing import List, Literal, Tuple


NaicsLevel = Literal["sector", "subsector", "group", "industry"]


def normalize_naics_codes(
    naics_code: str | List[str] | set[str] | tuple[str, ...] | None,
) -> list[str]:
    """
    Normalize NAICS input (string or collection) into a sorted, unique list of codes.
    """
    if naics_code is None:
        return []

    if isinstance(naics_code, (list, set, tuple)):
        codes = [str(code).strip() for code in naics_code if str(code).strip()]
    else:
        code = str(naics_code).strip()
        codes = [code] if code else []

    return sorted(set(codes))


def naics_level(code: str) -> NaicsLevel:
    """
    Classify a single NAICS code by length:
      - 2 digits: sector
      - 3 digits: subsector
      - 4 digits: industry group
      - 5–6 digits: industry
    """
    c = str(code).strip()
    if len(c) <= 2:
        return "sector"
    if len(c) == 3:
        return "subsector"
    if len(c) == 4:
        return "group"
    return "industry"


def build_naics_values_and_hierarchy(code: str) -> Tuple[str, str]:
    """
    Build a VALUES clause + optional hierarchy fragment for a single NAICS code.

    Returns:
      (values_clause, hierarchy_clause)

      - values_clause: e.g. "VALUES ?industryCode {naics:NAICS-22131}."
      - hierarchy_clause: extra fio:subcodeOf triples if needed.
    """
    c = str(code).strip()
    if not c:
        return "", ""

    level = naics_level(c)

    if level == "industry":
        # 5–6 digit NAICS industry code
        return f"VALUES ?industryCode {{naics:NAICS-{c}}}.", ""

    if level == "group":
        # 4-digit NAICS industry group
        return f"VALUES ?industryGroup {{naics:NAICS-{c}}}.", ""

    if level == "subsector":
        # 3-digit NAICS subsector
        return (
            f"VALUES ?industrySubsector {{naics:NAICS-{c}}}.",
            "?industryGroup fio:subcodeOf ?industrySubsector .",
        )

    # sector (2 digits)
    return (
        f"VALUES ?industrySector {{naics:NAICS-{c}}}.",
        "\n".join(
            [
                "?industryGroup fio:subcodeOf ?industrySubsector .",
                "?industrySubsector fio:subcodeOf ?industrySector .",
            ]
        ),
    )


def build_simple_naics_values(code: str) -> str:
    """
    Simplified helper for cases that only distinguish:
      - full industry code (>4 digits) -> ?industryCode
      - group/sector (<=4 digits)      -> ?industryGroup

    This mirrors the downstream `_build_industry_filter` behavior.
    """
    c = str(code).strip()
    if not c:
        return ""
    if len(c) > 4:
        return f"VALUES ?industryCode {{naics:NAICS-{c}}}."
    return f"VALUES ?industryGroup {{naics:NAICS-{c}}}."

