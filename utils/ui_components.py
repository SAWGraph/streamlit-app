"""
Reusable UI Components
Includes hierarchical dropdowns and other shared UI elements
"""
from __future__ import annotations

import streamlit as st
from typing import Dict, List, Optional

# Import st_ant_tree for dropdown tree selector
try:
    from st_ant_tree import st_ant_tree
    ANT_TREE_AVAILABLE = True
except ImportError:
    ANT_TREE_AVAILABLE = False


def build_naics_hierarchy(naics_dict: Dict[str, str]) -> Dict[str, Dict]:
    """
    Build a hierarchical structure from flat NAICS dictionary.
    Nests codes under their longest existing ancestor in the dictionary.
    This avoids generating placeholder "NAICS XXX" nodes for missing intermediate levels.
    """
    hierarchy: Dict[str, Dict] = {}
    nodes: Dict[str, Dict] = {}

    # 1. Create a node for every code in the dictionary
    for code, name in sorted(naics_dict.items()):
        nodes[code] = {
            "name": name, 
            "children": {},
            "code": code
        }

    # 2. Organize into hierarchy
    for code, node in sorted(nodes.items()):
        # Find longest existing ancestor
        parent_code = None
        for i in range(len(code) - 1, 1, -1):
            prefix = code[:i]
            if prefix in nodes:
                parent_code = prefix
                break
        
        if parent_code:
            # Add to parent's children
            nodes[parent_code]["children"][code] = node
        else:
            # No ancestor found, add to root
            hierarchy[code] = node

    return hierarchy


def convert_to_ant_tree_format(hierarchy: Dict[str, Dict]) -> List[Dict]:
    """
    Convert hierarchy to st_ant_tree format.
    
    Format: [{"value": "code", "title": "Name (Code)", "children": [...]}]
    """
    tree_data = []

    def process_node(code: str, data: Dict) -> Dict:
        # Title format: "Industry Name (Code)"
        title = f"{data['name']} ({code})"
        
        node = {
            "value": code,
            "title": title
        }
        children = data.get("children", {})
        if children:
            node["children"] = [
                process_node(child_code, child_data)
                for child_code, child_data in sorted(children.items())
            ]
        return node

    for code, data in sorted(hierarchy.items()):
        tree_data.append(process_node(code, data))

    return tree_data


def render_hierarchical_naics_selector(
    naics_dict: Dict[str, str],
    key: str,
    default_index: int = 0,
    default_value: Optional[str] = None,
    use_sidebar: bool = True,
    multi_select: bool = False,
    allow_empty: bool = False,
) -> List[str] | str:
    """
    Render a hierarchical NAICS industry selector using st_ant_tree dropdown.
    """
    hierarchy = build_naics_hierarchy(naics_dict)

    if ANT_TREE_AVAILABLE:
        tree_data = convert_to_ant_tree_format(hierarchy)
        default_val = [default_value] if default_value else None

        # Use st_ant_tree dropdown
        with st.sidebar if use_sidebar else st.container():
            selected = st_ant_tree(
                treeData=tree_data,
                placeholder="Select Industry Type...",
                allowClear=True,
                showSearch=True,
                treeLine=True,
                defaultValue=default_val,
                key=key
            )

        # Return selected code
        if selected and len(selected) > 0:
            return selected if multi_select else selected[0]
        elif default_value:
            return [default_value] if multi_select else default_value
        elif allow_empty:
            return [] if multi_select else ""
        else:
            fallback = list(naics_dict.keys())[default_index] if naics_dict else ""
            return [fallback] if multi_select and fallback else fallback

    else:
        # Fallback to selectbox
        container = st.sidebar if use_sidebar else st
        return _render_fallback_selector(
            hierarchy,
            naics_dict,
            key,
            default_index,
            container,
            multi_select=multi_select,
            allow_empty=allow_empty,
        )


def _render_fallback_selector(
    hierarchy: Dict[str, Dict],
    naics_dict: Dict[str, str],
    key: str,
    default_index: int,
    container=None,
    multi_select: bool = False,
    allow_empty: bool = False,
) -> List[str] | str:
    """Fallback selector using indented selectbox."""
    if container is None:
        container = st

    options = []
    code_to_option = {}

    def add_to_options(node_code: str, node_data: Dict, level: int = 0):
        name = node_data["name"]
        indent = "  " * level
        prefix = "├─ " if level > 0 else ""
        display_name = f"{indent}{prefix}{node_code} - {name}"
        options.append(display_name)
        code_to_option[node_code] = display_name

        for child_code, child_data in sorted(node_data.get("children", {}).items()):
            add_to_options(child_code, child_data, level + 1)

    for code, data in sorted(hierarchy.items()):
        add_to_options(code, data, level=0)

    option_to_code = {v: k for k, v in code_to_option.items()}

    if multi_select:
        default_option = options[default_index] if options and not allow_empty else None
        selected_options = container.multiselect(
            "Select Industry Type",
            options=options,
            default=[default_option] if default_option else [],
            key=key,
            help="Select NAICS industry codes"
        )
        return [
            option_to_code.get(option, "")
            for option in selected_options
            if option in option_to_code
        ]

    if allow_empty:
        options = ["-- All Industries --"] + options

    selected_display = container.selectbox(
        "Select Industry Type",
        options=options,
        index=0 if allow_empty else default_index,
        key=key,
        help="Select NAICS industry code"
    )

    if allow_empty and selected_display == "-- All Industries --":
        return ""
    return option_to_code.get(selected_display, list(naics_dict.keys())[0] if naics_dict else "")
