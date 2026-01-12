"""
Reusable UI Components
Includes hierarchical dropdowns and other shared UI elements
"""
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
    Build a hierarchical structure from flat NAICS dictionary that ensures:
    - 3-digit codes are the top level
    - 4-digit codes nest under their 3-digit parent
    - 5- and 6-digit codes nest under their 4-digit parent
    """
    hierarchy: Dict[str, Dict] = {}

    # Prepare unique sets of prefixes
    three_digit_prefixes = sorted({code[:3] for code in naics_dict})
    four_digit_prefixes = sorted({code[:4] for code in naics_dict if len(code) >= 4})

    # Helper to get a friendly label, falling back to the code
    def _label_for(code: str) -> str:
        return naics_dict.get(code, f"NAICS {code}")

    # Create or update 3-digit parents
    for prefix in three_digit_prefixes:
        hierarchy[prefix] = {"name": _label_for(prefix), "children": {}}

    # Track 4-digit nodes for easy lookup
    four_digit_nodes: Dict[str, Dict] = {}
    for code in four_digit_prefixes:
        parent_prefix = code[:3]
        parent_node = hierarchy.setdefault(
            parent_prefix,
            {"name": _label_for(parent_prefix), "children": {}}
        )
        node = {"name": _label_for(code), "children": {}}
        parent_node["children"][code] = node
        four_digit_nodes[code] = node

    # Place 5- and 6-digit codes under their 4-digit parent
    for code in sorted(naics_dict):
        if len(code) < 5:
            continue
        parent_code = code[:4]
        parent_node = four_digit_nodes.get(parent_code)
        if parent_node is None:
            parent_prefix = parent_code[:3]
            parent_node = hierarchy.setdefault(
                parent_prefix,
                {"name": _label_for(parent_prefix), "children": {}}
            )["children"].setdefault(
                parent_code,
                {"name": _label_for(parent_code), "children": {}}
            )
            four_digit_nodes[parent_code] = parent_node
        parent_node["children"][code] = {"name": _label_for(code), "children": {}}

    return hierarchy


def convert_to_ant_tree_format(hierarchy: Dict[str, Dict]) -> List[Dict]:
    """
    Convert hierarchy to st_ant_tree format.

    Format: [{"value": "code", "title": "code - name", "children": [...]}]
    """
    tree_data = []

    def process_node(code: str, data: Dict) -> Dict:
        node = {
            "value": code,
            "title": f"{code} - {data['name']}"
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
        )


def _render_fallback_selector(
    hierarchy: Dict[str, Dict],
    naics_dict: Dict[str, str],
    key: str,
    default_index: int,
    container=None,
    multi_select: bool = False,
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
        default_option = options[default_index] if options else None
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

    selected_display = container.selectbox(
        "Select Industry Type",
        options=options,
        index=default_index,
        key=key,
        help="Select NAICS industry code"
    )

    return option_to_code.get(selected_display, list(naics_dict.keys())[0] if naics_dict else "")
