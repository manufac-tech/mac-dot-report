from .db01_setup import (
    build_full_output_dict
)

from .db02_main_df import (
    build_main_dataframe,
    df_merge_2_actual,
    replace_string_blanks
)

from .db04_id_gen import (
    get_next_unique_id,
)
from .db05_item_type import (
    determine_item_type,
    is_symlink,
    is_alias,
    get_file_type,
    get_folder_type,
    resolve_item_type,
    detect_alias_type,
    detect_symlink_target_type
)
from .db06_load_rp import (
    load_rp_dataframe,
    create_git_rp_column,
    read_gitignore_items
)
from .db07_load_hm import load_hm_dataframe
from .db08_load_db import load_dotbot_yaml_dataframe
from .db09_load_di import (
    correct_and_validate_dot_info_df,
    load_di_dataframe
)

from .db18_org import (
    # add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns_main,
    sort_items_1_out_group,  # MAYBE REMOVE
    sort_items_2_indiv  # MAYBE REMOVE
)
from .db21_rep_df import (
    build_report_dataframe,
    handle_nan_values,
    filter_no_show_rows
)
from .db26_f_mg1 import (
    field_match_master,
    dot_structure_status,
    subsystem_docs,
    subsystem_db_all,
    alert_sym_overwrite
)
from .db27_f_mg2 import alert_in_doc_not_fs  # New import
from .db28_f_mg3 import (
    consolidate_fields,
    get_conditions_actions,
    remove_consolidated_columns
)

from .db30_debug import print_debug_info

__all__ = [
    "build_full_output_dict",
    "build_main_dataframe",
    "df_merge_2_actual",
    "replace_string_blanks",
    "get_next_unique_id",
    "determine_item_type",
    "is_symlink",
    "is_alias",
    "get_file_type",
    "get_folder_type",
    "resolve_item_type",
    "detect_alias_type",
    "detect_symlink_target_type",
    "load_hm_dataframe",
    "load_rp_dataframe",
    "create_git_rp_column",
    "read_gitignore_items",
    "load_dotbot_yaml_dataframe",
    "correct_and_validate_dot_info_df",
    "load_di_dataframe",
    # "add_and_populate_out_group",
    "apply_output_grouping",
    "reorder_columns_main",
    "sort_items_1_out_group",  # MAYBE REMOVE
    "sort_items_2_indiv",  # MAYBE REMOVE
    "build_report_dataframe",
    "handle_nan_values",
    "filter_no_show_rows",
    "field_match_master",
    "dot_structure_status",
    "subsystem_docs",
    "subsystem_db_all",
    "alert_sym_overwrite",
    "alert_in_doc_not_fs",  # New function
    "consolidate_fields",  # Updated function name
    "get_conditions_actions",  # New function
    "remove_consolidated_columns",  # New function
    "print_debug_info"
]