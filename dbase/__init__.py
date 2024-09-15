from .dbase01_setup import (
    build_full_output_dict
)

from .dbase02_main_df import (
    build_main_dataframe,
    merge_dataframes,  # Moved from dbase17_merge
    # create_input_df_dict,  # Moved from dbase03_init
)

# from .dbase03_init import (
#     # initialize_main_dataframe,
# )

from .dbase04_id_gen import (
    get_next_unique_id,
    field_merge_1_uid,
    field_merge_2_uid,
    field_merge_3_uid
)
from .dbase05_item_type import (
    determine_item_type,
    is_symlink,
    is_alias,
    get_file_type,
    get_folder_type,
    resolve_item_type,
    detect_alias_type,
    detect_symlink_target_type
)
from .dbase06_load_hm import load_hm_dataframe
from .dbase07_load_rp import (
    load_rp_dataframe,
    create_git_rp_column,
    read_gitignore_items
)
from .dbase08_load_db import load_dotbot_yaml_dataframe
from .dbase09_load_di import (
    correct_and_validate_dot_info_df,
    replace_string_blanks,
    load_di_dataframe
)
from .dbase16_validate import (
    validate_df_dict_current_and_main,
    validate_values, # MAYBE REMOVE
    # replace_string_blanks
)
from .dbase18_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns_main,
    sort_items_1_out_group, # MAYBE REMOVE
    sort_items_2_indiv # MAYBE REMOVE
)
from .dbase21_rep_df import (
    build_report_dataframe,
    handle_nan_values,
    filter_no_show_rows
)
from .dbase26_f_mg1 import (
    # compare_docs_di_and_db,
    # compare_fs_rp_and_hm,
    # field_merge_main,
    # calc_final_merge_status,
    perform_full_matching,
    dot_structure_status,
    subsystem_docs,
    subsystem_db_all,
    alert_sym_overwrite
)

from .dbase30_debug import print_debug_info

__all__ = [
    "build_full_output_dict",
    "build_main_dataframe",
    "merge_dataframes",  # Moved from dbase17_merge
    "create_input_df_dict",  # Moved from dbase03_init
    # "initialize_main_dataframe",
    "get_next_unique_id",
    "field_merge_1_uid",
    "field_merge_2_uid",
    "field_merge_3_uid",
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
    "replace_string_blanks",
    "load_di_dataframe",
    "validate_df_dict_current_and_main",
    "validate_values",  # MAYBE REMOVE
    # "replace_string_blanks",  # This is commented out in the imports
    "add_and_populate_out_group",
    "apply_output_grouping",
    "reorder_columns_main",
    "sort_items_1_out_group",  # MAYBE REMOVE
    "sort_items_2_indiv",  # MAYBE REMOVE
    "build_report_dataframe",
    "handle_nan_values",
    "filter_no_show_rows",
    # "compare_docs_di_and_db",
    # "compare_fs_rp_and_hm",
    "perform_full_matching",
    "dot_structure_status",
    "subsystem_docs",
    "subsystem_db_all",
    "alert_sym_overwrite",
    "print_debug_info"
]