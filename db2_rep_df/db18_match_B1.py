
import pandas as pd
import numpy as np

from .db21_make_df_r_sup import insert_blank_rows, reorder_dfr_cols_perm
from .db22_rpt_mg1_mast import field_match_master
from .db25_mrg_match import consolidate_fields
from .db26_status import detect_status_master
from .db29_resolve_config import resolve_fields_master
from .db40_term_disp import reorder_dfr_cols_for_cli

from db5_global.db52_dtype_dict import f_types_vals


def detect_full_domain_match(report_dataframe):
    print("DataFrame before processing:")
    print(report_dataframe)

    # Normalize missing values across all item_name_* columns
    item_name_columns = [
        'item_name_rp_db', 'item_name_rp_cf', 'item_name_rp',
        'item_name_hm_db', 'item_name_hm_cf', 'item_name_hm'
    ]
    for col in item_name_columns:
        # Convert to string and strip whitespace
        report_dataframe[col] = report_dataframe[col].astype(str).str.strip()
        # Replace common missing value representations with np.nan
        report_dataframe[col] = report_dataframe[col].replace(
            to_replace=['nan', '<NA>', 'NaN', 'None', 'NoneType', ''],
            value=np.nan
        )

    for index, row in report_dataframe.iterrows():
        try:
            # Extract item names for repo and home
            repo_names = [
                row['item_name_rp_cf'],  # CSV Config
                row['item_name_rp']      # File System
            ]
            home_names = [
                row['item_name_hm_cf'],  # CSV Config
                row['item_name_hm']      # File System
            ]

            # YAML config names (only relevant when item is in both Repo and Home)
            repo_name_db = row['item_name_rp_db']
            home_name_db = row['item_name_hm_db']

            # Function to check if all elements in a list are equal and non-missing
            def all_non_missing_and_equal(lst):
                # Remove NaNs
                lst_filtered = [x for x in lst if pd.notna(x)]
                # If list is empty after removing NaNs, return None
                if not lst_filtered:
                    return None
                # Check if all non-NaN values are equal
                if all(x == lst_filtered[0] for x in lst_filtered):
                    return lst_filtered[0]  # Return the consistent name
                else:
                    return None  # Names are inconsistent within the domain

            # Get consistent names for repo and home
            repo_name = all_non_missing_and_equal(repo_names)
            home_name = all_non_missing_and_equal(home_names)

            # Initialize debug status
            debug_status = ""

            # Apply matching logic based on updated requirements
            if repo_name and home_name:
                # Both Repo and Home have consistent names
                # Now check if YAML config entries exist and are consistent
                if pd.notna(repo_name_db) and pd.notna(home_name_db):
                    # Check if YAML config names are consistent and match Repo and Home names
                    if repo_name_db == home_name_db == repo_name == home_name:
                        dot_struc_value = 'rp>hm'
                        debug_status = 'both_full_match'
                    else:
                        dot_struc_value = None  # Names inconsistent in YAML config
                        debug_status = 'names_mismatch_in_yaml'
                else:
                    dot_struc_value = None  # YAML entries missing
                    debug_status = 'yaml_entries_missing_for_both'
            elif repo_name and not home_name:
                # Only Repo has consistent names
                # YAML config entries should be missing
                if pd.isna(repo_name_db) and pd.isna(home_name_db):
                    dot_struc_value = 'rp'
                    debug_status = 'repo_only_full_match'
                else:
                    dot_struc_value = None  # Unexpected YAML entries
                    debug_status = 'unexpected_yaml_entries_for_repo_only'
            elif home_name and not repo_name:
                # Only Home has consistent names
                # YAML config entries should be missing
                if pd.isna(repo_name_db) and pd.isna(home_name_db):
                    dot_struc_value = 'hm'
                    debug_status = 'home_only_full_match'
                else:
                    dot_struc_value = None  # Unexpected YAML entries
                    debug_status = 'unexpected_yaml_entries_for_home_only'
            else:
                # Neither Repo nor Home have consistent names or are missing
                dot_struc_value = None
                debug_status = 'no_full_match'

            # Assign the dot_struc value if applicable
            if dot_struc_value is not None:
                report_dataframe.at[index, 'dot_struc'] = dot_struc_value

            # Update the st_misc field with the debug status
            report_dataframe.at[index, 'st_misc'] = debug_status

            # Additional debug information
            print(f"Index {index} - repo_name: {repo_name}, home_name: {home_name}, "
                  f"repo_name_db: {repo_name_db}, home_name_db: {home_name_db}, "
                  f"dot_struc: {dot_struc_value}, st_misc: {debug_status}")

        except Exception as e:
            print(f"Error processing index {index}: {e}")

    print("DataFrame after processing:")
    print(report_dataframe)

    return report_dataframe

