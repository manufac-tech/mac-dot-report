import pandas as pd
import numpy as np

from .db18_match_B1 import detect_full_domain_match
from .db21_make_df_r_sup import insert_blank_rows, reorder_dfr_cols_perm
from .db22_rpt_mg1_mast import field_match_master
from .db25_mrg_match import consolidate_fields
from .db26_status import detect_status_master
from .db29_resolve_config import resolve_fields_master
from .db40_term_disp import reorder_dfr_cols_for_cli

from db5_global.db52_dtype_dict import f_types_vals

# Opt-in to the future behavior for downcasting
pd.set_option('future.no_silent_downcasting', True)

def build_report_dataframe(main_df_dict):
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    report_dataframe = add_report_fields(report_dataframe)

    report_dataframe = sort_filter_report_df(report_dataframe, unhide_hidden=False)
    # report_dataframe = insert_blank_rows(report_dataframe)
    # report_dataframe = reorder_dfr_cols_perm(report_dataframe)

    # Apply the detect_status_master function with the status_checks_config
    # report_dataframe = detect_status_master(report_dataframe) 
    # report_dataframe = resolve_fields_master(report_dataframe)
    # report_dataframe = post_build_nan_replace(report_dataframe)

    # Apply the new repo only detection function
    report_dataframe = detect_full_domain_match(report_dataframe)
    
    # Print the result to the console
    # print("🟪 DEBUG: Full Report DataFrame")
    # print(report_dataframe[['item_name_repo', 'item_type_repo', 'item_name_home', 'item_type_home', 'm_consol_dict']])
    
    report_dataframe = reorder_dfr_cols_for_cli( # Reorder columns for CLI display
        report_dataframe,
        show_all_fields=False,
        show_main_fields=False,
        show_status_fields=False,
        show_setup_group=True,
    )

    return report_dataframe



# def detect_full_domain_match(report_dataframe):
#     print("DataFrame before processing:")
#     print(report_dataframe)

#     # Normalize missing values across all item_name_* columns
#     item_name_columns = [
#         'item_name_rp_db', 'item_name_rp_cf', 'item_name_rp',
#         'item_name_hm_db', 'item_name_hm_cf', 'item_name_hm'
#     ]
#     for col in item_name_columns:
#         # Convert to string and strip whitespace
#         report_dataframe[col] = report_dataframe[col].astype(str).str.strip()
#         # Replace common missing value representations with np.nan
#         report_dataframe[col] = report_dataframe[col].replace(
#             to_replace=['nan', '<NA>', 'NaN', 'None', 'NoneType', ''],
#             value=np.nan
#         )

#     for index, row in report_dataframe.iterrows():
#         try:
#             # Extract item names for repo and home
#             repo_names = [
#                 row['item_name_rp_cf'],  # CSV Config
#                 row['item_name_rp']      # File System
#             ]
#             home_names = [
#                 row['item_name_hm_cf'],  # CSV Config
#                 row['item_name_hm']      # File System
#             ]

#             # YAML config names (only relevant when item is in both Repo and Home)
#             repo_name_db = row['item_name_rp_db']
#             home_name_db = row['item_name_hm_db']

#             # Function to check if all elements in a list are equal and non-missing
#             def all_non_missing_and_equal(lst):
#                 # Remove NaNs
#                 lst_filtered = [x for x in lst if pd.notna(x)]
#                 # If list is empty after removing NaNs, return None
#                 if not lst_filtered:
#                     return None
#                 # Check if all non-NaN values are equal
#                 if all(x == lst_filtered[0] for x in lst_filtered):
#                     return lst_filtered[0]  # Return the consistent name
#                 else:
#                     return None  # Names are inconsistent within the domain

#             # Get consistent names for repo and home
#             repo_name = all_non_missing_and_equal(repo_names)
#             home_name = all_non_missing_and_equal(home_names)

#             # Initialize debug status
#             debug_status = ""

#             # Apply matching logic based on updated requirements
#             if repo_name and home_name:
#                 # Both Repo and Home have consistent names
#                 # Now check if YAML config entries exist and are consistent
#                 if pd.notna(repo_name_db) and pd.notna(home_name_db):
#                     # Check if YAML config names are consistent and match Repo and Home names
#                     if repo_name_db == home_name_db == repo_name == home_name:
#                         dot_struc_value = 'rp>hm'
#                         debug_status = 'both_full_match'
#                     else:
#                         dot_struc_value = None  # Names inconsistent in YAML config
#                         debug_status = 'names_mismatch_in_yaml'
#                 else:
#                     dot_struc_value = None  # YAML entries missing
#                     debug_status = 'yaml_entries_missing_for_both'
#             elif repo_name and not home_name:
#                 # Only Repo has consistent names
#                 # YAML config entries should be missing
#                 if pd.isna(repo_name_db) and pd.isna(home_name_db):
#                     dot_struc_value = 'rp'
#                     debug_status = 'repo_only_full_match'
#                 else:
#                     dot_struc_value = None  # Unexpected YAML entries
#                     debug_status = 'unexpected_yaml_entries_for_repo_only'
#             elif home_name and not repo_name:
#                 # Only Home has consistent names
#                 # YAML config entries should be missing
#                 if pd.isna(repo_name_db) and pd.isna(home_name_db):
#                     dot_struc_value = 'hm'
#                     debug_status = 'home_only_full_match'
#                 else:
#                     dot_struc_value = None  # Unexpected YAML entries
#                     debug_status = 'unexpected_yaml_entries_for_home_only'
#             else:
#                 # Neither Repo nor Home have consistent names or are missing
#                 dot_struc_value = None
#                 debug_status = 'no_full_match'

#             # Assign the dot_struc value if applicable
#             if dot_struc_value is not None:
#                 report_dataframe.at[index, 'dot_struc'] = dot_struc_value

#             # Update the st_misc field with the debug status
#             report_dataframe.at[index, 'st_misc'] = debug_status

#             # Additional debug information
#             print(f"Index {index} - repo_name: {repo_name}, home_name: {home_name}, "
#                   f"repo_name_db: {repo_name_db}, home_name_db: {home_name_db}, "
#                   f"dot_struc: {dot_struc_value}, st_misc: {debug_status}")

#         except Exception as e:
#             print(f"Error processing index {index}: {e}")

#     print("DataFrame after processing:")
#     print(report_dataframe)

#     return report_dataframe



def add_report_fields(report_dataframe):
    df = report_dataframe
    new_columns = { # Define the new columns to add
        'item_name_repo': f_types_vals['item_name_repo'],
        'item_type_repo': f_types_vals['item_type_repo'],
        'item_name_home': f_types_vals['item_name_home'],
        'item_type_home': f_types_vals['item_type_home'],
        'sort_out': f_types_vals['sort_out'],
        'st_docs': f_types_vals['st_docs'],
        'st_alert': f_types_vals['st_alert'],
        'dot_struc': f_types_vals['dot_struc'],
        'st_db_all': f_types_vals['st_db_all'],
        'st_misc': f_types_vals['st_misc'],
        'm_status_dict': f_types_vals['m_status_dict'],
        'm_consol_dict': f_types_vals['m_consol_dict'],
        'm_status_result': f_types_vals['m_status_result'],
        'm_consol_result': f_types_vals['m_consol_result'],
    }
    
    for column, properties in new_columns.items(): # Create the new columns ( + types & vals)
        dtype = properties['dtype']
        default_value = properties['default']
        df[column] = pd.Series([default_value] * len(df), dtype=dtype)
    
    df['sort_out'] = df['sort_out'].fillna(-1) # Initialize 'sort_out' column with -1

    return df



def post_build_nan_replace(df): # Replace NaN vals
    # Replace NaN values in string columns with empty strings
    string_columns = df.select_dtypes(include=['object', 'string']).columns # ...with ""
    df[string_columns] = df[string_columns].fillna('').infer_objects(copy=False)

    numeric_columns = df.select_dtypes(include=['number']).columns # ...with 0
    df[numeric_columns] = df[numeric_columns].fillna(0)

    boolean_columns = df.select_dtypes(include=['bool']).columns # ...with False
    df[boolean_columns] = df[boolean_columns].fillna(False)

    nullable_int_columns = df.select_dtypes(include=['Int64', 'Int32', 'Int16']).columns # ...with 0
    df[nullable_int_columns] = df[nullable_int_columns].fillna(0)

    nullable_bool_columns = df.select_dtypes(include=['boolean']).columns # ...with False
    df[nullable_bool_columns] = df[nullable_bool_columns].fillna(False)

    df = df.fillna('') # ...with ""

    return df

def sort_filter_report_df(df, unhide_hidden):
    # df = df[df['no_show_cf'] == False].copy()  # Filter out rows where 'no_show_cf' is set to True
    if unhide_hidden:
        df['secondary_sort_key'] = df['git_rp'].apply(lambda x: 1 if x == False else 0)

    df['secondary_sort_key'] = df['git_rp'].apply(lambda x: 1 if x == False else 0) # TEMP NEW COL for sort 2 (git)

    df['tertiary_sort_key'] = df['sort_orig'] # The tertiary sort key is the original sort order
    
    # Sort the DataFrame by 'sort_out', 'secondary_sort_key', and 'tertiary_sort_key'
    df = df.sort_values(by=['sort_out', 'secondary_sort_key', 'tertiary_sort_key'], ascending=[True, True, True])
    
    df = df.drop(columns=['secondary_sort_key', 'tertiary_sort_key']) # Drop the Git sort TEMP NEW COL    
    return df