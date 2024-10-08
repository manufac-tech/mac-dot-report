import pandas as pd
import numpy as np
from db5_global.db52_dtype_dict import f_types_vals

from .db21_make_df_r_sup import insert_blank_rows, reorder_dfr_cols_perm
from .db22_rpt_mg1_mast import field_match_master
from .db25_mrg_match import consolidate_fields
from .db26_status import detect_status_master
from .db29_resolve_config import resolve_fields_master
from .db40_term_disp import reorder_dfr_cols_for_cli

# Opt-in to the future behavior for downcasting
pd.set_option('future.no_silent_downcasting', True)

def build_report_dataframe(main_df_dict):
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    report_dataframe = add_report_fields(report_dataframe)

    # Apply field matching and consolidation
    # report_dataframe, field_merge_rules = field_match_master(report_dataframe)
    # report_dataframe = consolidate_fields(report_dataframe, field_merge_rules).copy()

    report_dataframe = sort_filter_report_df(report_dataframe, unhide_hidden=False)
    report_dataframe = insert_blank_rows(report_dataframe)
    report_dataframe = reorder_dfr_cols_perm(report_dataframe)

    # Apply the detect_status_master function with the status_checks_config
    report_dataframe = detect_status_master(report_dataframe) 
    # report_dataframe = resolve_fields_master(report_dataframe)
    report_dataframe = post_build_nan_replace(report_dataframe)
    
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