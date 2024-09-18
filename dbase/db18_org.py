import os
import logging
import pandas as pd
import numpy as np

from .db07_load_hm import load_hm_dataframe
from .db09_load_di import load_di_dataframe


def apply_output_grouping(df):
    # Sort the entire DataFrame by 'sort_orig'
    df_sorted = df.sort_values('sort_orig', ascending=True)
    df_sorted = df_sorted.reset_index(drop=True)
    return df_sorted

def reorder_dfm_cols_perm(df):
    # Define the desired column order based on the provided fields
    desired_order = [
        'item_name', 'item_type', 'unique_id',
        'item_name_rp', 'item_type_rp', 'git_rp', 'item_name_hm', 'item_type_hm',
        'item_name_hm_db', 'item_type_hm_db', 'item_name_rp_db', 'item_type_rp_db',
        'item_name_rp_di', 'item_type_rp_di', 'item_name_hm_di', 'item_type_hm_di',
        'dot_struc_di', 'cat_1_di', 'cat_1_name_di', 'cat_2_di', 'comment_di', 'no_show_di',
        'sort_orig',
        'unique_id_rp', 'unique_id_db', 'unique_id_hm', 'unique_id_di'
    ]
    
    # Ensure all columns in desired_order are in the DataFrame
    for col in desired_order:
        if col not in df.columns:
            print(f"Warning: Column {col} not found in DataFrame")
    
    # Reorder columns
    df = df[desired_order]
    
    return df

def reorder_dfr_cols_perm(df):
    # Define the desired column order based on the provided fields
    desired_order = [
        'st_alert', 'item_name_home', 'item_type_home', 'item_name_repo', 'item_type_repo', 'git_rp', 'cat_1_di', 'cat_1_name_di', 'cat_2_di', 'comment_di',
        'dot_struc_di',
        'dot_struc', 'st_db_all', 'st_docs', 'st_misc',
        'sort_orig', 'sort_out',
        'no_show_di', 'unique_id'
    ]
    
    # Ensure all columns in desired_order are in the DataFrame
    for col in desired_order:
        if col not in df.columns:
            print(f"Warning: Column {col} not found in DataFrame")
    
    # Reorder columns
    df = df[desired_order]
    
    return df

def reorder_dfr_cols_for_cli(report_dataframe, show_all_fields, show_final_output, show_field_merge, show_field_merge_dicts):
    # PROVIDES DATAFRAME FIELD _GROUPS_ TO DISPLAY IN CLI W/O EXCEEDING WIDTH
    
    # All Fields Group
    all_fields_columns = report_dataframe.columns.tolist()

    # Final Output Group
    final_output_columns = [
        # 'unique_id',
        'st_alert',
        'item_name_repo', 'item_type_repo', 'item_name_home', 'item_type_home', 
        'git_rp', 'cat_1_di', 
        # 'cat_1_name_di', 
        'cat_2_di',
        'comment_di',
        'dot_struc_di',
        'dot_struc', 'st_db_all', 'st_docs',
        'st_misc',
        'sort_orig', 'sort_out'
    ]

    # Field Merge Group
    field_merge_columns = [
        'st_misc', 'st_alert', 'st_db_all', 'st_docs', 'dot_struc',
        'sort_out', 'sort_orig'
    ]

    # Display All Fields Section
    if show_all_fields:
        print("All Fields Group:")
        print(report_dataframe[all_fields_columns])
        print("\n" * 2)

    # Display Final Output Section
    if show_final_output:
        print("Final Output Group:")
        print(report_dataframe[final_output_columns])
        print("\n" * 2)

    # Display Field Merge Section
    if show_field_merge:
        print("Field Merge Group:")
        print(report_dataframe[field_merge_columns])
        print("\n" * 2)

    # Return reordered DataFrame if needed
    return report_dataframe