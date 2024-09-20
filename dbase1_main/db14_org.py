import os
import logging
import pandas as pd
import numpy as np

from dbase2_load.db07_load_hm import load_hm_dataframe
from dbase2_load.db09_load_di import load_di_dataframe


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
        'st_alert', 'item_name_home', 'item_type_home', 'item_name_repo', 'item_type_repo', 'git_rp', 
        # 'secondary_sort_key', 'tertiary_sort_key',
        'cat_1_di', 'cat_1_name_di', 'cat_2_di', 'comment_di',
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
        'git_rp',
        # 'secondary_sort_key', 'tertiary_sort_key',
        'dot_struc_di',
        'dot_struc', 'st_db_all', 'st_docs',
        'cat_1_di', 'cat_2_di',
        # 'comment_di',
        # 'st_misc',
        # 'sort_orig', 'sort_out'
    ]

    # Field Merge Group
    field_merge_columns = [
        'st_misc', 'st_alert', 'st_db_all', 'st_docs', 'dot_struc',
        'sort_out', 'sort_orig'
    ]

    # Display All Fields Section
    if show_all_fields:
        print_dataframe_section(report_dataframe, all_fields_columns, "All Fields Group")

    # Display Final Output Section
    if show_final_output:
        print_dataframe_section(report_dataframe, final_output_columns, "Final Output Group")

    # Display Field Merge Section
    if show_field_merge:
        print_dataframe_section(report_dataframe, field_merge_columns, "Field Merge Group")

    # Return reordered DataFrame if needed
    return report_dataframe

def print_dataframe_section(df, columns, title):
    print(f"{title}:")
    print(df[columns])
    print("\n" * 2)

    # print(f"{title} (Data Types):")
    # data_types_df = df[columns].apply(lambda col: col.apply(lambda x: type(x).__name__))
    # print(data_types_df)
    # print("\n" * 2)