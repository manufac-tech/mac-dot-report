import os
import logging
import pandas as pd
import numpy as np

from .dbase06_load_hm import load_hm_dataframe
from .dbase09_load_di import load_di_dataframe
from .dbase16_validate import validate_df_dict_current_and_main
from .dbase17_merge import merge_dataframes


def add_and_populate_out_group(df):
    # Create 'out_group' column to indicate new, missing, or matched items in the report
    df['out_group'] = pd.Series(dtype='Int64')  # Create out_group column

    # Group 2: Matched items (based on di_match status)
    # df.loc[df['m_status_3'] == 'di_match', 'out_group'] = 1  

    # Group 1: Missing items (based on ERR:unmatched_di status)
    # df.loc[df['m_status_3'] == 'ERR:unmatched_di', 'out_group'] = 2  

    # Group 3: New items (based on ERR:home_only status)
    # df.loc[df['m_status_1'] == 'ERR:home_only', 'out_group'] = 3

    # Group 4: Catch-all for uncategorized items
    # df['out_group'].fillna(4, inplace=True)
    df['out_group'] = df['out_group'].fillna(4)

    return df

def apply_output_grouping(df):
    # Convert out_group to an integer for sorting
    df['out_group'] = df['out_group'].astype(int)

    # Sort each group individually, ensuring all groups are included
    group1 = df[df['out_group'] == 1].sort_values('original_order', ascending=True)  # Sort by original_order for group 1
    group2 = df[df['out_group'] == 2].sort_values('item_name', ascending=True)       # Sort by item_name for group 2
    group3 = df[df['out_group'] == 3].sort_values('item_name', ascending=True)       # Sort by item_name for group 3
    group4 = df[df['out_group'] == 4].sort_values('item_name', ascending=True)       # Sort by item_name for group 4 (catch-all)

    # Concatenate the sorted groups back together, including group 4
    df_sorted = pd.concat([group1, group2, group3, group4])

    # Reset index to maintain a continuous index if needed
    df_sorted = df_sorted.reset_index(drop=True)

    return df_sorted

def reorder_columns(df):
    reordered_columns = [
        # 'item_name', 'item_type',
        'unique_id', 'unique_id_hm', 'unique_id_rp', 'unique_id_db', 'unique_id_di',
        'dot_structure_di',
        'item_name_hm', 'item_type_hm',
        'item_name_rp', 'item_type_rp',
        'item_name_hm_db', 'item_name_rp_db', 'item_type_hm_db', 'item_type_rp_db',
        'item_name_hm_di', 'item_type_rp_di', 'cat_1_di', 'cat_1_name_di', 'comment_di', 'cat_2_di',
        'git_rp',
        'no_show_di',
        # 'm_status_1', 'm_status_2', 'm_status_3',
        'out_group',
        'original_order'
    ]
    reordered_columns = [col for col in reordered_columns if col in df.columns]
    return df[reordered_columns]

def sort_items_1_out_group(df):
    # # Assign output groups based on matching conditions
    # df.loc[df['_merge'] == 'left_only', 'out_group'] = 1  # Group 1: FS items not in dot_info
    # df.loc[df['_merge'] == 'both', 'out_group'] = 2       # Group 2: Matched items
    # df.loc[df['_merge'] == 'right_only', 'out_group'] = 3  # Group 3: dot_info items not in FS

    # # Convert out_group to an integer for sorting
    # df['out_group'] = df['out_group'].astype(int)

    # # Sort by out_group in ascending order
    # df.sort_values(by='out_group', ascending=True, inplace=True)

    return df

def sort_items_2_indiv(df):
    # # Group 1: Sort by item_name
    # group1 = df[df['out_group'] == 1].sort_values('item_name')

    # # Group 2: Sort by original_order
    # group2 = df[df['out_group'] == 2].sort_values('original_order')

    # # Group 3: Sort by item_name
    # group3 = df[df['out_group'] == 3].sort_values('item_name')

    # # Concatenate the sorted groups
    # df_sorted = pd.concat([group1, group2, group3])

    # # Reset index to maintain a continuous index if needed
    # df_sorted = df_sorted.reset_index(drop=True)

    return df_sorted