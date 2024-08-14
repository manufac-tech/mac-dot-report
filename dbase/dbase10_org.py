import os
import logging
import pandas as pd
import numpy as np

from .dbase04_load_fs import load_fs_dataframe
from .dbase05_load_tp import load_tp_dataframe
from .dbase08_validate import validate_dataframes
from .dbase09_merge import merge_dataframes


def add_and_populate_out_group(df):
    # Create 'out_group' column to indicate new, missing, or matched items in the report.
    df['out_group'] = pd.Series(dtype='Int64')  # Create out_group column
    df['out_group'] = df['_merge'].map({
        'left_only': 1,  # Unmatched in filesystem
        'both': 2,       # Matched
        'right_only': 3  # Unmatched in template
    })
    return df

def apply_output_grouping(df):
    # Assign output groups based on matching conditions
    df.loc[df['_merge'] == 'left_only', 'out_group'] = 1  # Group 1: FS items not in template
    df.loc[df['_merge'] == 'both', 'out_group'] = 2       # Group 2: Matched items
    df.loc[df['_merge'] == 'right_only', 'out_group'] = 3  # Group 3: Template items not in FS

    # Convert out_group to an integer for sorting
    df['out_group'] = df['out_group'].astype(int)

    # Sort by out_group first, then apply secondary sorting
    df.sort_values(by=['out_group', 'item_name', 'original_order'], 
                   ascending=[True, True, True], 
                   inplace=True)

    return df

def reorder_columns(df):
    reordered_columns = [
        'item_name', 'item_type', 'unique_id',
        'fs_item_name', 'fs_item_type',
        'tp_item_name', 'tp_item_type',
        'tp_cat_1', 'tp_cat_1_name', 'tp_comment', 'tp_cat_2', 
        'no_show', 'original_order', '_merge'
    ]
    reordered_columns = [col for col in reordered_columns if col in df.columns]
    return df[reordered_columns]

def sort_items_1_out_group(df):
    # Assign output groups based on matching conditions
    df.loc[df['_merge'] == 'left_only', 'out_group'] = 1  # Group 1: FS items not in template
    df.loc[df['_merge'] == 'both', 'out_group'] = 2       # Group 2: Matched items
    df.loc[df['_merge'] == 'right_only', 'out_group'] = 3  # Group 3: Template items not in FS

    # Convert out_group to an integer for sorting
    df['out_group'] = df['out_group'].astype(int)

    # Sort by out_group in ascending order
    df.sort_values(by='out_group', ascending=True, inplace=True)

    return df

def sort_items_2_indiv(df):
    # Group 1: Sort by item_name
    group1 = df[df['out_group'] == 1].sort_values('item_name')

    # Group 2: Sort by original_order
    group2 = df[df['out_group'] == 2].sort_values('original_order')

    # Group 3: Sort by item_name
    group3 = df[df['out_group'] == 3].sort_values('item_name')

    # Concatenate the sorted groups
    df_sorted = pd.concat([group1, group2, group3])

    # Reset index to maintain a continuous index if needed
    df_sorted = df_sorted.reset_index(drop=True)

    return df_sorted