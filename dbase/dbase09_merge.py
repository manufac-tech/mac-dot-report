import logging
import pandas as pd
import numpy as np

def merge_dataframes(home_items_df, repo_items_df):
    """Merge the home items DataFrame with the repo items DataFrame."""
    
    main_dataframe = pd.merge(
        home_items_df, repo_items_df,
        left_on=['fs_item_name', 'fs_item_type'],
        right_on=['rp_item_name', 'rp_item_type'],
        how='outer',
        indicator=True
    )

    # Create a unified unique_id column
    main_dataframe['unique_id'] = main_dataframe.apply(merge_fields_unique_id, axis=1, df1_prefix='fs', df2_prefix='rp')

    # Match and consolidate rows based on item names and types
    main_dataframe['item_name'] = main_dataframe.apply(merge_fields_name, axis=1, df1_prefix='fs', df2_prefix='rp')
    main_dataframe['item_type'] = main_dataframe.apply(merge_fields_type, axis=1, df1_prefix='fs', df2_prefix='rp')

    # Debug: Print columns after creating item_name, item_type, and unique_id
    print("Columns after creating item_name, item_type, and unique_id:", main_dataframe.columns)

    return main_dataframe

def merge_fields_name(row, df1_prefix, df2_prefix):
    """Merge and unify item names from two DataFrames during the merge."""
    df1_name = row[f'{df1_prefix}_item_name'] if pd.notna(row[f'{df1_prefix}_item_name']) else ''
    df2_name = row[f'{df2_prefix}_item_name'] if pd.notna(row[f'{df2_prefix}_item_name']) else ''

    # Prioritize names: First DataFrame > Second DataFrame
    if row['_merge'] == 'left_only':  # Only in the first DataFrame
        return df1_name
    elif row['_merge'] == 'right_only':  # Only in the second DataFrame
        return df2_name
    elif df1_name:  # Prioritize the first DataFrame name if both exist
        return df1_name
    elif df2_name:
        return df2_name
    else:
        return ''  # Default to empty string if all are missing

def merge_fields_type(row, df1_prefix, df2_prefix):
    """Merge and unify item types from two DataFrames during the merge."""
    df1_type = row[f'{df1_prefix}_item_type'] if pd.notna(row[f'{df1_prefix}_item_type']) else np.nan
    df2_type = row[f'{df2_prefix}_item_type'] if pd.notna(row[f'{df2_prefix}_item_type']) else np.nan

    if row['_merge'] == 'left_only':  # Only in the first DataFrame
        return df1_type
    elif row['_merge'] == 'right_only':  # Only in the second DataFrame
        return df2_type
    elif pd.notna(df1_type):  # Prioritize the first DataFrame item type if both exist
        return df1_type
    elif pd.notna(df2_type):
        return df2_type
    else:
        return np.nan  # Use NaN for unknown states

def merge_fields_unique_id(row, df1_prefix, df2_prefix):
    """Merge and unify unique IDs from two DataFrames during the merge."""
    df1_id = row[f'{df1_prefix}_unique_id'] if pd.notna(row[f'{df1_prefix}_unique_id']) else None
    df2_id = row[f'{df2_prefix}_unique_id'] if pd.notna(row[f'{df2_prefix}_unique_id']) else None

    if row['_merge'] == 'left_only':  # Only in the first DataFrame
        return df1_id
    elif row['_merge'] == 'right_only':  # Only in the second DataFrame
        return df2_id
    elif pd.notna(df1_id):  # Prioritize the first DataFrame unique_id if both exist
        return df1_id
    elif pd.notna(df2_id):
        return df2_id
    else:
        return np.nan  # Use NaN for unknown states