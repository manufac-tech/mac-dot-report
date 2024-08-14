import logging
import pandas as pd
import numpy as np

def merge_dataframes(dot_items_df, template_df):
    """Merge the dot items DataFrame with the template DataFrame."""
    
    # Log the initial DataFrames
    # logging.debug("Initial dot_items_df:\n%s", dot_items_df.to_string())
    # logging.debug("Initial template_df:\n%s", template_df.to_string())

    # Merge the DataFrames with an indicator
    main_dataframe = pd.merge(
        dot_items_df, template_df,
        left_on=['fs_item_name', 'fs_item_type'],
        right_on=['tp_item_name', 'tp_item_type'],
        how='outer',
        indicator=True  # Add indicator to identify source of each row
    )

    # Log the merged DataFrame
    # logging.debug("Merged DataFrame:\n%s", main_dataframe.to_string())

    # Create a unified unique_id column
    main_dataframe['unique_id'] = main_dataframe.apply(merge_fields_unique_id, axis=1)

    # Log the merged DataFrame with unique_id
    # logging.debug("Merged DataFrame with unique_id:\n%s", main_dataframe[['fs_unique_id', 'tp_unique_id', 'unique_id', '_merge']].to_string())

    # Match and consolidate rows based on item names and types
    main_dataframe['item_name'] = main_dataframe.apply(merge_fields_name, axis=1)
    main_dataframe['item_type'] = main_dataframe.apply(merge_fields_type, axis=1)

    # Log final merged DataFrame
    # logging.debug("Final Merged DataFrame:\n%s", main_dataframe.to_string())

    return main_dataframe

def merge_fields_name(row):
    """Merge and unify item names from filesystem and template during the merge."""
    fs_name = row['fs_item_name'] if pd.notna(row['fs_item_name']) else ''
    tp_name = row['tp_item_name'] if pd.notna(row['tp_item_name']) else ''

    # logging.debug(f"Processing row: fs_name='{fs_name}', tp_name='{tp_name}', merge_type='{row['_merge']}'")

    if row['_merge'] == 'left_only':  # Only in filesystem
        return fs_name
    elif row['_merge'] == 'right_only':  # Only in template
        return tp_name
    elif fs_name:  # Prioritize filesystem name if both exist
        return fs_name
    elif tp_name:
        return tp_name
    else:
        return ''  # Default to empty string if both are missing

def merge_fields_type(row):
    """Merge and unify item types from filesystem and template during the merge."""
    if row['_merge'] == 'left_only':  # Only in filesystem
        return row['fs_item_type']
    elif row['_merge'] == 'right_only':  # Only in template
        return row['tp_item_type']
    elif pd.notna(row['fs_item_type']):
        return row['fs_item_type']  # Prioritize filesystem item type if both exist
    elif pd.notna(row['tp_item_type']):
        return row['tp_item_type']
    else:
        return np.nan  # Use NaN for unknown states

def merge_fields_unique_id(row):
    """Merge and unify unique IDs from filesystem and template during the merge."""
    fs_id = row['fs_unique_id'] if pd.notna(row['fs_unique_id']) else None
    tp_id = row['tp_unique_id'] if pd.notna(row['tp_unique_id']) else None

    # logging.debug(f"Processing row: fs_id='{fs_id}', tp_id='{tp_id}', merge_type='{row['_merge']}'")

    if row['_merge'] == 'left_only':  # Only in filesystem
        return fs_id
    elif row['_merge'] == 'right_only':  # Only in template
        return tp_id
    elif pd.notna(fs_id):  # Prioritize filesystem unique_id if both exist
        return fs_id
    elif pd.notna(tp_id):
        return tp_id
    else:
        return np.nan  # Use NaN for unknown states