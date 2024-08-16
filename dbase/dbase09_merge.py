import logging
import pandas as pd
import numpy as np

import pandas as pd

def merge_dataframes(df1, df2, suffix_mapping, merge_type='outer', verbose=False):
    """Merge two DataFrames using dynamic suffix mapping with error handling and additional functionality."""
    
    # Basic validation
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise ValueError("Both inputs must be pandas DataFrames.")
    if not isinstance(suffix_mapping, dict) or 'df1' not in suffix_mapping or 'df2' not in suffix_mapping:
        raise ValueError("suffix_mapping must be a dictionary with 'df1' and 'df2' keys.")
    
    suffix1, df1 = suffix_mapping['df1']
    suffix2, df2 = suffix_mapping['df2']
    
    if not isinstance(suffix1, str) or not isinstance(suffix2, str):
        raise ValueError("Suffixes must be strings.")
    
    required_columns = [f'{suffix1}_item_name', f'{suffix1}_item_type', f'{suffix2}_item_name', f'{suffix2}_item_type']
    for col in required_columns:
        if col not in df1.columns or col not in df2.columns:
            raise KeyError(f"Missing required column '{col}' in one of the DataFrames.")
    
    # Perform the merge operation with error handling
    try:
        main_dataframe = pd.merge(
            df1, df2,
            left_on=[f'{suffix1}_item_name', f'{suffix1}_item_type'],
            right_on=[f'{suffix2}_item_name', f'{suffix2}_item_type'],
            how=merge_type,
            indicator=True
        )
    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    # Create a unified unique_id column
    main_dataframe['unique_id'] = main_dataframe.apply(
        merge_fields_unique_id, axis=1, df1_prefix=suffix1, df2_prefix=suffix2
    )
    
    # Match and consolidate rows based on item names and types
    main_dataframe['item_name'] = main_dataframe.apply(
        merge_fields_name, axis=1, df1_prefix=suffix1, df2_prefix=suffix2
    )
    main_dataframe['item_type'] = main_dataframe.apply(
        merge_fields_type, axis=1, df1_prefix=suffix1, df2_prefix=suffix2
    )
    
    if verbose:
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