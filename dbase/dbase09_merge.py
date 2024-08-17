import logging
import pandas as pd
import numpy as np

def add_suffix_to_df2_fields(df2, df2_field_suffix):
    """Add the provided suffix to all columns in df2 except for 'item_name'."""
    df2_renamed = df2.rename(columns={col: f"{col}_{df2_field_suffix}" for col in df2.columns if col != 'item_name'})
    return df2_renamed

def merge_dataframes(df1, df2, df2_field_suffix, merge_type='outer', verbose=False):
    """Merge two DataFrames using dynamic suffix mapping with error handling and additional functionality."""

    # Basic validation
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise ValueError("Both inputs must be pandas DataFrames.")
    
    # Rename df2 columns with the suffix, except for 'item_name'
    df2 = add_suffix_to_df2_fields(df2, df2_field_suffix)
    
    # Debug output to check columns before merge
    if verbose:
        print(f"Columns in df1: {df1.columns}")
        print(f"Columns in df2: {df2.columns}")
    
    # Define the merge key
    merge_keys = ['item_name']
    
    # Perform the merge operation with error handling
    try:
        main_dataframe = pd.merge(
            df1, df2,
            left_on=merge_keys,
            right_on=merge_keys,
            how=merge_type,
            indicator=True
        )
    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    if verbose:
        print(f"First 5 rows of Merged DataFrame after merge:\n{main_dataframe.head()}")
    
    return main_dataframe