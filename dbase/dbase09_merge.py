import logging
import pandas as pd
import numpy as np

def merge_dataframes(df1, df2, df2_field_suffix, merge_type='outer', verbose=False, match_on_item_type=False):
    """Merge two DataFrames using dynamic suffix mapping with error handling and additional functionality."""

    # Basic validation
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise ValueError("Both inputs must be pandas DataFrames.")
    
    # Determine merge keys based on whether item_type should be included
    if match_on_item_type:
        merge_keys = ['item_name', 'item_type']
    else:
        merge_keys = ['item_name']
    
    # Debug: Print the first few rows of both DataFrames before the merge
    # print(f"db🟪9️⃣ First 5 rows of df1 before merge:\n{df1.head()}")
    # print(f"db🟪9️⃣ First 5 rows of df2 before merge:\n{df2.head()}")

    # Perform the merge operation with error handling
    try:
        main_dataframe = pd.merge(
            df1, df2,
            left_on=merge_keys,  # Use determined merge keys
            right_on=merge_keys,
            how=merge_type,
            suffixes=('', f'_{df2_field_suffix}'),  # Apply suffix only to the right-hand DataFrame
            indicator=True
        )
    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    # Debug: Print the first few rows of the merged DataFrame
    print(f"db🟪9️⃣ First 5 rows of Merged DataFrame after merge:\n{main_dataframe.head()}")
    
    return main_dataframe