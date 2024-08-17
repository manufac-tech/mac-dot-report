import logging
import pandas as pd
import numpy as np

import pandas as pd

def merge_dataframes(df1, df2, suffix1, suffix2, merge_type='outer', verbose=False):
    """Merge two DataFrames using dynamic suffix mapping with error handling and additional functionality."""

    # Basic validation
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise ValueError("Both inputs must be pandas DataFrames.")
    
    # Perform the merge operation with error handling
    try:
        main_dataframe = pd.merge(
            df1, df2,
            left_on=['item_name', 'item_type'],  # Use original columns for merge
            right_on=['item_name', 'item_type'],
            how=merge_type,
            suffixes=(f'_{suffix1}', f'_{suffix2}'),  # Apply suffixes here
            indicator=True
        )
    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    # Debug: Print the suffixes before the merge
    # print(f"db🟪9️⃣ Suffix1: {suffix1}, Suffix2: {suffix2}")

    # Debug: Print the column names after the merge
    # print(f"db🟪9️⃣ Columns after merge:\n{main_dataframe.columns}")

    # Debug: Print the first few rows after the merge
    # print(f"db🟪9️⃣ DataFrame after merge:\n{main_dataframe.head()}")

    # Comment out the function calls
    # main_dataframe['item_name'] = main_dataframe.apply(
    #     merge_fields_name, axis=1, suffix1=suffix1, suffix2=suffix2
    # )
    # main_dataframe['item_type'] = main_dataframe.apply(
    #     merge_fields_type, axis=1, suffix1=suffix1, suffix2=suffix2
    # )
    
    # if verbose:
    #     print("Columns after creating item_name, item_type:", main_dataframe.columns)
    
    return main_dataframe

# def merge_fields_name(row, suffix1, suffix2):
#     """Merge and unify item names from two DataFrames during the merge."""
#     df1_name = row[f'item_name_{suffix1}'] if pd.notna(row[f'item_name_{suffix1}']) else ''
#     df2_name = row[f'item_name_{suffix2}'] if pd.notna(row[f'item_name_{suffix2}']) else ''

#     # Prioritize names: First DataFrame > Second DataFrame
#     if row['_merge'] == 'left_only':  # Only in the first DataFrame
#         return df1_name
#     elif row['_merge'] == 'right_only':  # Only in the second DataFrame
#         return df2_name
#     elif df1_name:  # Prioritize the first DataFrame name if both exist
#         return df1_name
#     elif df2_name:
#         return df2_name
#     else:
#         return ''  # Default to empty string if all are missing

# def merge_fields_type(row, suffix1, suffix2):
#     """Merge and unify item types from two DataFrames during the merge."""
#     df1_type = row[f'item_type_{suffix1}'] if pd.notna(row[f'item_type_{suffix1}']) else np.nan
#     df2_type = row[f'item_type_{suffix2}'] if pd.notna(row[f'item_type_{suffix2}']) else np.nan

#     if row['_merge'] == 'left_only':  # Only in the first DataFrame
#         return df1_type
#     elif row['_merge'] == 'right_only':  # Only in the second DataFrame
#         return df2_type
#     elif pd.notna(df1_type):  # Prioritize the first DataFrame item type if both exist
#         return df1_type
#     elif pd.notna(df2_type):
#         return df2_type
#     else:
#         return np.nan  # Use NaN for unknown states

# def merge_fields_unique_id(row, suffix1, suffix2):
#     """Merge and unify unique IDs from two DataFrames during the merge."""
#     df1_id = row[f'unique_id_{suffix1}'] if pd.notna(row[f'unique_id_{suffix1}']) else None
#     df2_id = row[f'unique_id_{suffix2}'] if pd.notna(row[f'unique_id_{suffix2}']) else None

#     if row['_merge'] == 'left_only':  # Only in the first DataFrame
#         return df1_id
#     elif row['_merge'] == 'right_only':  # Only in the second DataFrame
#         return df2_id
#     elif pd.notna(df1_id):  # Prioritize the first DataFrame unique_id if both exist
#         return df1_id
#     elif pd.notna(df2_id):
#         return df2_id
#     else:
#         return np.nan  # Use NaN for unknown states