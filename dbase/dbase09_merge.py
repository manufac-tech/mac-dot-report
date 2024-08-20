import logging
import pandas as pd
import numpy as np

def merge_dataframes(main_df_dict, input_df_dict_section, merge_type='outer', verbose=True):
    # Extract the DataFrames from the dictionary sections
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict_section['dataframe']
    merge_field = input_df_dict_section['merge_field']

    # Debug output to check columns before merge
    if verbose:
        print(f"Columns in main_df: {main_df.columns}")
        print(f"Columns in input_df: {input_df.columns}")
    
    # Perform the merge operation with error handling
    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=merge_field,
            right_on=merge_field,
            how=merge_type,
            indicator=True
        )
    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    if verbose:
        print(f"First 5 rows of Merged DataFrame after merge:\n{merged_dataframe.head()}")
    
    return merged_dataframe