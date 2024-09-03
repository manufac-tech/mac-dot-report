import logging
import pandas as pd
import numpy as np

from .dbase08_validate import replace_string_blanks

def merge_dataframes(main_df_dict, input_df_dict_section, merge_type='outer', verbose=True):
    # Extract the DataFrames from the dictionary sections
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict_section['dataframe']
    left_merge_field = main_df_dict['merge_field']
    right_merge_field = input_df_dict_section['merge_field']

    # Perform the merge operation without the indicator column
    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how=merge_type
        ).copy()
        
        # Debugging step: Check the DataFrame state before cleaning
        # if verbose:
        #     print("\nBefore cleaning:")
        #     print(merged_dataframe.isna().sum())

        # Apply the blank replacement after the merge
        merged_dataframe = replace_string_blanks(merged_dataframe)

        # Debugging step: Check the DataFrame state after cleaning
        # if verbose:
        #     print("\nAfter cleaning:")
        #     print(merged_dataframe.isna().sum())

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe