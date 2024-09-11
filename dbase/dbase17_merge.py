import logging
import pandas as pd
import numpy as np

from .dbase16_validate import replace_string_blanks

def merge_dataframes(main_df_dict, input_df_dict_section, merge_type='outer', verbose=False):
    # Extract the DataFrames from the dictionary sections
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict_section['dataframe']
    left_merge_field = main_df_dict['merge_field']
    right_merge_field = input_df_dict_section['merge_field']

    # Debugging step: Check the DataFrame state before merging
    if verbose:
        print(f"\nBefore merging with '{input_df_dict_section['suffix']}' DataFrame:")
        print(f"main_df['{left_merge_field}'] (all rows):\n{main_df[[left_merge_field]].to_string(index=False)}")
        print(f"input_df['{right_merge_field}'] (all rows):\n{input_df[[right_merge_field]].to_string(index=False)}")

    # Perform the merge operation
    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how=merge_type
        ).copy()

        # Debugging step: Check the DataFrame state after the merge
        if verbose:
            print(f"\nAfter merging with '{input_df_dict_section['suffix']}' DataFrame:")
            print(f"Merged DataFrame (all rows, {left_merge_field} and {right_merge_field}):")
            print(merged_dataframe[[left_merge_field, right_merge_field]].to_string(index=False))

        # Debugging step: Check the DataFrame columns before cleaning
        if verbose:
            print("\nMerged DataFrame Columns (before cleaning):")
            print(merged_dataframe.columns)

        # Apply the blank replacement after the merge
        merged_dataframe = replace_string_blanks(merged_dataframe)

        # Debugging step: Check the DataFrame columns after cleaning
        if verbose:
            print("\nMerged DataFrame Columns (after cleaning):")
            print(merged_dataframe.columns)

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe