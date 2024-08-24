import logging
import pandas as pd
import numpy as np

def merge_dataframes(main_df_dict, input_df_dict_section, merge_type='outer', verbose=False):
    # Extract the DataFrames from the dictionary sections
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict_section['dataframe']
    left_merge_field = main_df_dict['merge_field']
    right_merge_field = input_df_dict_section['merge_field']

    # print("Main DataFrame:", main_df)
    # print("Input DataFrame:", input_df)

    # Debug output to check columns before merge
    # if verbose:
        # print(f"Columns in main_df: {main_df.columns}")
        # print(f"Columns in input_df: {input_df.columns}")
    
    # print(f"Left Merge Field: {left_merge_field}")
    # print(f"Right Merge Field: {right_merge_field}")

    # print(f"Columns in Main DataFrame: {main_df.columns}")
    # print(f"Columns in Input DataFrame: {input_df.columns}")



    # Perform the merge operation with error handling
    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how=merge_type,
            indicator=True
        )
        
        # Rename the right merge field to retain it with a suffix
        merged_dataframe = merged_dataframe.rename(columns={right_merge_field: f"{right_merge_field}_suffix"})

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")

    # print(f"Merged DataFrame Columns: {merged_dataframe.columns}")
    # print(f"First 5 rows of Merged DataFrame:\n{merged_dataframe.head()}")

    # if verbose:
        # print(f"First 5 rows of Merged DataFrame after merge:\n{merged_dataframe.head()}")

    return merged_dataframe