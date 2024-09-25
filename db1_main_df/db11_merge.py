import pandas as pd
import logging

from db2_global.db03_dtype_dict import f_types_vals
from .db12_merge_sup import consolidate_post_merge1, consolidate_post_merge3, print_main_df_build_hist


# Add the unique ID generation code
current_unique_id = 1

def get_next_unique_id():
    global current_unique_id
    unique_id = current_unique_id
    current_unique_id += 1
    return unique_id

def df_merge_sequence(main_df, home_df, dotbot_df, dot_info_df, print_df):
    left_merge_field = 'item_name' # Only declared once; it remains the "left input" for all merges

    # First merge: repo and home
    right_merge_field = 'item_name_hm'
    main_df = df_merge(main_df, home_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge1(main_df)

    # Second merge: repo+home and dotbot R+H
    right_merge_field = 'item_name_rp_db'
    main_df = df_merge(main_df, dotbot_df, left_merge_field, right_merge_field)  # Merge the DataFrames

    # Third merge: repo+home+dotbot and dot_info R+H
    right_merge_field = 'item_name_rp_di'
    main_df = df_merge(main_df, dot_info_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge3(main_df)

    return main_df

def df_merge(main_df, input_df, left_merge_field, right_merge_field, merge_type='outer'):
    main_df_build_hist = {"df1": main_df.copy(), "df2": input_df.copy()}

    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how=merge_type
        ).copy()

        # merged_dataframe = replace_string_blanks(merged_dataframe) # ⭕️ BLANK HANDLING 

        main_df_build_hist["df3"] = merged_dataframe.copy()
        # print_main_df_build_hist(main_df_build_hist) # Print the build history 🟡

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe


def replace_string_blanks(df): # ⭕️ BLANK HANDLING 
    # pass
    for column in df.columns:
        # Handle string columns
        if pd.api.types.is_string_dtype(df[column]):
            df[column] = df[column].astype(str)
            # Replace variations of NA, including case-insensitive matches
            df[column] = df[column].str.replace(r'(?i)^<na>$', '', regex=True)
            df[column] = df[column].str.replace(r'(?i)^nan$', '', regex=True)
            df[column] = df[column].str.replace(r'(?i)^none$', '', regex=True)
            # Fill remaining NaN values with empty string
            df[column] = df[column].fillna('')

        # Handle Int64 columns
        elif pd.api.types.is_integer_dtype(df[column]) or pd.api.types.is_dtype_equal(df[column].dtype, "Int64"):
            # Replace NaN or pd.NA with 0 for Int64 columns
            df[column] = df[column].fillna(0).astype('Int64')

    return df
