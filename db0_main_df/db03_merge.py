import pandas as pd
import logging

from db5_global.db52_dtype_dict import f_types_vals
from .db04_merge_sup import consolidate_post_merge1, consolidate_post_merge3, print_main_df_build_hist

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

        main_df_build_hist["df3"] = merged_dataframe.copy()
        # print_main_df_build_hist(main_df_build_hist) # Print the build history 🟡

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe
