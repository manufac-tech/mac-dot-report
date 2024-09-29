import pandas as pd
import logging

from db5_global.db52_dtype_dict import f_types_vals
from .db14_merge_sup import consolidate_post_merge1, consolidate_post_merge3, print_main_df_build_hist

def df_merge_sequence(main_df, home_df, dotbot_df, user_config_df, print_df):
    left_merge_field = 'item_name' # Only declared once; it remains the "left input" for all merges

    # First merge: repo and home
    right_merge_field = 'item_name_hm'
    main_df = df_merge(main_df, home_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge1(main_df)

    # Second merge: repo+home and dotbot R+H
    right_merge_field = 'item_name_rp_db'
    main_df = df_merge(main_df, dotbot_df, left_merge_field, right_merge_field)  # Merge the DataFrames

    # Third merge: repo+home+dotbot and dot_info R+H
    right_merge_field = 'item_name_rp_cf'
    main_df = df_merge(main_df, user_config_df, left_merge_field, right_merge_field)  # Merge the DataFrames
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
        print_main_df_build_hist(main_df_build_hist) # Print the build history 🟡

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe

def apply_output_grouping(df):
    # Sort the entire DataFrame by 'sort_orig'
    df_sorted = df.sort_values('sort_orig', ascending=True)
    df_sorted = df_sorted.reset_index(drop=True)
    return df_sorted

def reorder_dfm_cols_perm(df):
    # Define the desired column order based on the provided fields
    desired_order = [
        'item_name', 'item_type', 'unique_id',
        'item_name_rp', 'item_type_rp', 'git_rp', 'item_name_hm', 'item_type_hm',
        'item_name_hm_db', 'item_type_hm_db', 'item_name_rp_db', 'item_type_rp_db',
        'item_name_rp_cf', 'item_type_rp_cf', 'item_name_hm_cf', 'item_type_hm_cf',
        'dot_struc_cf', 'cat_1_cf', 'cat_1_name_cf', 'cat_2_cf', 'comment_cf', 'no_show_cf',
        'sort_orig',
        'unique_id_rp', 'unique_id_db', 'unique_id_hm', 'unique_id_cf'
    ]
    
    # Ensure all columns in desired_order are in the DataFrame
    for col in desired_order:
        if col not in df.columns:
            print(f"Warning: Column {col} not found in DataFrame")
    
    # Reorder columns
    df = df[desired_order]
    
    return df


# def handle_duplicates(df):
#     # Identify duplicates based on 'item_name'
#     duplicates = df[df.duplicated(subset=['item_name'], keep=False)]

#     # Prioritize filesystem sources over configuration documents
#     for item_name in duplicates['item_name'].unique():
#         duplicate_rows = duplicates[duplicates['item_name'] == item_name]
#         if len(duplicate_rows) > 1:
#             # Prioritize row with non-null 'item_name_home'
#             priority_row = duplicate_rows[duplicate_rows['item_name_home'].notna()]
#             if not priority_row.empty:
#                 df = df.drop(duplicate_rows.index)
#                 df = df.append(priority_row)
    
#     # Remove any remaining duplicates
#     df = df.drop_duplicates(subset=['item_name', 'item_type'], keep='first')
#     return df



# def df_merge(main_df, input_df, left_merge_field, right_merge_field, merge_type='outer'):
#     main_df_build_hist = {"df1": main_df.copy(), "df2": input_df.copy()}


#     if merge_step == 'b': 
#         left_merge_field = right_merge_field
#         right_merge_field = left_merge_field
#         merge_type == 'inner'

#     try:
#         merged_dataframe = pd.merge(
#             main_df, input_df,
#             left_on=left_merge_field,
#             right_on=right_merge_field,
#             how=merge_type
#         ).copy()

#         main_df_build_hist["df3"] = merged_dataframe.copy()
#         # print_main_df_build_hist(main_df_build_hist) # Print the build history 🟡

#     except Exception as e:
#         raise RuntimeError(f"Error during merge: {e}")

#     if merge_step == 'b':
#         merge_step = 'a'
#     elif merge_step == 'a':
#         merge_step = 'b'
    
#     return merged_dataframe