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
    
    # Debugging: Print DataFrame before adding merge key
    print("DataFrame before adding merge key (First 5 rows):\n", main_df.head())
    print("DataFrame columns before adding merge key:\n", main_df.columns)
    
    main_df = create_merge_key_post_merge1(main_df)  # Create and populate the merge key field
    
    # Debugging: Print DataFrame after adding merge key
    print("DataFrame after adding merge key (First 5 rows):\n", main_df.head())
    print("DataFrame columns after adding merge key:\n", main_df.columns)

    # Second merge: repo+home and dotbot R+H
    right_merge_field = 'item_name_rp_db'
    main_df = df_merge(main_df, dotbot_df, left_merge_field, right_merge_field)  # Merge the DataFrames

    # Third merge: repo+home+dotbot and user_config R+H
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

def create_merge_key_post_merge1(df):
    # Add the new merge key column
    df['item_name_m_key'] = df.apply(
        lambda row: row['item_name_rp'] if not pd.isna(row['item_name_rp']) else row['item_name_hm'], axis=1
    )
    df["item_name_m_key"] = df["item_name_m_key"].astype(f_types_vals["item_name_rp"]['dtype'])
    return df

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
        'unique_id_rp', 'unique_id_db', 'unique_id_hm', 'unique_id_cf',
        'item_name_m_key',
    ]
    
    # Ensure all columns in desired_order are in the DataFrame
    for col in desired_order:
        if col not in df.columns:
            print(f"Warning: Column {col} not found in DataFrame")
    
    # Reorder columns
    df = df[desired_order]
    
    return df