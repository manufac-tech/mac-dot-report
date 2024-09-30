import pandas as pd
import logging

from db5_global.db52_dtype_dict import f_types_vals
from .db14_merge_sup import consolidate_post_merge1, consolidate_post_merge3, print_main_df_build_hist

def df_merge_sequence(main_df, home_df, dotbot_df, user_config_df, print_df):
    left_merge_field = 'item_name_rp'  # Use item_name_rp for the first merge

    # First merge: repo and home
    right_merge_field = 'item_name_hm'
    main_df = df_merge(main_df, home_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge1(main_df)
    
    main_df = create_merge_key_post_merge1(main_df)  # Create and populate the merge key field

    # Second merge: repo+home and dotbot R+H
    left_merge_field = 'item_name_m_key'
    right_merge_field = 'item_name_db_m_key'
    main_df = df_merge(main_df, dotbot_df, left_merge_field, right_merge_field)  # Merge the DataFrames

    # Consolidate merge key fields after the second merge
    main_df = consolidate_merge_key_post_merge2(main_df)

    # Third merge: repo+home+dotbot and user_config R+H
    right_merge_field = 'item_name_cf_m_key'
    main_df = df_merge(main_df, user_config_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_merge_key_post_merge3(main_df)
    main_df = consolidate_post_merge3(main_df)

    # Final cleanup: propagate merge key to item_name and drop merge key fields
    main_df = merge_key_final_cleanup(main_df)

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

def create_merge_key_post_merge1(df):
    # Add the new merge key column
    df['item_name_m_key'] = df.apply(
        lambda row: row['item_name_rp'] if not pd.isna(row['item_name_rp']) else row['item_name_hm'], axis=1
    )
    df["item_name_m_key"] = df["item_name_m_key"].astype(f_types_vals["item_name_rp"]['dtype'])
    
    return df

def consolidate_merge_key_post_merge2(df):
    # Consolidate the merge key fields to handle <NA> values
    df['item_name_db_m_key'] = df.apply(
        lambda row: row['item_name_m_key'] if pd.isna(row['item_name_db_m_key']) else row['item_name_db_m_key'], axis=1
    )
    df["item_name_db_m_key"] = df["item_name_db_m_key"].astype(f_types_vals["item_name_rp"]['dtype'])
    
    return df

def consolidate_merge_key_post_merge3(df):
    # Consolidate the merge key fields to handle <NA> values
    df['item_name_cf_m_key'] = df.apply(
        lambda row: row['item_name_db_m_key'] if pd.isna(row['item_name_cf_m_key']) else row['item_name_cf_m_key'], axis=1
    )
    df['item_name_cf_m_key'] = df.apply(
        lambda row: row['item_name_m_key'] if pd.isna(row['item_name_cf_m_key']) else row['item_name_cf_m_key'], axis=1
    )
    df["item_name_cf_m_key"] = df["item_name_cf_m_key"].astype(f_types_vals["item_name_rp"]['dtype'])
    
    return df

def merge_key_final_cleanup(df):
    # Propagate the merge key to item_name
    df['item_name'] = df['item_name_cf_m_key']
    
    # Drop the merge key fields
    df = df.drop(columns=['item_name_m_key', 'item_name_db_m_key', 'item_name_cf_m_key'])
    
    return df