import os
import logging
import pandas as pd
from db1_main_df.db11_merge import get_next_unique_id
from db1_main_df.db03_dtype_dict import f_types_vals

def correct_and_validate_dot_info_df(dot_info_df):
    # Correct values: Replace NaN with empty strings in 'comment_di' field
    dot_info_df['comment_di'] = dot_info_df['comment_di'].fillna('')
    return dot_info_df

def load_di_dataframe():
    try:
        dot_info_file_path = "./data/dotrep_config.csv"
        
        # Load the CSV with explicit data types for the columns using the 'dtype' value from f_types_vals
        dot_info_df = pd.read_csv(dot_info_file_path, dtype={
            "item_name_rp_di": f_types_vals["item_name_rp_di"]['dtype'],
            "item_name_hm_di": f_types_vals["item_name_hm_di"]['dtype'],
            "dot_struc_di": f_types_vals["dot_struc_di"]['dtype'],
            "item_type_rp_di": f_types_vals["item_type_rp_di"]['dtype'],
            "item_type_hm_di": f_types_vals["item_type_hm_di"]['dtype'],
            "cat_1_di": f_types_vals["cat_1_di"]['dtype'],
            "cat_1_name_di": f_types_vals["cat_1_name_di"]['dtype'],
            "cat_2_di": f_types_vals["cat_2_di"]['dtype'],
            "comment_di": f_types_vals["comment_di"]['dtype'],
            "no_show_di": f_types_vals["no_show_di"]['dtype']
        }).copy()

        # Record the original order of rows
        dot_info_df['sort_orig'] = (dot_info_df.index + 1).astype(f_types_vals["sort_orig"]['dtype'])  # Convert to Int64 explicitly

        # Assign unique IDs
        dot_info_df['unique_id_di'] = dot_info_df.apply(lambda row: get_next_unique_id(), axis=1)
        dot_info_df["unique_id_di"] = dot_info_df["unique_id_di"].astype(f_types_vals["unique_id_di"]['dtype'])

        # Apply the correction and validation function
        dot_info_df = correct_and_validate_dot_info_df(dot_info_df)

        # Input dataframe display toggle
        show_output = False
        show_full_df = False

        if show_output:
            if show_full_df:
                print("7️⃣ dot_info DataFrame:\n", dot_info_df)
            else:
                print("7️⃣ dot_info DataFrame (First 5 rows):\n", dot_info_df.head())

        return dot_info_df
    except Exception as e:
        logging.error(f"Error loading dot_info CSV: {e}")
        return pd.DataFrame()