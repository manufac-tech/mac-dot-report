import os
import logging
import pandas as pd
from dbase1_main.db11_merge import get_next_unique_id
from dbase1_main.db03_dtype_dict import field_types  # Import the field_types dictionary

def correct_and_validate_dot_info_df(dot_info_df):
    # Correct values: Replace NaN with empty strings in 'comment_di' field
    dot_info_df['comment_di'] = dot_info_df['comment_di'].fillna('')
    return dot_info_df

def load_di_dataframe():
    try:
        dot_info_file_path = "./data/dotrep_config.csv"
        
        # Load the CSV with explicit data types for the columns using the field_types dictionary
        dot_info_df = pd.read_csv(dot_info_file_path, dtype={
            "item_name_rp_di": field_types["item_name_rp_di"],
            "item_name_hm_di": field_types["item_name_hm_di"],
            "dot_struc_di": field_types["dot_struc_di"],
            "item_type_rp_di": field_types["item_type_rp_di"],
            "item_type_hm_di": field_types["item_type_hm_di"],
            "cat_1_di": field_types["cat_1_di"],
            "cat_1_name_di": field_types["cat_1_name_di"],
            "cat_2_di": field_types["cat_2_di"],
            "comment_di": field_types["comment_di"],
            "no_show_di": field_types["no_show_di"]
        }).copy()

        # Record the original order of rows
        dot_info_df['sort_orig'] = (dot_info_df.index + 1).astype(field_types["sort_orig"])  # Convert to Int64 explicitly

        # Assign unique IDs
        dot_info_df['unique_id_di'] = dot_info_df.apply(lambda row: get_next_unique_id(), axis=1)
        dot_info_df["unique_id_di"] = dot_info_df["unique_id_di"].astype(field_types["unique_id_di"])

        # Apply the correction and validation function
        dot_info_df = correct_and_validate_dot_info_df(dot_info_df)

        # Toggle output directly within the function
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