import os
import logging
import pandas as pd
from .dbase04_id_gen import get_next_unique_id
from .dbase16_validate import validate_values

def correct_and_validate_dot_info_df(dot_info_df):
    # Correct values: Replace NaN with empty strings in 'comment_di' field
    dot_info_df['comment_di'] = dot_info_df['comment_di'].fillna('')

    return dot_info_df

def replace_string_blanks(df):
    for column in df.columns:
        if pd.api.types.is_string_dtype(df[column]):
            # Convert everything to string to ensure we can replace all forms of NA
            df[column] = df[column].astype(str)
            # Replace all variations of NA, including case-insensitive matches
            df[column] = df[column].str.replace(r'(?i)^<na>$', '', regex=True)
            df[column] = df[column].str.replace(r'(?i)^nan$', '', regex=True)
            df[column] = df[column].str.replace(r'(?i)^none$', '', regex=True)
            # Fill remaining NaN values with empty string
            df[column] = df[column].fillna('')
    return df

def load_di_dataframe():
    try:
        dot_info_file_path = "./data/dot-info.csv"
        
        # Load the CSV with explicit data types for the columns
        dot_info_df = pd.read_csv(dot_info_file_path, dtype={
            "item_name_di": "string",
            "item_type_di": "string",
            "cat_1_di": "string",
            "cat_1_name_di": "string",
            "comment_di": "string",
            "cat_2_di": "string",
            "no_show": "bool",
            "dot_items_fs": "string"
        }).copy()

        # Record the original order of rows
        dot_info_df['original_order'] = (dot_info_df.index + 1).astype("Int64")  # Convert to Int64 explicitly

        # Assign unique IDs
        dot_info_df['unique_id_di'] = dot_info_df.apply(lambda row: get_next_unique_id(), axis=1)
        dot_info_df["unique_id_di"] = dot_info_df["unique_id_di"].astype("Int64")

        # Apply the correction and validation function
        dot_info_df = correct_and_validate_dot_info_df(dot_info_df)

        # Apply the enhanced blank replacement function
        dot_info_df = replace_string_blanks(dot_info_df)

        # Toggle output directly within the function
        show_output = True  # Change to False to disable output
        show_full_df = True  # Change to True to show the full DataFrame

        if show_output:
            if show_full_df:
                print("7️⃣ dot_info DataFrame:\n", dot_info_df)
            else:
                print("7️⃣ dot_info DataFrame (First 5 rows):\n", dot_info_df.head())

        return dot_info_df
    except Exception as e:
        logging.error(f"Error loading dot_info CSV: {e}")
        return pd.DataFrame()