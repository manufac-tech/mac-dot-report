import os
import logging
import pandas as pd
from .dbase02_id_gen import get_next_unique_id
from .dbase08_validate import validate_values

def correct_and_validate_template_df(template_df):
    # Correct values: Replace NaN with empty strings in 'comment_tp' field
    template_df['comment_tp'] = template_df['comment_tp'].fillna('')

    return template_df

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

def load_tp_dataframe():
    try:
        template_file_path = "./data/mac-dot-template.csv"
        
        # Load the CSV with explicit data types for the columns
        template_df = pd.read_csv(template_file_path, dtype={
            "item_name": "string",
            "item_type": "string",
            "cat_1": "string",
            "cat_1_name": "string",
            "comment": "string",
            "cat_2": "string",
            "no_show": "bool"
        })

        # Record the original order of rows
        template_df['original_order'] = (template_df.index + 1).astype("Int64")  # Convert to Int64 explicitly

        # Assign unique IDs
        template_df['unique_id_tp'] = template_df.apply(lambda row: get_next_unique_id(), axis=1)
        template_df["unique_id_tp"] = template_df["unique_id_tp"].astype("Int64")

        # Apply the correction and validation function
        template_df = correct_and_validate_template_df(template_df)

        # Apply the enhanced blank replacement function
        template_df = replace_string_blanks(template_df)

        # Toggle output directly within the function
        show_output = True  # Change to False to disable output
        show_full_df = False  # Change to True to show the full DataFrame

        if show_output:
            if show_full_df:
                print("7️⃣ Template DataFrame:\n", template_df)
            else:
                print("7️⃣ Template DataFrame (First 5 rows):\n", template_df.head())

        return template_df
    except Exception as e:
        logging.error(f"Error loading template CSV: {e}")
        return pd.DataFrame()