import os
import logging
import pandas as pd
from .dbase02_id_gen import get_next_unique_id
from .dbase08_validate import validate_values

def correct_and_validate_template_df(template_df):
    # Correct values: Replace NaN with empty strings in 'comment_tp' field
    template_df['comment_tp'].fillna('', inplace=True)

    # Validate primary fields (item_name_tp, item_type_tp)
    # if template_df['item_name_tp'].isnull().any():
    #     logging.error("Some rows in 'item_name_tp' have missing or empty values.")
    #     template_df = template_df[template_df['item_name_tp'].notnull() & (template_df['item_name_tp'] != '')]

    # valid_item_types = ['folder', 'file', 'alias']
    # if not template_df['item_type_tp'].isin(valid_item_types).all():
    #     invalid_values = template_df['item_type_tp'][~template_df['item_type_tp'].isin(valid_item_types)]
    #     logging.warning(f"Invalid values found in 'item_type_tp': {invalid_values.tolist()}")

    return template_df

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

        # Assign unique IDs
        template_df['unique_id_tp'] = template_df.apply(lambda row: get_next_unique_id(), axis=1)
        template_df["unique_id_tp"] = template_df["unique_id_tp"].astype("Int64")

        # Apply the correction and validation function
        template_df = correct_and_validate_template_df(template_df)

        # Toggle output directly within the function
        show_output = True  # Change to False to disable output
        show_full_df = True  # Change to False to show only the first 5 rows

        if show_output:
            if show_full_df:
                print("7️⃣ Template DataFrame:\n", template_df)
            else:
                print("7️⃣ Template DataFrame (First 5 rows):\n", template_df.head())

        return template_df
    except Exception as e:
        logging.error(f"Error loading template CSV: {e}")
        return pd.DataFrame()