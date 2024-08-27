import os
import logging
import pandas as pd
from .dbase02_id_gen import get_next_unique_id
from .dbase08_validate import validate_values

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

        # Validate and correct values in the DataFrame
        template_df = validate_values(template_df, {
            "item_type": {
                "valid_types": ['folder', 'file', 'alias'],
            },
            "original_order": {
                "assign_sequence": True,
            },
            "comment": {
                "fillna": '',
            },
            "item_name": {
                "ensure_not_null": True,
            }
        })

        # Assign unique IDs
        template_df['unique_id_tp'] = template_df.apply(lambda row: get_next_unique_id(), axis=1)
        template_df["unique_id_tp"] = template_df["unique_id_tp"].astype("Int64")

        # Rename columns to add the _tp suffix
        template_df.rename(columns={
            "item_name": "item_name_tp",
            "item_type": "item_type_tp",
            "unique_id": "unique_id_tp"  # Ensure unique_id column is correctly named
        }, inplace=True)

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