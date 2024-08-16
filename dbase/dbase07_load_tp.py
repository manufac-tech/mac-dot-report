import os
import logging
import pandas as pd
from .dbase02_id_gen import get_next_unique_id
from .dbase08_validate import validate_values

def load_tp_dataframe():
    try:
        # Define the path directly in the module
        template_file_path = "./data/mac-dot-template.csv"
        
        template_df = pd.read_csv(template_file_path, dtype={
            "item_name": object,
            "item_type": object,
            "cat_1": object,
            "cat_1_name": object,
            "comment": object,
            "cat_2": object,
            "no_show": bool  # Ensure no_show is read as boolean
        })

        # Correct specific values in the DataFrame according to the given rules.
        validate_values(template_df, {
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

        # Assign unique IDs using the centralized function
        template_df['unique_id'] = template_df.apply(lambda row: get_next_unique_id(), axis=1)

        return template_df
    except Exception as e:
        logging.error(f"Error loading template CSV: {e}")
        return pd.DataFrame()