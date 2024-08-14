import os
import logging
import pandas as pd
import numpy as np

from .dbase02_id_gen import get_next_unique_id
from .dbase03_item_type import determine_item_type  # Import the new item type detection function
from .dbase08_validate import validate_values

def load_tp_dataframe(template_file_path, start_id=None):
    try:
        template_df = pd.read_csv(template_file_path, dtype={
            "tp_item_name": object,
            "tp_item_type": object,
            "tp_cat_1": object,
            "tp_cat_1_name": object,
            "tp_comment": object,
            "tp_cat_2": object,
            "no_show": bool  # Ensure no_show is read as boolean
        })

        # Apply item type detection using the new function
        template_df['tp_item_type'] = template_df['tp_item_name'].apply(determine_item_type)

        # Correct specific values in the DataFrame according to the given rules.
        validate_values(template_df, {
            "tp_item_type": {
                "valid_types": ['folder', 'file', 'file_sym', 'folder_sym', 'file_alias', 'folder_alias', '[NO FILETYPE]'],
            },
            "original_order": {
                "assign_sequence": True,
            },
            "tp_comment": {
                "fillna": '',
            },
            "tp_item_name": {
                "ensure_not_null": True,
            }
        })

        # Assign unique IDs using the centralized function
        template_df['tp_unique_id'] = template_df.apply(lambda row: get_next_unique_id(), axis=1)

        return template_df
    except Exception as e:
        logging.error(f"Error loading template CSV: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error