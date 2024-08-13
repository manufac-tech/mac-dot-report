import os
import logging
import pandas as pd
import numpy as np

from .dbase4_validate import (
    validate_values
)

def load_tp_dataframe(template_file_path, start_id=None):
    """
    Load the CSV template into a staging DataFrame with index and prepare it.

    Args:
        template_file_path (str): Path to the template CSV file.
        start_id (int, optional): The starting unique ID for unmatched template items.

    Returns:
        DataFrame: A DataFrame containing the template data with an original order index.
    """
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

        # Corrects specific values in the DataFrame according to the given rules.
        validate_values(template_df, {
            "tp_item_type": {
                "valid_types": ['folder', 'file', '[NO FILETYPE]'],
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
     
        # Assign unique IDs to unmatched template items starting from start_id if provided
        if start_id is not None:
            template_df['tp_unique_id'] = np.arange(start_id, start_id + len(template_df))
        else:
            template_df['tp_unique_id'] = np.nan  # Ensure the column exists

        return template_df
    except Exception as e:
        logging.error(f"Error loading template CSV: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error