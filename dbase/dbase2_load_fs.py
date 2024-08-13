import os
import logging
import pandas as pd
import numpy as np

def load_fs_dataframe():
    """
    Create a DataFrame from the dot items in the home directory and calculate the starting ID for template items.

    Returns:
        tuple: A tuple containing the DataFrame with filesystem items and the starting unique ID for template items.
    """
    dot_items = []
    home_dir_path = os.path.expanduser("~")

    for item in os.listdir(home_dir_path):
        if item.startswith("."):
            item_path = os.path.join(home_dir_path, item)
            item_type = 'folder' if os.path.isdir(item_path) else 'file'
            dot_items.append({"fs_item_name": item, "fs_item_type": item_type})

    df = pd.DataFrame(dot_items)

    # Assign sequential unique ID
    df['fs_unique_id'] = np.arange(1, len(df) + 1)

    # Calculate the starting ID for template items
    id_max_fs = df['fs_unique_id'].max()
    id_start_tp = id_max_fs + 1

    # Return both the DataFrame and the starting ID
    return df, id_start_tp
