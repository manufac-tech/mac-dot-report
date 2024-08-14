import os
import logging
import pandas as pd
import numpy as np

from .dbase02_id_gen import get_next_unique_id


def load_fs_dataframe():
    # Your existing code to create the DataFrame
    dot_items = []
    home_dir_path = os.path.expanduser("~")

    for item in os.listdir(home_dir_path):
        if item.startswith("."):
            item_path = os.path.join(home_dir_path, item)
            item_type = 'folder' if os.path.isdir(item_path) else 'file'
            dot_items.append({
                "fs_item_name": item, 
                "fs_item_type": item_type, 
                "fs_unique_id": get_next_unique_id()
            })

    df = pd.DataFrame(dot_items)

    # Calculate the starting ID for template items
    # id_start_tp = df['fs_unique_id'].max() + 1

    return df