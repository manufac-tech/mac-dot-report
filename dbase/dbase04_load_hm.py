import os
import logging
import pandas as pd
import numpy as np

from .dbase02_id_gen import get_next_unique_id
from .dbase03_item_type import determine_item_type

def load_hm_dataframe():
    dot_items = []
    home_dir_path = os.path.expanduser("~")

    for item in os.listdir(home_dir_path):
        if item.startswith("."):
            item_path = os.path.join(home_dir_path, item)
            item_type = determine_item_type(item_path)
            dot_items.append({
                "fs_item_name": item, 
                "fs_item_type": item_type, 
                "fs_unique_id": get_next_unique_id()
            })

    df = pd.DataFrame(dot_items)

    return df