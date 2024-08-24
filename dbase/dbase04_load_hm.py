import os
import logging
import pandas as pd

from .dbase02_id_gen import get_next_unique_id
from .dbase03_item_type import determine_item_type

def load_hm_dataframe():
    dot_items = []
    home_dir_path = os.path.expanduser("~")  # Define the home directory path

    for item in os.listdir(home_dir_path):
        if item.startswith("."):
            item_path = os.path.join(home_dir_path, item)
            item_type = determine_item_type(item_path)
            item_name_hm = item  # Define item_name_hm based on the item name

            dot_items.append({
                "item_name_hm": item, 
                "item_type_hm": item_type, 
                "unique_id_hm": get_next_unique_id()
            })

    df = pd.DataFrame(dot_items)

    # Explicitly set data types
    df["item_name_hm"] = df["item_name_hm"].astype("string")
    df["item_type_hm"] = df["item_type_hm"].astype("string")
    df["unique_id_hm"] = df["unique_id_hm"].astype("Int64")

    return df