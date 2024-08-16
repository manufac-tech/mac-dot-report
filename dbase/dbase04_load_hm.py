import os
import logging
import pandas as pd

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
                "item_name": item, 
                "item_type": item_type, 
                "unique_id": get_next_unique_id()
            })

    df = pd.DataFrame(dot_items)

    return df

# def load_hm_dataframe():
#     # Create a test DataFrame with three rows
#     data = {
#         "item_name": ["test_item1", "test_item2", "test_item3"],
#         "item_type": ["file", "file", "file"],
#         "unique_id": [1, 2, 3]  # Manually assign unique IDs for the test
#     }

#     df = pd.DataFrame(data, dtype="object")
#     df["unique_id"] = df["unique_id"].astype("Int64")  # Ensure unique_id is Int64

#     return df