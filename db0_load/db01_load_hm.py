import os
import logging
import pandas as pd

from db1_main_df.db14_merge_sup import get_next_unique_id
from db1_main_df.db12_get_type import determine_item_type
from db5_global.db52_dtype_dict import f_types_vals

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

    df = pd.DataFrame(dot_items).copy()

    # Explicitly set data types using the 'dtype' value from the f_types_vals dictionary
    df["item_name_hm"] = df["item_name_hm"].astype(f_types_vals["item_name_hm"]['dtype'])
    df["item_type_hm"] = df["item_type_hm"].astype(f_types_vals["item_type_hm"]['dtype'])
    df["unique_id_hm"] = df["unique_id_hm"].astype(f_types_vals["unique_id_hm"]['dtype'])

    # Input dataframe display toggle
    show_output = False
    show_full_df = False

    if show_output:
        if show_full_df:
            print("4️⃣ Home DataFrame:\n", df)
        else:
            print("4️⃣ Home DataFrame (First 5 rows):\n", df.head())

    return df