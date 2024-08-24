import os
import logging
import pandas as pd

from .dbase02_id_gen import get_next_unique_id
from .dbase03_item_type import determine_item_type

def load_rp_dataframe():
    repo_items = []
   
    home_dir_path = os.path.expanduser("~")  # Define the home directory path
    repo_path = os.path.join(home_dir_path, "._dotfiles/dotfiles_srb_repo")  # Define the repo path

    for item in os.listdir(repo_path):
        if item.startswith("."):
            item_path = os.path.join(repo_path, item)
            item_type = determine_item_type(item_path)
            repo_items.append({
                "item_name_rp": item,  # Use the suffix '_rp' consistently
                "item_type_rp": item_type,  # Use the suffix '_rp' consistently
                "unique_id_rp": get_next_unique_id()  # Use the suffix '_rp' consistently
            })

    df = pd.DataFrame(repo_items)

    # Explicitly set data types
    df["item_name_rp"] = df["item_name_rp"].astype("string")
    df["item_type_rp"] = df["item_type_rp"].astype("string")
    df["unique_id_rp"] = df["unique_id_rp"].astype("Int64")  # Ensure unique_id_rp is Int64

    return df