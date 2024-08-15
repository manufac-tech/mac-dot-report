import os
import logging
import pandas as pd
import numpy as np

from .dbase02_id_gen import get_next_unique_id
from .dbase03_item_type import determine_item_type

def load_repo_dataframe(repo_path):
    repo_items = []

    for item in os.listdir(repo_path):
        if item.startswith("."):
            item_path = os.path.join(repo_path, item)
            item_type = determine_item_type(item_path)
            repo_items.append({
                "rp_item_name": item, 
                "rp_item_type": item_type, 
                "rp_unique_id": get_next_unique_id()
            })

    df = pd.DataFrame(repo_items)

    # Create unified 'unique_id'
    df['unique_id'] = df['rp_unique_id']

    return df