import os
import logging
import pandas as pd
import numpy as np

from .dbase04_load_hm import load_home_items  # Ensure this import is correct
from .dbase05_load_rp import load_repo_dataframe
from .dbase06_load_db import load_dotbot_yaml_dataframe
from .dbase07_load_tp import load_tp_dataframe
from .dbase08_validate import validate_dataframes
from .dbase09_merge import merge_dataframes
from .dbase10_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns,
    sort_items_1_out_group,
    sort_items_2_indiv,
)

def build_main_dataframe(template_path, dotbot_yaml_path, repo_path):
    # Load the dataframes (assuming these functions are defined elsewhere)
    home_items_df = load_home_items(template_path, dotbot_yaml_path)
    repo_items_df = load_repo_items(repo_path)

    # Debug: Print columns of home_items_df and repo_items_df before validation
    print("Columns in home_items_df before validation:", home_items_df.columns)
    print("Columns in repo_items_df before validation:", repo_items_df.columns)

    # Validate the individual DataFrames
    home_items_df, repo_items_df = validate_dataframes(home_items_df, repo_items_df)

    # Merge the dataframes
    main_dataframe = merge_dataframes(home_items_df, repo_items_df)

    # Debug: Print columns after merging
    print("Columns after merging:", main_dataframe.columns)

    return main_dataframe