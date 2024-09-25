import os
import logging
import pandas as pd
import numpy as np

from db3_load.db07_load_hm import load_hm_dataframe
from db3_load.db09_load_di import load_di_dataframe


# def apply_output_grouping(df):
#     # Sort the entire DataFrame by 'sort_orig'
#     df_sorted = df.sort_values('sort_orig', ascending=True)
#     df_sorted = df_sorted.reset_index(drop=True)
#     return df_sorted

# def reorder_dfm_cols_perm(df):
#     # Define the desired column order based on the provided fields
#     desired_order = [
#         'item_name', 'item_type', 'unique_id',
#         'item_name_rp', 'item_type_rp', 'git_rp', 'item_name_hm', 'item_type_hm',
#         'item_name_hm_db', 'item_type_hm_db', 'item_name_rp_db', 'item_type_rp_db',
#         'item_name_rp_di', 'item_type_rp_di', 'item_name_hm_di', 'item_type_hm_di',
#         'dot_struc_di', 'cat_1_di', 'cat_1_name_di', 'cat_2_di', 'comment_di', 'no_show_di',
#         'sort_orig',
#         'unique_id_rp', 'unique_id_db', 'unique_id_hm', 'unique_id_di'
#     ]
    
#     # Ensure all columns in desired_order are in the DataFrame
#     for col in desired_order:
#         if col not in df.columns:
#             print(f"Warning: Column {col} not found in DataFrame")
    
#     # Reorder columns
#     df = df[desired_order]
    
#     return df





