
import pandas as pd
# from .dbase16_validate import validate_df_dict_current_and_main

current_unique_id = 1

def get_next_unique_id():
    global current_unique_id
    unique_id = current_unique_id
    current_unique_id += 1
    return unique_id

