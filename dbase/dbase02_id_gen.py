
import pandas as pd
from .dbase08_validate import validate_dataframes

current_unique_id = 1

def get_next_unique_id():
    global current_unique_id
    unique_id = current_unique_id
    current_unique_id += 1
    return unique_id

# def pre_merge_processing(df1, df2, suffix1, suffix2):
#     # Configuration for validation
#     config = {
#         'item_name': {'valid_types': ['string'], 'ensure_not_null': True},
#         'item_type': {'valid_types': ['string'], 'ensure_not_null': True},
#         'unique_id': {'valid_types': ['numeric'], 'ensure_not_null': True}
#     }
    
#     # Validate the two DataFrames
#     validate_dataframes(config, df1, df2)

#     # Create a dictionary to map each DataFrame's suffix and DataFrame
#     suffix_mapping = {
#         'df1': [suffix1, df1],  # Use generic key 'df1'
#         'df2': [suffix2, df2]   # Use generic key 'df2'
#     }

#     return suffix_mapping