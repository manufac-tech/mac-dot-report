import pandas as pd

from .dbase04_load_hm import load_hm_dataframe
from .dbase05_load_rp import load_rp_dataframe
from .dbase06_load_db import load_dotbot_yaml_dataframe
from .dbase07_load_tp import load_tp_dataframe
from .dbase09_merge import merge_dataframes
from .dbase08_validate import validate_dataframes

pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.set_option('display.width', None)  # Set the display width to None
pd.set_option('display.max_colwidth', None)  # Set the max column width to None

def build_main_dataframe():
    # Step 1: Define DataFrames and paths
    dataframes_to_merge = {
        'home': {
            'load_function': load_hm_dataframe, 
            'suffix': 'hm'
        },
        'repo': {
            'load_function': load_rp_dataframe, 
            'suffix': 'rp'
        },
        # Uncomment for additional dataframes
        # 'dotbot': {
        #     'load_function': load_dotbot_yaml_dataframe, 
        #     'suffix': 'db'
        # },
        # 'template': {
        #     'load_function': load_tp_dataframe, 
        #     'suffix': 'tp'
        # }
    }

    # Step 2: Initialize the main dataframe using the first DataFrame
    main_dataframe = initialize_main_dataframe(dataframes_to_merge)
    print(f"🟧 Initial Main DataFrame after initialization:\n{main_dataframe}")

    # Step 3: Load all dataframes into a dictionary
    loaded_dataframes = {}
    for df_name, df_info in dataframes_to_merge.items():
        loaded_dataframes[df_name] = df_info['load_function']()
        print(f"db1️⃣ {df_name} DataFrame loaded:\n{loaded_dataframes[df_name]}")

    # Step 4: Perform the merges with subsequent DataFrames
    for df_name in list(dataframes_to_merge.keys())[1:]:  # Iterate through remaining DataFrames
        validate_dataframes(main_dataframe, loaded_dataframes[df_name])  # Validate both dataframes
        df2_field_suffix = dataframes_to_merge[df_name]['suffix']

        # Directly pass the current DataFrame and its suffix to the merge function
        main_dataframe = merge_dataframes(main_dataframe, loaded_dataframes[df_name], df2_field_suffix)
        print(f"🟧 Main DataFrame after merging with {df_name}:\n{main_dataframe}")
    
    print(f"🟧 Final Main DataFrame after merging all DataFrames:\n{main_dataframe}")
    
    return main_dataframe

def initialize_main_dataframe(dataframes_to_merge):
    # Step 1: Identify the first DataFrame programmatically
    first_df_name = list(dataframes_to_merge.keys())[0]  # Get the first DataFrame name
    first_df_info = dataframes_to_merge[first_df_name]
    df1_field_suffix = first_df_info['suffix']

    # Step 2: Load the first DataFrame
    home_dataframe = first_df_info['load_function']()
    print(f"db1️⃣ {first_df_name} DataFrame loaded:\n{home_dataframe}")

    # Step 3: Check if required columns exist
    required_columns = ['item_name', 'item_type']
    for col in required_columns:
        if col not in home_dataframe.columns:
            raise ValueError(f"Column '{col}' not found in the DataFrame '{first_df_name}'")

    # Step 4: Duplicate item_name and item_type with a suffix for the first DataFrame
    home_dataframe[f'item_name_{df1_field_suffix}'] = home_dataframe['item_name']
    home_dataframe[f'item_type_{df1_field_suffix}'] = home_dataframe['item_type']

    # Step 5: Create the global item_name and item_type fields
    home_dataframe['item_name'] = home_dataframe[f'item_name_{df1_field_suffix}']  # Initially use the first DataFrame's values
    home_dataframe['item_type'] = home_dataframe[f'item_type_{df1_field_suffix}']

    print(f"Initialization complete. Main DataFrame after initializing:\n{home_dataframe}")

    return home_dataframe