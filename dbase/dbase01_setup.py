import pandas as pd

from .dbase04_load_hm import load_hm_dataframe
from .dbase05_load_rp import load_rp_dataframe
from .dbase06_load_db import load_dotbot_yaml_dataframe
from .dbase07_load_tp import load_tp_dataframe
from .dbase09_merge import merge_dataframes
from .dbase08_validate import validate_dataframes

def build_main_dataframe():
    # Step 1: Define DataFrames, suffixes, and paths
    dataframes_to_merge = {
        'home': {
            'load_function': load_hm_dataframe, 
            'suffix': 'hm'
        },
        'repo': {
            'load_function': load_rp_dataframe, 
            'suffix': 'rp'
        },
        # 'dotbot': {
        #     'load_function': load_dotbot_yaml_dataframe, 
        #     'suffix': 'db'
        # },
        # 'template': {
        #     'load_function': load_tp_dataframe, 
        #     'suffix': 'tp'
        # }
    }

    # Step 2: Load the DataFrames and define the main data frame immediately
    loaded_dataframes = {}
    for df_name, df_info in dataframes_to_merge.items():
        df = df_info['load_function']()  # Call the load function without passing the path
        loaded_dataframes[df_name] = df
        print(f"{df_name} DataFrame loaded:\n", df)

    # Define the main DataFrame immediately from the first DataFrame
    main_dataframe = loaded_dataframes['home']  # Start with the first DataFrame as the main data frame
    print(f"Main DataFrame initialized:\n", main_dataframe)

    # Step 3: Perform the merges with subsequent DataFrames
    for df_name in list(dataframes_to_merge.keys())[1:]:  # Iterate through remaining DataFrames
        validate_dataframes(main_dataframe, loaded_dataframes[df_name])  # Validate both data frames
        suffix1 = dataframes_to_merge['home']['suffix']
        suffix2 = dataframes_to_merge[df_name]['suffix']

        suffix_mapping = {
            'df1': [suffix1, main_dataframe],
            'df2': [suffix2, loaded_dataframes[df_name]]
        }
        
        main_dataframe = merge_dataframes(main_dataframe, loaded_dataframes[df_name], suffix_mapping)
        print(f"Main DataFrame after merging with {df_name}:\n", main_dataframe)

    return main_dataframe