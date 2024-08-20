import pandas as pd

from .dbase04_load_hm import load_hm_dataframe
from .dbase05_load_rp import load_rp_dataframe
from .dbase06_load_db import load_dotbot_yaml_dataframe
from .dbase07_load_tp import load_tp_dataframe
from .dbase09_merge import merge_dataframes
from .dbase08_validate import validate_df_current_and_main

pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.set_option('display.width', None)  # Set the display width to None
pd.set_option('display.max_colwidth', None)  # Set the max column width to None

def build_main_dataframe():
    # Step 1: Define DataFrames and paths
    input_df_dict = {
        'home': {
            'dataframe': load_hm_dataframe(),
            'suffix': 'hm',
            'merge_field': 'item_name_hm',
            'name_field': 'item_name_hm',
            'type_field': 'item_type_hm'
        },
        'repo': {
            'dataframe': load_rp_dataframe(),
            'suffix': 'rp',
            'merge_field': 'item_name_rp',
            'name_field': 'item_name_rp',
            'type_field': 'item_type_rp'
        },
        # Additional DataFrames that were muted can be reintroduced as needed
        # 'dotbot': {
        #     'dataframe': load_dotbot_yaml_dataframe(),
        #     'suffix': 'db',
        #     'merge_field': 'item_name_rp_db',
        #     'name_field': 'item_name_rp_db',
        #     'type_field': 'item_type_db'
        # },
        # 'template': {
        #     'dataframe': load_tp_dataframe(),
        #     'suffix': 'tp',
        #     'merge_field': 'item_name_tp',
        #     'name_field': 'item_name_tp',
        #     'type_field': 'item_type_tp'
        # }
    }

    # Step 2: Initialize the main dataframe using the first DataFrame
    first_df_section = list(input_df_dict.values())[0]  # Get the first dictionary section
    main_df_dict = initialize_main_dataframe(first_df_section)

    # Step 3: Perform the merges with subsequent DataFrames
    for df_name in list(input_df_dict.keys())[1:]:  # Iterate through remaining DataFrames
        input_df_dict_section = input_df_dict[df_name]

        # Print statements to show the data being sent for validation
        print(f"Validating current_input_df_section: {df_name}")
        print(f"DataFrame:\n{input_df_dict_section['dataframe'].head()}")
        print(f"Main DataFrame:\n{main_df_dict['dataframe'].head()}")

        # Perform validation
        # validate_df_current_and_main(main_df_dict, input_df_dict_section)

        # Directly pass the current DataFrame and its suffix to the merge function
        main_df_dict['dataframe'] = merge_dataframes(main_df_dict['dataframe'], input_df_dict_section['dataframe'])

    print(f"Final Main DataFrame after merging all DataFrames:\n{main_df_dict['dataframe'].head()}")

    return main_df_dict

def initialize_main_dataframe(first_df_section):
    # Extract the actual DataFrame from the section
    first_df = first_df_section['dataframe']
    df_suffix = first_df_section['suffix']

    # Print the DataFrame before making changes
    print(f"🟪 INIT - First DataFrame before adding global fields:\n{first_df.head()}")

    # Duplicate item_name, item_type, and unique_id without the suffix to create global fields
    first_df['item_name'] = first_df[f'item_name_{df_suffix}']
    first_df['item_type'] = first_df[f'item_type_{df_suffix}']
    first_df['unique_id'] = first_df[f'unique_id_{df_suffix}']

    # Create the dictionary section for the main DataFrame
    main_df_dict = {
        'dataframe': first_df,
        'suffix': '', 
        'merge_field': 'item_name',
        'name_field': 'item_name',
        'type_field': 'item_type'
    }

    # Print the DataFrame after adding global fields
    print(f"🟪 INIT - Main DataFrame after adding global fields:\n{first_df.head()}")

    # Return the dictionary section for the main DataFrame
    return main_df_dict