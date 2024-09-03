import pandas as pd

from .dbase04_load_hm import load_hm_dataframe
from .dbase05_load_rp import load_rp_dataframe
from .dbase06_load_db import load_dotbot_yaml_dataframe
from .dbase07_load_tp import load_tp_dataframe
from .dbase08_validate import validate_df_dict_current_and_main
from .dbase09_merge import merge_dataframes
from .dbase11_debug import print_debug_info

pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.set_option('display.width', None)  # Set the display width to None
pd.set_option('display.max_colwidth', None)  # Set the max column width to None

def build_main_dataframe():
    # Define DataFrames and paths
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
        'dotbot': {
            'dataframe': load_dotbot_yaml_dataframe(),
            'suffix': 'db',
            'merge_field': 'item_name_rp_db',
            'name_field': 'item_name_rp_db',
            'type_field': 'item_type_db'
        },
        'template': {
            'dataframe': load_tp_dataframe(),
            'suffix': 'tp',
            'merge_field': 'item_name_tp',
            'name_field': 'item_name_tp',
            'type_field': 'item_type_tp'
        }
    }

    # Initialize the main DataFrame
    main_df_dict = initialize_main_dataframe(input_df_dict['home'])

    # Specify the output level here: 'full', 'short', or 'none'
    print_df = 'short'  # User sets this

    # Perform the merges with subsequent DataFrames and output the result of each merge
    max_iterations = 3  # Set the maximum number of iterations for merging
    for iteration, df_name in enumerate(list(input_df_dict.keys())[1:], start=1):  # Start from the second DataFrame
        if iteration > max_iterations:
            break

        print(f"\n 1️⃣ Iteration {iteration}: Merging with '{df_name}' DataFrame")
        
        input_df_dict_section = input_df_dict[df_name]

        # Validate the main DataFrame dict and current input DataFrame section
        main_df_dict = validate_df_dict_current_and_main(main_df_dict, main_df_dict, 'main')
        input_df_dict_section = validate_df_dict_current_and_main(input_df_dict_section, input_df_dict_section, df_name)

        # Merge the validated DataFrames
        main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section)

        # Call the print function to display the result of each merge
        # print_debug_info(section_name=df_name, section_dict=main_df_dict, print_df=print_df)

    # Final printout after all merges are completed
    print_debug_info(section_name='final', section_dict=main_df_dict, print_df=print_df)

    return main_df_dict

def initialize_main_dataframe(first_df_section):
    # Extract information
    main_dataframe = first_df_section['dataframe'].copy()
    df1_field_suffix = first_df_section['suffix']

    # Create global fields
    main_dataframe['item_name'] = main_dataframe[f'item_name_{df1_field_suffix}']
    main_dataframe['item_type'] = main_dataframe[f'item_type_{df1_field_suffix}']
    main_dataframe['unique_id'] = main_dataframe[f'unique_id_{df1_field_suffix}']

    print_df = 'short'  # Specify the output level here: 'full', 'short', or 'none'

    # Create the main DataFrame dictionary
    main_df_dict = {
        'dataframe': main_dataframe,
        'suffix': '',  # No suffix for global fields
        'merge_field': 'item_name',
        'name_field': 'item_name',
        'type_field': 'item_type'
    }

    # Call the print function
    print_debug_info(section_name='initialize', section_dict=main_df_dict, print_df=print_df)

    return main_df_dict