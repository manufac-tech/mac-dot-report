import pandas as pd

from .dbase02_id_gen import (
    field_merge_1_uid,
    field_merge_2_uid,
    field_merge_3_uid
)
from .dbase04_load_hm import load_hm_dataframe
from .dbase05_load_rp import load_rp_dataframe
from .dbase06_load_db import load_dotbot_yaml_dataframe
from .dbase07_load_di import load_di_dataframe
from .dbase08_validate import validate_df_dict_current_and_main
from .dbase09_merge import merge_dataframes
from .dbase10_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns
)
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
            'type_field': 'item_type_hm',
            'unique_id_merge_func': None
        },
        'repo': {
            'dataframe': load_rp_dataframe(),
            'suffix': 'rp',
            'merge_field': 'item_name_rp',
            'name_field': 'item_name_rp',
            'type_field': 'item_type_rp',
            'unique_id_merge_func': 'field_merge_1_uid'
        },
        'dotbot': {
            'dataframe': load_dotbot_yaml_dataframe(),
            'suffix': 'db',
            'merge_field': 'item_name_rp_db',
            'name_field': 'item_name_rp_db',
            'type_field': 'item_type_hm_db',
            'unique_id_merge_func': 'field_merge_2_uid'
        },
        'dot_info': {
            'dataframe': load_di_dataframe(),
            'suffix': 'di',
            'merge_field': 'item_name_di',
            'name_field': 'item_name_di',
            'type_field': 'item_type_di',
            'unique_id_merge_func': 'field_merge_3_uid'
        }
    }

    # Initialize the main DataFrame
    main_df_dict = initialize_main_dataframe(input_df_dict['home'])

    # Specify the output level here: 'full', 'short', or 'none'
    print_df = 'none'  # User sets this

    # Perform the merges with subsequent DataFrames and output the result of each merge
    max_iterations = 3  # Set the maximum number of iterations for merging
    for iteration, df_name in enumerate(list(input_df_dict.keys())[1:], start=1):  # Start from the second DataFrame
        if iteration > max_iterations:
            break

        print(f"\n 1️⃣ Iteration {iteration}: Merging with '{df_name}' DataFrame")
        
        input_df_dict_section = input_df_dict[df_name]

        # Merge the DataFrames
        main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section)

        # Print the function name before calling it
        merge_func_name = input_df_dict_section.get('unique_id_merge_func')
        print(f"DEBUG: Retrieved function name from dict: {merge_func_name}")  # This will print the function name

        if merge_func_name:
            merge_func = globals().get(merge_func_name)
            if merge_func:
                print(f"DEBUG: Found the function '{merge_func_name}', calling it now...")  # Print before calling the function
                main_df_dict['dataframe'] = merge_func(main_df_dict['dataframe'])
            else:
                print(f"Error: Function '{merge_func_name}' not found.")
        else:
            print(f"No merge function for '{df_name}'.")

        # Call the print function to display the result of each merge
        print_debug_info(section_name=df_name, section_dict=main_df_dict, print_df='none')

    # After the final merge
    main_df_dict['dataframe'] = add_and_populate_out_group(main_df_dict['dataframe'])

    # Ensure original_order is Int64 and handle missing values
    main_df_dict['dataframe']['original_order'] = main_df_dict['dataframe']['original_order'].fillna(-1).astype('Int64')

    main_df_dict['dataframe'] = apply_output_grouping(main_df_dict['dataframe'])
    main_df_dict['dataframe'] = reorder_columns(main_df_dict['dataframe'])
    
    # Final printout after all merges are completed
    print_debug_info(section_name='final', section_dict=main_df_dict, print_df='full')

    return main_df_dict

def initialize_main_dataframe(first_df_section):
    # Extract information
    main_dataframe = first_df_section['dataframe'].copy()
    df1_field_suffix = first_df_section['suffix']

    # Create global fields
    main_dataframe['item_name'] = main_dataframe[f'item_name_{df1_field_suffix}']
    main_dataframe['item_type'] = main_dataframe[f'item_type_{df1_field_suffix}']
    main_dataframe['unique_id'] = main_dataframe[f'unique_id_{df1_field_suffix}']

    # Add status fields for each merge
    main_dataframe['m_status_1'] = ''  # Status after first merge (Home + Repo)
    main_dataframe['m_status_2'] = ''  # Status after second merge (Home + Repo + DotBot)
    main_dataframe['m_status_3'] = ''  # Status after third merge (for future use)

    print_df = 'none'  # Specify the output level here: 'full', 'short', or 'none'

    # Create the main DataFrame dictionary
    main_df_dict = {
        'dataframe': main_dataframe,
        'suffix': '',  # No suffix for global fields
        'merge_field': 'item_name',
        'name_field': 'item_name',
        'type_field': 'item_type'
    }

    # Call the print function
    # print_debug_info(section_name='initialize', section_dict=main_df_dict, print_df=print_df)

    return main_df_dict