import pandas as pd

from .dbase04_load_hm import load_hm_dataframe
from .dbase05_load_rp import load_rp_dataframe
from .dbase06_load_db import load_dotbot_yaml_dataframe
from .dbase07_load_tp import load_tp_dataframe
from .dbase09_merge import merge_dataframes
from .dbase08_validate import validate_df_dict_current_and_main

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

    # Debug: Output full dictionaries for all sections (home, repo, dotbot, template)
    for section_name, section_dict in input_df_dict.items():
        print(f"\n1️⃣ Dictionary for '{section_name}' section:")
        print(f"  dataframe_name: '{section_name}_dataframe'")
        print(f"  dataframe_shape: {section_dict['dataframe'].shape}")
        print(f"  suffix: '{section_dict['suffix']}'")
        print(f"  merge_field: '{section_dict['merge_field']}'")
        print(f"  name_field: '{section_dict['name_field']}'")
        print(f"  type_field: '{section_dict['type_field']}'")

    # Step 2: Initialize the main dataframe using the first DataFrame
    first_df_section = list(input_df_dict.items())[0][1]  # Get the first dictionary section
    main_df_dict = initialize_main_dataframe(first_df_section)

    # Step 3: Perform the merges with subsequent DataFrames and output the result of each merge

    max_iterations = 3  # Set the maximum number of iterations for merging
    for iteration, df_name in enumerate(list(input_df_dict.keys())[1:], start=1):  # Start from the second DataFrame
        if iteration > max_iterations:
            break

        print(f"\n1️⃣ Iteration {iteration}: Merging with '{df_name}' DataFrame")
        
        input_df_dict_section = input_df_dict[df_name]

        # Validate the main DataFrame dict and current input DataFrame section
        main_df_dict = validate_df_dict_current_and_main(main_df_dict, main_df_dict, 'main')
        input_df_dict_section = validate_df_dict_current_and_main(input_df_dict_section, input_df_dict_section, df_name)

        # Merge the validated DataFrames
        main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section)

        # Debug: Output the result of the merge
        print(f"\n1️⃣ Result of the merge (Iteration {iteration}) with '{df_name}':\n{main_df_dict['dataframe'].head()}")

        # Final printout after all merges are completed
        print("1️⃣ 🟩 [Final] Final Main DataFrame after all merges:\n", main_df_dict['dataframe'].head())
        print("1️⃣ 🟩 [Final] Final Main DataFrame Dictionary Summary:")
        print(f"  dataframe_shape: {main_df_dict['dataframe'].shape}")
        print(f"  suffix: {main_df_dict['suffix']}")
        print(f"  merge_field: {main_df_dict['merge_field']}")
        print(f"  name_field: {main_df_dict['name_field']}")
        print(f"  type_field: {main_df_dict['type_field']}")

    return main_df_dict

def initialize_main_dataframe(first_df_section):
    # Step 1: Extract the necessary information
    main_dataframe = first_df_section['dataframe']
    df1_field_suffix = first_df_section['suffix']

    # Step 2: Duplicate item_name, item_type, and unique_id without the suffix to create global fields
    main_dataframe['item_name'] = main_dataframe[f'item_name_{df1_field_suffix}']
    main_dataframe['item_type'] = main_dataframe[f'item_type_{df1_field_suffix}']
    main_dataframe['unique_id'] = main_dataframe[f'unique_id_{df1_field_suffix}']

    # Toggle output directly within the function
    show_output = True  # Change to False to disable output
    show_full_df = False  # Change to True to show the full DataFrame

    if show_output:
        if show_full_df:
            print("1️⃣ 🟩 [Initialize] Main DataFrame after initialization:\n", main_dataframe)
        else:
            print("1️⃣ 🟩 [Initialize] Main DataFrame after initialization (First 5 rows):\n", main_dataframe.head())

    # Step 3: Create the dictionary section for the main DataFrame with consistent structure
    main_df_dict = {
        'dataframe': main_dataframe,
        'suffix': '',  # No suffix for the global fields
        'merge_field': 'item_name',  # Global merge field
        'name_field': 'item_name',   # Global name field
        'type_field': 'item_type'    # Global type field
    }

    # Toggle output for the main_df_dict
    show_dict_output = True  # Change to False to disable output

    if show_dict_output:
        # Print a summary instead of the full DataFrame
        print(f"1️⃣ 🟩 [Initialize] Main DataFrame Dictionary (Summary):")
        print(f"  dataframe_name: 'main_dataframe'")
        print(f"  dataframe_shape: {main_dataframe.shape}")
        print(f"  suffix: ''")
        print(f"  merge_field: 'item_name'")
        print(f"  name_field: 'item_name'")
        print(f"  type_field: 'item_type'")

    return main_df_dict