import pandas as pd

from .dbase06_load_hm import load_hm_dataframe
from .dbase07_load_rp import load_rp_dataframe
from .dbase08_load_db import load_dotbot_yaml_dataframe
from .dbase09_load_di import load_di_dataframe
from .dbase30_debug import print_debug_info

def create_input_df_dict():
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
            'type_field': 'item_type_rp_di',
            'unique_id_merge_func': 'field_merge_3_uid'
        }
    }
    return input_df_dict

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

    # Create the main_dataframe dictionary
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