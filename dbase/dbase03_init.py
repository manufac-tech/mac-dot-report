import pandas as pd

from .dbase06_load_hm import load_hm_dataframe
from .dbase07_load_rp import load_rp_dataframe
from .dbase08_load_db import load_dotbot_yaml_dataframe
from .dbase09_load_di import load_di_dataframe
from .dbase30_debug import print_debug_info


def initialize_main_dataframe(first_df_section):
    # Extract information
    main_dataframe = first_df_section['dataframe'].copy()
    df1_field_suffix = first_df_section['suffix']

    # Create global fields
    main_dataframe['item_name'] = main_dataframe[f'item_name_{df1_field_suffix}']
    main_dataframe['item_type'] = main_dataframe[f'item_type_{df1_field_suffix}']
    main_dataframe['unique_id'] = main_dataframe[f'unique_id_{df1_field_suffix}']

    print_df = 'none'  # Specify the output level here: 'full', 'short', or 'none'

    # Keep using the dictionary structure for the main DataFrame
    main_df_dict = {
        'dataframe': main_dataframe,
        'suffix': '',  # No suffix for global fields
        'merge_field': 'item_name',  # Keeping 'item_name' as the primary merge field
    }

    # Print debugging information if needed
    print_debug_info(section_name='initialize', section_dict=main_df_dict, print_df=print_df)

    return main_df_dict