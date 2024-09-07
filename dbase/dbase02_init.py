import pandas as pd

from .dbase03_id_gen import (
    field_merge_1_uid,
    field_merge_2_uid,
    field_merge_3_uid
)
from .dbase06_load_hm import load_hm_dataframe
from .dbase07_load_rp import load_rp_dataframe
from .dbase08_load_db import load_dotbot_yaml_dataframe
from .dbase09_load_di import load_di_dataframe
from .dbase16_validate import validate_df_dict_current_and_main
from .dbase17_merge import merge_dataframes
from .dbase18_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns
)
from .dbase20_debug import print_debug_info

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