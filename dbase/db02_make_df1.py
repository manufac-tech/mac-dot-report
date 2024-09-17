import pandas as pd

from .db18_org import (
    apply_output_grouping,
    reorder_columns_main
)
from .db30_debug import print_debug_info
from .db06_load_rp import load_rp_dataframe
from .db07_load_hm import load_hm_dataframe
from .db08_load_db import load_dotbot_yaml_dataframe
from .db09_load_di import load_di_dataframe
from .db17_merge import df_merge_1_setup  # Import the moved function

def build_main_dataframe():
    # Define individual DataFrames
    repo_df = load_rp_dataframe()
    home_df = load_hm_dataframe()
    dotbot_df = load_dotbot_yaml_dataframe()
    dot_info_df = load_di_dataframe()
    main_df = repo_df.copy() # Initialize the main_dataframe from the REPO FOLDER

    # Create global fields
    main_df['item_name'] = main_df['item_name_rp']
    main_df['item_type'] = main_df['item_type_rp']
    main_df['unique_id'] = main_df['unique_id_rp']

    print_df = 'none'  # Specify the output level here: 'full', 'short', or 'none'
    print_debug_info(section_name='initialize', section_dict={'dataframe': main_df}, print_df=print_df)

    # THE MERGE
    main_df = df_merge_1_setup(main_df, home_df, dotbot_df, dot_info_df, print_df) # Perform the merges

    # POST-MERGE OPERATIONS

    main_df['sort_orig'] = main_df['sort_orig'].fillna(-1).astype('Int64') # sort_orig = Int64, handle missing vals

    main_df = apply_output_grouping(main_df)

    main_df = main_df.sort_values('sort_orig', ascending=True) # Sort the entire DataFrame by 'sort_orig'
    main_df = main_df.reset_index(drop=True)

    main_df = reorder_columns_main(main_df)
    full_main_dataframe = main_df

    return full_main_dataframe
