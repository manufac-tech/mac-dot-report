import pandas as pd

from .dbase03_init import (
    initialize_main_dataframe,
    create_input_df_dict
)
# from .dbase04_id_gen import (
#     field_merge_1_uid,
#     field_merge_2_uid,
#     field_merge_3_uid
# )
from .dbase17_merge import merge_dataframes
from .dbase18_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns_main
)
# from .dbase21_rep_df import build_report_dataframe
from .dbase30_debug import print_debug_info

def build_main_dataframe():
    input_df_dict = create_input_df_dict()  # Define DataFrames and paths
    main_df_dict = initialize_main_dataframe(input_df_dict['home'])  # Initialize the main_dataframe
    
    print_df = 'none'  # Specify the output level here: 'full', 'short', or 'none'

    # First merge: home and repo
    input_df_dict_section_repo = input_df_dict['repo']
    main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section_repo)  # Merge the DataFrames
    print_debug_info(section_name='repo', section_dict=main_df_dict, print_df=print_df)

    # Second merge: home+repo and dotbot
    input_df_dict_section_dotbot = input_df_dict['dotbot']
    main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section_dotbot)  # Merge the DataFrames
    print_debug_info(section_name='dotbot', section_dict=main_df_dict, print_df=print_df)

    # Third merge: home+repo+dotbot and dot_info
    input_df_dict_section_dot_info = input_df_dict['dot_info']
    main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section_dot_info)  # Merge the DataFrames
    print_debug_info(section_name='dot_info', section_dict=main_df_dict, print_df=print_df)

    # After the final merge, process the DataFrame
    main_df_dict['dataframe'] = add_and_populate_out_group(main_df_dict['dataframe'])

    # Ensure original_order is Int64 and handle missing values
    main_df_dict['dataframe']['original_order'] = main_df_dict['dataframe']['original_order'].fillna(-1).astype('Int64')

    # Apply output grouping
    main_df_dict['dataframe'] = apply_output_grouping(main_df_dict['dataframe'])
    # main_df_dict['dataframe'] = reorder_columns_main(main_df_dict['dataframe'])

    full_main_dataframe = main_df_dict['dataframe']  # This is the final, fully merged dataframe

    return full_main_dataframe