import pandas as pd

from .dbase03_init import (
    initialize_main_dataframe,
    create_input_df_dict
)
from .dbase18_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns_main
)
from .dbase30_debug import print_debug_info
from .dbase09_load_di import replace_string_blanks  # Ensure this import is present

def build_main_dataframe():
    input_df_dict = create_input_df_dict()  # Define DataFrames and paths
    main_df_dict = initialize_main_dataframe(input_df_dict['home'])  # Initialize the main_dataframe
    
    print_df = 'none'  # Specify the output level here: 'full', 'short', or 'none'

    # First merge: home and repo
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict['repo']['dataframe']
    left_merge_field = main_df_dict['merge_field']
    right_merge_field = input_df_dict['repo']['merge_field']
    main_df_dict['dataframe'] = merge_dataframes(main_df, input_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    print_debug_info(section_name='repo', section_dict=main_df_dict, print_df=print_df)

    # Second merge: home+repo and dotbot
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict['dotbot']['dataframe']
    left_merge_field = main_df_dict['merge_field']
    right_merge_field = input_df_dict['dotbot']['merge_field']
    main_df_dict['dataframe'] = merge_dataframes(main_df, input_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    print_debug_info(section_name='dotbot', section_dict=main_df_dict, print_df=print_df)

    # Third merge: home+repo+dotbot and dot_info
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict['dot_info']['dataframe']
    left_merge_field = main_df_dict['merge_field']
    right_merge_field = input_df_dict['dot_info']['merge_field']
    main_df_dict['dataframe'] = merge_dataframes(main_df, input_df, left_merge_field, right_merge_field)  # Merge the DataFrames
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

def merge_dataframes(main_df, input_df, left_merge_field, right_merge_field, merge_type='outer'):
    # Perform the merge operation
    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how=merge_type
        ).copy()

        # Apply the blank replacement after the merge
        merged_dataframe = replace_string_blanks(merged_dataframe)

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe