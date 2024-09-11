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
    main_df_dict = initialize_main_dataframe(input_df_dict['home']) # Initialize the main_dataframe
    
    print_df = 'none'  # Specify the output level here: 'full', 'short', or 'none'

    max_iterations = 3  # diagnostic

    # Perform the merges with subsequent DataFrames and output the result of each merge
    for iteration, df_name in enumerate(list(input_df_dict.keys())[1:], start=1):  # Start from the second DataFrame
        if iteration > max_iterations: # diagnostic
            break

        # print(f"\n 1️⃣ Iteration {iteration}: Merging with '{df_name}' DataFrame")

        input_df_dict_section = input_df_dict[df_name]

        main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section) # Merge the DataFrames

        # merge_func_name = input_df_dict_section.get('unique_id_merge_func') 

        # print(f"DEBUG: Retrieved function name from dict: {merge_func_name}") # Print the function name before calling it

        # if merge_func_name:
        #     merge_func = globals().get(merge_func_name)
        #     if merge_func:
        #         print(f"DEBUG: Found the function '{merge_func_name}', calling it now...")  # Print before calling the function
        #         main_df_dict['dataframe'] = merge_func(main_df_dict['dataframe'])
        #     else:
        #         print(f"Error: Function '{merge_func_name}' not found.")
        # else:
        #     print(f"No merge function for '{df_name}'.")

        # Call the print function to display the result of each merge
        print_debug_info(section_name=df_name, section_dict=main_df_dict, print_df='none')

    # After the final merge
    main_df_dict['dataframe'] = add_and_populate_out_group(main_df_dict['dataframe'])

    # Ensure original_order is Int64 and handle missing values
    main_df_dict['dataframe']['original_order'] = main_df_dict['dataframe']['original_order'].fillna(-1).astype('Int64')

    main_df_dict['dataframe'] = apply_output_grouping(main_df_dict['dataframe'])
    # main_df_dict['dataframe'] = reorder_columns_main(main_df_dict['dataframe'])

    full_main_dataframe = main_df_dict['dataframe']  # This is the final, fully merged dataframe

    return full_main_dataframe
