import pandas as pd
from .dbase03_init import create_input_df_dict, initialize_main_dataframe
from .dbase09_load_di import replace_string_blanks
from .dbase18_org import add_and_populate_out_group, apply_output_grouping
from .dbase30_debug import print_debug_info

def build_main_dataframe():
    input_df_dict = create_input_df_dict()  # Define DataFrames and paths
    main_df_dict = initialize_main_dataframe(input_df_dict['home'])  # Initialize the main_dataframe

    # First merge: home and repo
    input_df_dict_section_repo = input_df_dict['repo']
    main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section_repo)
    print_debug_info(section_name='repo', section_dict=main_df_dict, print_df='none')

    # Second merge: home+repo and dotbot
    input_df_dict_section_dotbot = input_df_dict['dotbot']
    main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section_dotbot)
    print_debug_info(section_name='dotbot', section_dict=main_df_dict, print_df='none')

    # Third merge: home+repo+dotbot and dot_info
    input_df_dict_section_dot_info = input_df_dict['dot_info']
    main_df_dict['dataframe'] = merge_dataframes(main_df_dict, input_df_dict_section_dot_info)
    print_debug_info(section_name='dot_info', section_dict=main_df_dict, print_df='none')

    # After the final merge
    main_df_dict['dataframe'] = add_and_populate_out_group(main_df_dict['dataframe'])

    # Ensure 'original_order' is Int64 and handle missing values
    main_df_dict['dataframe']['original_order'] = main_df_dict['dataframe']['original_order'].fillna(-1).astype('Int64')

    # Apply output grouping
    main_df_dict['dataframe'] = apply_output_grouping(main_df_dict['dataframe'])

    # Final, fully merged DataFrame
    full_main_dataframe = main_df_dict['dataframe']

    return full_main_dataframe

def merge_dataframes(main_df_dict, input_df_dict_section, merge_type='outer'):
    # Extract the DataFrames from the dictionary sections
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict_section['dataframe']
    left_merge_field = main_df_dict['merge_field']
    right_merge_field = input_df_dict_section['merge_field']

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