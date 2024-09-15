import pandas as pd

from .dbase18_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns_main
)
from .dbase30_debug import print_debug_info
from .dbase06_load_hm import load_hm_dataframe
from .dbase07_load_rp import load_rp_dataframe
from .dbase08_load_db import load_dotbot_yaml_dataframe
from .dbase09_load_di import load_di_dataframe

def build_main_dataframe():
    # Define individual DataFrames
    home_df = load_hm_dataframe()
    repo_df = load_rp_dataframe()
    dotbot_df = load_dotbot_yaml_dataframe()
    dot_info_df = load_di_dataframe()

    # Initialize the main_dataframe
    main_df = home_df.copy()

    # Create global fields
    main_df['item_name'] = main_df['item_name_hm']
    main_df['item_type'] = main_df['item_type_hm']
    main_df['unique_id'] = main_df['unique_id_hm']

    print_df = 'none'  # Specify the output level here: 'full', 'short', or 'none'

    # Print debugging information if needed
    print_debug_info(section_name='initialize', section_dict={'dataframe': main_df}, print_df=print_df)

    # First merge: home and repo
    left_merge_field = 'item_name'
    right_merge_field = 'item_name_rp'
    main_df = merge_dataframes(main_df, repo_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    print_debug_info(section_name='repo', section_dict={'dataframe': main_df}, print_df=print_df)

    # Second merge: home+repo and dotbot
    right_merge_field = 'item_name_rp_db'
    main_df = merge_dataframes(main_df, dotbot_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    print_debug_info(section_name='dotbot', section_dict={'dataframe': main_df}, print_df=print_df)

    # Third merge: home+repo+dotbot and dot_info
    right_merge_field = 'item_name_rp_di'
    main_df = merge_dataframes(main_df, dot_info_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    print_debug_info(section_name='dot_info', section_dict={'dataframe': main_df}, print_df=print_df)

    # After the final merge, process the DataFrame
    main_df = add_and_populate_out_group(main_df)

    # Ensure original_order is Int64 and handle missing values
    main_df['original_order'] = main_df['original_order'].fillna(-1).astype('Int64')

    # Apply output grouping
    main_df = apply_output_grouping(main_df)
    # main_df = reorder_columns_main(main_df)

    full_main_dataframe = main_df  # This is the final, fully merged dataframe

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

def replace_string_blanks(df):
    for column in df.columns:
        if pd.api.types.is_string_dtype(df[column]):
            # Convert everything to string to ensure we can replace all forms of NA
            df[column] = df[column].astype(str)
            # Replace all variations of NA, including case-insensitive matches
            df[column] = df[column].str.replace(r'(?i)^<na>$', '', regex=True)
            df[column] = df[column].str.replace(r'(?i)^nan$', '', regex=True)
            df[column] = df[column].str.replace(r'(?i)^none$', '', regex=True)
            # Fill remaining NaN values with empty string
            df[column] = df[column].fillna('')
    return df