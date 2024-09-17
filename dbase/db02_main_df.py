import pandas as pd

from .db18_org import (
    # add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns_main
)
from .db30_debug import print_debug_info
from .db06_load_rp import load_rp_dataframe
from .db07_load_hm import load_hm_dataframe
from .db08_load_db import load_dotbot_yaml_dataframe
from .db09_load_di import load_di_dataframe

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
    # Create 'sort_out' column to indicate new, missing, or matched items in the report
    # main_df['sort_out'] = pd.Series(dtype='Int64')  # Create sort_out column

    # main_df['sort_out'] = main_df['sort_out'].fillna(-1)
    main_df['sort_orig'] = main_df['sort_orig'].fillna(-1).astype('Int64') # sort_orig = Int64, handle missing vals

    main_df = apply_output_grouping(main_df)

    main_df = main_df.sort_values('sort_orig', ascending=True) # Sort the entire DataFrame by 'sort_orig'
    main_df = main_df.reset_index(drop=True)

    # main_df = reorder_columns_main(main_df)
    full_main_dataframe = main_df

    return full_main_dataframe

def df_merge_1_setup(main_df, home_df, dotbot_df, dot_info_df, print_df):
    left_merge_field = 'item_name' # Only declared once; it remains the "left input" for all merges

    # First merge: repo and home
    right_merge_field = 'item_name_hm'
    main_df = df_merge_2_actual(main_df, home_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge1(main_df)

    # Second merge: repo+home and dotbot R+H
    right_merge_field = 'item_name_rp_db'
    main_df = df_merge_2_actual(main_df, dotbot_df, left_merge_field, right_merge_field)  # Merge the DataFrames

    # Third merge: repo+home+dotbot and dot_info R+H
    right_merge_field = 'item_name_rp_di'
    main_df = df_merge_2_actual(main_df, dot_info_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge3(main_df)

    return main_df

def df_merge_2_actual(main_df, input_df, left_merge_field, right_merge_field, merge_type='outer'):
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

def consolidate_post_merge1(main_df): # Copies the priority name to item_name
    def consolidate(row):
        if pd.notna(row['item_name_rp']) and row['item_name_rp'] != "":
            return row['item_name_rp']
        elif pd.notna(row['item_name_hm']) and row['item_name_hm'] != "":
            return row['item_name_hm']
        else:
            return row['item_name']  # If both are null, keep the original value

    main_df['item_name'] = main_df.apply(consolidate, axis=1)
    return main_df

def consolidate_post_merge3(main_df): # Copies the priority name to item_name
    def consolidate(row):
        if pd.notna(row['item_name_rp_di']) and row['item_name_rp_di'] != "":
            return row['item_name_rp_di']
        elif pd.notna(row['item_name_hm_di']) and row['item_name_hm_di'] != "":
            return row['item_name_hm_di']
        else:
            return row['item_name']  # If both are null, keep the original value

    main_df['item_name'] = main_df.apply(consolidate, axis=1)
    return main_df