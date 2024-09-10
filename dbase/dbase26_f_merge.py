import pandas as pd

from .dbase16_validate import validate_df_dict_current_and_main


def compare_documents(main_df):
    # Compare name fields first
    names_match = (
        (main_df['item_name_hm_db'] == main_df['item_name_rp_db']) &
        (main_df['item_name_hm_di'] == main_df['item_name_rp_di'])
    )
    main_df.loc[names_match, 'r_status_1'] = 'Name Match'
    main_df.loc[~names_match, 'r_status_1'] = 'Name Mismatch'

    # Compare type fields, accounting for expected symlinks in Home and actual items in Repo
    types_match = (
        ((main_df['item_type_rp_db'].isin(['file', 'folder', 'file_alias', 'folder_alias'])) &
         (main_df['item_type_hm_db'].isin(['file_sym', 'folder_sym'])))
    )
    main_df.loc[types_match, 'r_status_2'] = 'Type Match'
    main_df.loc[~types_match, 'r_status_2'] = 'Type Mismatch'

    return main_df

def field_merge_2(main_df):

    return main_df

def field_merge_3(main_df):

    return main_df



# def field_merge_1(main_df):
#     # Define the condition for matching names
#     names_match = (main_df['item_name_home'] == main_df['item_name_repo']) & \
#                   (main_df['item_name_home'] == main_df['item_name_hm']) & \
#                   (main_df['item_name_home'] == main_df['item_name_rp'])  # Add more comparisons as needed

#     # Apply status based on the match condition
#     main_df.loc[names_match, 'r_status_1'] = 'Name Match'
#     main_df.loc[~names_match, 'r_status_1'] = 'Name Mismatch'

#     return main_df