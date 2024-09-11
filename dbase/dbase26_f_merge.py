import pandas as pd

from .dbase16_validate import validate_df_dict_current_and_main


def compare_documents(main_df):
    # Fill NaN with an empty string in the name and type columns
    main_df[['item_name_hm_db', 'item_name_rp_db', 'item_name_hm_di', 'item_name_rp_di']] = \
        main_df[['item_name_hm_db', 'item_name_rp_db', 'item_name_hm_di', 'item_name_rp_di']].fillna('')
    
    main_df[['item_type_hm_db', 'item_type_rp_db']] = \
        main_df[['item_type_hm_db', 'item_type_rp_db']].fillna('')

    # Define the condition for matching names
    names_match = (
        (main_df['item_name_hm_db'] == main_df['item_name_rp_db']) &
        (main_df['item_name_hm_di'] == main_df['item_name_rp_di'])
    )

    # Define the condition for matching types
    types_match = (
        ((main_df['item_type_rp_db'].isin(['file', 'folder', 'file_alias', 'folder_alias'])) &
         (main_df['item_type_hm_db'].isin(['file_sym', 'folder_sym'])))
    )

    # Concatenate the match status, handling each case carefully
    main_df['r_status_1'] = main_df.apply(
        lambda row: (
            (f"N_Yes" if names_match[row.name] else "N_No") + 
            ", " + 
            (f"T_Yes" if types_match[row.name] else "T_No")
        ), axis=1
    )

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