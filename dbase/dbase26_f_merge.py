import pandas as pd

from .dbase16_validate import validate_df_dict_current_and_main


def compare_docs_di_and_db(main_df):
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


def compare_fs_rp_and_hm(main_df):
    # Fill NaN with an empty string ONLY in the name and type columns for the file system
    main_df[['item_name_rp', 'item_name_hm', 'item_type_rp', 'item_type_hm']] = \
        main_df[['item_name_rp', 'item_name_hm', 'item_type_rp', 'item_type_hm']].fillna('')

    # Match names between repo and home folder
    names_match_fs = (main_df['item_name_rp'] == main_df['item_name_hm'])

    # Match types between repo and home folder
    types_match_fs = (
        (main_df['item_type_rp'].isin(['file', 'folder', 'file_alias', 'folder_alias'])) &
        (main_df['item_type_hm'].isin(['file_sym', 'folder_sym']))
    )

    # Concatenate the results into RStatus2 for basic name and type matching
    main_df['r_status_2'] = main_df.apply(
        lambda row: f"N_Yes, T_Yes" if names_match_fs[row.name] and types_match_fs[row.name] 
        else f"N_Yes, T_No" if names_match_fs[row.name] 
        else f"N_No, T_Yes" if types_match_fs[row.name]
        else "N_No, T_No",
        axis=1
    )

    # Call the check_fs_conditions function to handle edge cases
    main_df = check_fs_conditions(main_df)

    return main_df

# Function to check specific file system conditions (home-only, repo-only, sym overwrite, new home item)
def check_fs_conditions(main_df):
    # Check for Home-only condition [**MAY CAUSE ERROR IF NaN**]
    home_only_condition = (main_df['item_name_rp'] == '') & (main_df['item_name_hm'] != '')
    main_df.loc[home_only_condition, 'r_status_3'] = 'Home Only'

    # Check for Repo-only condition [**MAY CAUSE ERROR IF NaN**]
    repo_only_condition = (main_df['item_name_hm'] == '') & (main_df['item_name_rp'] != '')
    main_df.loc[repo_only_condition, 'r_status_3'] = 'Repo Only'

    # Check for symlink overwritten by an actual file
    sym_overwrite_condition = (main_df['item_type_hm'].isin(['file', 'folder'])) & \
                            (main_df['item_type_rp'].isin(['file', 'folder']))  # Ensure repo expects a symlink

    main_df.loc[sym_overwrite_condition, 'r_status_3'] = 'Sym Overwritten'

    # Check for new item in home (untracked by repo or documents)
    new_home_item_condition = (main_df['item_name_hm'] != '') & \
                            (main_df['item_name_rp'] == '') & \
                            (main_df['item_name_hm_di'] == '')  # Ensure it's not tracked in CSV

    main_df.loc[new_home_item_condition, 'r_status_3'] = 'New Home Item'

    return main_df




def field_merge_2(main_df):

    return main_df

def field_merge_3(main_df):

    return main_df

