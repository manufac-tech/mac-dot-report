import pandas as pd

from .dbase16_validate import validate_df_dict_current_and_main

def field_merge_main(report_dataframe):
    """Master function to handle field merging, comparing document and FS results."""
    
    # Apply field merge logic using both document and FS comparisons
    report_dataframe['final_status'] = report_dataframe.apply(
        lambda row: determine_merge_status(
            row,
            row['r_status_1'], 
            row['r_status_2'],
            check_fs_conditions(row)  # Pass the result of the FS check directly
        ), axis=1
    )

    return report_dataframe

def determine_merge_status(row, doc_status, fs_status, fs_condition):
    """Helper function to determine final merge status based on document and FS checks."""
    
    # Full Match condition
    if doc_status == 'N_Yes, T_Yes' and fs_status == 'N_Yes, T_Yes':
        return 'Full Match'
    
    # Check for specific FS conditions
    if fs_condition == 'Sym Overwritten':
        return 'Sym Overwritten'
    if fs_condition == 'New Home Item':
        return 'New Home Item'
    if fs_condition == 'Home Only':
        return 'Home Only'
    if fs_condition == 'Repo Only':
        return 'Repo Only'
    
    # Default to Mismatch
    return 'Mismatch'

def compare_docs_di_and_db(main_df):
    """Compare documents (dot-info.csv and DotBot YAML) for name and type matching."""

    # Fill NaN with an empty string in the name and type columns
    main_df[['item_name_hm_db', 'item_name_rp_db', 'item_name_hm_di', 'item_name_rp_di']] = \
        main_df[['item_name_hm_db', 'item_name_rp_db', 'item_name_hm_di', 'item_name_rp_di']].fillna('')

    main_df[['item_type_hm_db', 'item_type_rp_db']] = \
        main_df[['item_type_hm_db', 'item_type_rp_db']].fillna('')

    # Define the condition for matching names
    names_match = (
        (main_df['item_name_hm_db'] == main_df['item_name_rp_db']) &
        (main_df['item_name_hm_di'] == main_df['item_name_rp_di']) &
        (main_df['item_name_hm_db'] != '') & (main_df['item_name_rp_db'] != '') & 
        (main_df['item_name_hm_di'] != '') & (main_df['item_name_rp_di'] != '')
    )

    # Define the condition for matching types
    types_match = (
        (main_df['item_type_rp_db'].isin(['file', 'folder', 'file_alias', 'folder_alias'])) &
        (main_df['item_type_hm_db'].isin(['file_sym', 'folder_sym']))
    )

    # Concatenate the name and type match statuses
    main_df['r_status_1'] = main_df.apply(
        lambda row: (f"N_Yes" if names_match[row.name] else "N_No") + 
                    ", " + 
                    (f"T_Yes" if types_match[row.name] else "T_No"), 
        axis=1
    )

    return main_df


def compare_fs_rp_and_hm(main_df):
    """Compare file system items (repo and home folders) for name and type matching."""
    
    # Fill NaN with an empty string in the name and type columns for the file system
    main_df[['item_name_rp', 'item_name_hm', 'item_type_rp', 'item_type_hm']] = \
        main_df[['item_name_rp', 'item_name_hm', 'item_type_rp', 'item_type_hm']].fillna('')

    # Define the condition for matching names between repo and home folders
    names_match_fs = (main_df['item_name_rp'] == main_df['item_name_hm'])

    # Define the condition for matching types, accounting for expected symlinks in home
    types_match_fs = (
        (main_df['item_type_rp'].isin(['file', 'folder', 'file_alias', 'folder_alias'])) &
        (main_df['item_type_hm'].isin(['file_sym', 'folder_sym']))
    )

    # Concatenate the match status for file system into RStatus2
    main_df['r_status_2'] = main_df.apply(
        lambda row: f"N_Yes, T_Yes" if names_match_fs[row.name] and types_match_fs[row.name] 
        else f"N_Yes, T_No" if names_match_fs[row.name] 
        else f"N_No, T_Yes" if types_match_fs[row.name]
        else "N_No, T_No",
        axis=1
    )

    # Call CheckFSConditions to update RStatus3
    main_df = check_fs_conditions(main_df)

    return main_df

def check_fs_conditions(row):
    """Return specific FS conditions for a given row, handling empty strings."""
    
    # Check for Home-only condition (no matching item in repo)
    if row['item_name_rp'] == '' and row['item_name_hm'] != '':
        return 'Home Only'
    
    # Check for Repo-only condition (no matching item in home)
    if row['item_name_hm'] == '' and row['item_name_rp'] != '':
        return 'Repo Only'
    
    # Check for symlink overwritten by an actual file
    if row['item_type_hm'] in ['file', 'folder'] and row['item_type_rp'] in ['file', 'folder']:
        return 'Sym Overwritten'
    
    # Check for new item in home (untracked by repo or documents)
    if row['item_name_hm'] != '' and row['item_name_rp'] == '' and row['item_name_hm_di'] == '':
        return 'New Home Item'
    
    # Default case if no conditions match
    return None




def field_merge_2(main_df):

    return main_df

def field_merge_3(main_df):

    return main_df

