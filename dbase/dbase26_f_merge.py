import pandas as pd

from .dbase16_validate import validate_df_dict_current_and_main

def field_merge_main(report_dataframe):
    """Master function to handle field merging, comparing document and FS results."""
    
    # Run document comparison and get status
    report_dataframe['doc_status'] = report_dataframe.apply(lambda row: compare_docs_di_and_db(row), axis=1)
    
    # Run file system comparison and get status
    report_dataframe['fs_status'] = report_dataframe.apply(lambda row: compare_fs_rp_and_hm(row), axis=1)
    
    # Get file system condition (e.g., Sym Overwritten, Home Only)
    report_dataframe['fs_condition'] = report_dataframe.apply(lambda row: check_fs_conditions(row), axis=1)
    
    # Print to confirm the status fields are populated correctly
    print(report_dataframe[['doc_status', 'fs_status', 'fs_condition']].head())
    
    # Apply the field merge logic to determine the final status
    report_dataframe['final_status'] = report_dataframe.apply(
        lambda row: determine_merge_status(row, row['doc_status'], row['fs_status'], row['fs_condition']),
        axis=1
    )
    
    return report_dataframe

    return report_dataframe

def determine_merge_status(row):
    """Helper function to determine final merge status."""
    
    # Document comparison: Names and types
    doc_names_match = (row['item_name_hm_db'] == row['item_name_rp_db']) and \
                      (row['item_name_hm_di'] == row['item_name_rp_di'])
    doc_types_match = (row['item_type_hm_db'] in ['file_sym', 'folder_sym']) and \
                      (row['item_type_rp_db'] in ['file', 'folder'])

    # File system comparison: Names and types
    fs_names_match = (row['item_name_rp'] == row['item_name_hm'])
    fs_types_match = (row['item_type_rp'] in ['file', 'folder']) and \
                     (row['item_type_hm'] in ['file_sym', 'folder_sym'])

    # Check for a full match between documents and file system
    if doc_names_match and doc_types_match and fs_names_match and fs_types_match:
        return 'Full Match'

    # Check for other specific conditions like 'Sym Overwritten' or 'New Home Item'
    if row['r_status_3'] == 'Sym Overwritten':
        return 'Sym Overwritten'
    if row['r_status_3'] == 'New Home Item':
        return 'New Home Item'
    if row['r_status_3'] == 'Home Only':
        return 'Home Only'
    if row['r_status_3'] == 'Repo Only':
        return 'Repo Only'

    # Default to 'Mismatch' if none of the above conditions are met
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
    """Return specific FS conditions for a given row, handling NaN values properly."""
    
    # Check for Home-only condition (no matching item in repo)
    if pd.notna(row['item_name_hm']) and pd.isna(row['item_name_rp']) and pd.notna(row['item_name_hm_di']):
        return 'Home Only'
    
    # Check for Repo-only condition (no matching item in home)
    if pd.notna(row['item_name_rp']) and pd.isna(row['item_name_hm']):
        return 'Repo Only'
    
    # Check for symlink overwritten by an actual file
    if pd.notna(row['item_type_hm']) and pd.notna(row['item_type_rp']) and \
       row['item_type_hm'] in ['file', 'folder'] and row['item_type_rp'] in ['file', 'folder']:
        return 'Sym Overwritten'
    
    # Check for new item in home (untracked by repo or documents)
    if pd.notna(row['item_name_hm']) and pd.isna(row['item_name_rp']) and pd.isna(row['item_name_hm_di']):
        return 'New Home Item'
    
    # Default case if no conditions match
    return None




def field_merge_2(main_df):

    return main_df

def field_merge_3(main_df):

    return main_df

