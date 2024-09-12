import pandas as pd

from .dbase16_validate import validate_df_dict_current_and_main

def field_merge_main(report_dataframe):
    """Master function to handle field merging, comparing document and FS results."""

    # Perform document comparison and update the DataFrame
    report_dataframe = compare_docs_di_and_db(report_dataframe)
    
    # Perform file system comparison and update the DataFrame
    report_dataframe = compare_fs_rp_and_hm(report_dataframe)
    
    # Apply field merge logic using both document and FS comparisons
    report_dataframe['final_status'] = report_dataframe.apply(
        lambda row: determine_merge_status(
            row,
            row['fm_doc_comp'],  # Pass the dictionary instead of individual fields
            row['fs_status'],
            check_fs_conditions(row)  # Pass the result of the FS check directly
        ), axis=1
    )

    return report_dataframe

def determine_merge_status(row, doc_comparison, fs_status, fs_condition):
    """Helper function to determine final merge status based on document and FS checks."""

    # Use Booleans for match conditions
    names_match = doc_comparison['names_match']
    types_match = doc_comparison['types_match']

    # Full Match condition (both names and types match in the doc comparison)
    if names_match and types_match and fs_status == 'N_Yes, T_Yes':
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

def compare_docs_di_and_db(df):
    """Compare documents (dot-info.csv and DotBot YAML) for name and type matching."""

    # Fill NaN with an empty string in the name and type columns
    df[['item_name_hm_db', 'item_name_rp_db', 'item_name_hm_di', 'item_name_rp_di']] = \
        df[['item_name_hm_db', 'item_name_rp_db', 'item_name_hm_di', 'item_name_rp_di']].fillna('')

    df[['item_type_hm_db', 'item_type_rp_db']] = \
        df[['item_type_hm_db', 'item_type_rp_db']].fillna('')

    # Define the condition for matching names
    names_match = (
        (df['item_name_hm_db'] == df['item_name_rp_db']) &
        (df['item_name_hm_di'] == df['item_name_rp_di']) &
        (df['item_name_hm_db'] != '') & (df['item_name_rp_db'] != '') & 
        (df['item_name_hm_di'] != '') & (df['item_name_rp_di'] != '')
    )

    # Define the condition for matching types
    types_match = (
        (df['item_type_rp_db'].isin(['file', 'folder', 'file_alias', 'folder_alias'])) &
        (df['item_type_hm_db'].isin(['file_sym', 'folder_sym']))
    )

    # Concatenate the name and type match statuses into Booleans
    df['fm_doc_comp'] = df.apply(
        lambda row: {
            'names_match': bool(names_match[row.name]),
            'types_match': bool(types_match[row.name])
        }, 
        axis=1
    )

    return df


def compare_fs_rp_and_hm(df):
    """Compare file system items (repo and home folders) for name and type matching."""

    # Fill NaN with an empty string in the name and type columns for the file system
    df[['item_name_rp', 'item_name_hm', 'item_type_rp', 'item_type_hm']] = \
        df[['item_name_rp', 'item_name_hm', 'item_type_rp', 'item_type_hm']].fillna('')

    # Define the condition for matching names between repo and home folders
    names_match_fs = (df['item_name_rp'] == df['item_name_hm'])

    # Define the condition for matching types, accounting for expected symlinks in home
    types_match_fs = (
        (df['item_type_rp'].isin(['file', 'folder', 'file_alias', 'folder_alias'])) &
        (df['item_type_hm'].isin(['file_sym', 'folder_sym']))
    )

    # Store the results in a dictionary
    df['fm_fs_comp'] = df.apply(
        lambda row: {
            'names_match_fs': names_match_fs[row.name],
            'types_match_fs': types_match_fs[row.name]
        },
        axis=1
    )

    return df

def check_fs_conditions(row):
    """Function to check specific file system conditions (home-only, repo-only, sym overwrite, new home item)."""

    # Check for Home-only condition
    if row['item_name_rp'] == '' and row['item_name_hm'] != '':
        return 'Home Only'

    # Check for Repo-only condition
    if row['item_name_hm'] == '' and row['item_name_rp'] != '':
        return 'Repo Only'

    # Check for symlink overwritten by an actual file
    if (row['item_type_hm'] in ['file', 'folder']) and \
       (row['item_type_rp'] in ['file', 'folder']):  # Ensure repo expects a symlink
        return 'Sym Overwritten'

    return 'No Condition'




def field_merge_2(df):

    return df

def field_merge_3(df):

    return df

