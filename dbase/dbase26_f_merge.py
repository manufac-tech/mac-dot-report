import pandas as pd

from .dbase16_validate import validate_df_dict_current_and_main

def field_merge_main(report_dataframe):
    """Master function to handle field merging, comparing document and FS results."""
    # print("field_merge_main() called")
    
    # Perform document comparison and update the DataFrame
    report_dataframe = compare_docs_di_and_db(report_dataframe)
    
    # Perform file system comparison and update the DataFrame
    report_dataframe = compare_fs_rp_and_hm(report_dataframe)
    
    # Apply field merge logic using both document and FS comparisons
    report_dataframe['final_status'] = report_dataframe.apply(
        lambda row: determine_merge_status(
            row,
            row['doc_status'], 
            row['fs_status'],
            check_fs_conditions(row)  # Pass the result of the FS check directly
        ), axis=1
    )

    return report_dataframe

def determine_merge_status(row, doc_status, fs_status, fs_condition):
    """Helper function to determine final merge status based on document and FS checks."""
    # print(f"Processing row {row.name}...")
    
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

def compare_docs_di_and_db(report_dataframe):
    """Compare documents (dot-info.csv and DotBot YAML) for name and type matching and store results in a dictionary."""

    # Fill NaN with an empty string in the name and type columns
    report_dataframe[['item_name_hm_db', 'item_name_rp_db', 'item_name_hm_di', 'item_name_rp_di']] = \
        report_dataframe[['item_name_hm_db', 'item_name_rp_db', 'item_name_hm_di', 'item_name_rp_di']].fillna('')

    report_dataframe[['item_type_hm_db', 'item_type_rp_db']] = \
        report_dataframe[['item_type_hm_db', 'item_type_rp_db']].fillna('')

    # Apply logic row by row to compare names and types, storing results in fm_doc_comp
    def compare_row(row):
        names_match = (
            (row['item_name_hm_db'] == row['item_name_rp_db']) and
            (row['item_name_hm_di'] == row['item_name_rp_di']) and
            (row['item_name_hm_db'] != '') and (row['item_name_rp_db'] != '') and 
            (row['item_name_hm_di'] != '') and (row['item_name_rp_di'] != '')
        )
        types_match = (
            (row['item_type_rp_db'] in ['file', 'folder', 'file_alias', 'folder_alias']) and
            (row['item_type_hm_db'] in ['file_sym', 'folder_sym'])
        )
        return {
            'names_match': 'N_Yes' if names_match else 'N_No',
            'types_match': 'T_Yes' if types_match else 'T_No'
        }
    
    # Store the results in the 'fm_doc_comp' field for each row
    report_dataframe['fm_doc_comp'] = report_dataframe.apply(compare_row, axis=1)

    return report_dataframe


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

    # Concatenate the name and type match statuses
    main_df['fs_status'] = main_df.apply(
        lambda row: (f"N_Yes" if names_match_fs[row.name] else "N_No") + 
                    ", " + 
                    (f"T_Yes" if types_match_fs[row.name] else "T_No"), 
        axis=1
    )

    return main_df

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




def field_merge_2(main_df):

    return main_df

def field_merge_3(main_df):

    return main_df

