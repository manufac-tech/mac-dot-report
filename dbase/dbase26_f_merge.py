import pandas as pd

from .dbase16_validate import validate_df_dict_current_and_main

def field_merge_main(report_dataframe):
    """Master function to handle field merging, comparing document and FS results."""

    report_dataframe = compare_docs_di_and_db(report_dataframe) # Do doc comparison & update DataFrame
    report_dataframe = compare_fs_rp_and_hm(report_dataframe) # Do FS comparison & update DataFrame
    
    # Define a function to process merge status for each row
    def process_merge_status(row):
        doc_match_status = row['fm_doc_match']
        fs_match_status = row['fm_fs_match']
        return calc_final_merge_status(row, doc_match_status, fs_match_status)

    # Apply the merge status logic and update the DataFrame
    report_dataframe['final_status'] = report_dataframe.apply(process_merge_status, axis=1)

    return report_dataframe

def calc_final_merge_status(row, doc_comparison, fs_comparison):
    """Helper function to determine final merge status based on doc and FS checks."""

    # Use Booleans for match conditions
    names_match = doc_comparison['names_match']
    types_match = doc_comparison['types_match']
    names_match_fs = fs_comparison['names_match_fs']
    types_match_fs = fs_comparison['types_match_fs']

    # Full Match condition (both names and types match in the doc comparison)
    if names_match and types_match and names_match_fs and types_match_fs:
        return 'Full Match'
    
    # Check for specific FS conditions
    if not names_match_fs and types_match_fs:
        return 'Sym Overwritten'
    if names_match_fs and not types_match_fs:
        return 'New Home Item'
    if not names_match_fs and not types_match_fs:
        return 'Home Only'
    if names_match_fs and types_match_fs:
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
    df['fm_doc_match'] = df.apply(
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
    df['fm_fs_match'] = df.apply(
        lambda row: {
            'names_match_fs': names_match_fs[row.name],
            'types_match_fs': types_match_fs[row.name]
        },
        axis=1
    )

    return df

# def check_fs_conditions(row):
#     # """Function to check specific file system conditions (home-only, repo-only, sym overwrite, new home item)."""

#     # # Check for Home-only condition
#     # if row['item_name_rp'] == '' and row['item_name_hm'] != '':
#     #     print("Home Only matched")
#     #     return 'Home Only'

#     # # Check for Repo-only condition
#     # if row['item_name_hm'] == '' and row['item_name_rp'] != '':
#     #     print("Repo Only matched)")
#     #     return 'Repo Only'

#     # # Check for symlink overwritten by an actual file
#     # if (row['item_type_hm'] in ['file', 'folder']) and \
#     #    (row['item_type_rp'] in ['file', 'folder']):  # Ensure repo expects a symlink
#     #     print("Sym Overwritten matched")
#     #     return 'Sym Overwritten'

#     return 'No Condition'




def field_merge_2(df):

    return df

def field_merge_3(df):

    return df

