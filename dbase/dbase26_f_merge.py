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


def perform_full_matching(report_dataframe):
    # Ensure the necessary status fields are initialized
    if 'st_main' not in report_dataframe.columns:
        report_dataframe['st_main'] = 'TEMP_STATUS'
    
    if 'st_docs' not in report_dataframe.columns:
        report_dataframe['st_docs'] = 'TEMP_DOC_MATCH'
    
    if 'st_db_all' not in report_dataframe.columns:
        report_dataframe['st_db_all'] = 'TEMP_DB_ALL'

    if 'st_alert' not in report_dataframe.columns:
        report_dataframe['st_alert'] = 'TEMP_ALERT'

    # Call dot_structure_status() to perform structure matches (full match, home only, repo only)
    report_dataframe['st_main'] = dot_structure_status(report_dataframe)
    
    # Call subsystem_docs() for CSV/YAML match
    report_dataframe['st_docs'] = subsystem_docs(report_dataframe)
    
    # Call subsystem_db_all() for DotBot All status
    report_dataframe['st_db_all'] = subsystem_db_all(report_dataframe)

    # Call alert_sym_overwrite() for sym overwrite alert
    report_dataframe['st_alert'] = alert_sym_overwrite(report_dataframe)
    
    return report_dataframe


def alert_sym_overwrite(report_dataframe):
    """
    Function to check for SymLink Overwrite: when a file/folder in the home directory 
    has the same name as an item in the repo but is not a symlink.
    Updates 'st_alert' with 'SymLink Overwrite' based on the conditions.
    """

    # Check if item names match between repo and home
    name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_hm'])

    # Check if types differ in a way that suggests a symlink has been overwritten
    # Expect home type to be file_sym or folder_sym and repo type to be file or folder
    type_mismatch = (
        (report_dataframe['item_type_rp'] == 'file') & (report_dataframe['item_type_hm'] != 'file_sym')
    ) | (
        (report_dataframe['item_type_rp'] == 'folder') & (report_dataframe['item_type_hm'] != 'folder_sym')
    )

    # Set 'SymLink Overwrite' if names match but types differ as described above
    report_dataframe.loc[name_match & type_mismatch, 'st_alert'] = 'SymLink Overwrite'

    # Ensure no default 'TEMP_ALERT' remains
    report_dataframe['st_alert'] = report_dataframe['st_alert'].fillna('')

    return report_dataframe['st_alert']

def subsystem_db_all(report_dataframe):
    """
    Function to verify DotBot (db) alignment between repo, home, and DotBot YAML (.bot.yaml).
    Updates 'st_db_all' with 'Valid' or 'Invalid' based on the match.
    """

    # Check if repo folder (item_name_rp) matches the corresponding entry in .bot.yaml (item_name_rp_db)
    repo_name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_rp_db'])
    repo_type_match = (report_dataframe['item_type_rp'] == report_dataframe['item_type_rp_db'])

    # Check if home folder (item_name_hm) matches the corresponding entry in .bot.yaml (item_name_hm_db)
    home_name_match = (report_dataframe['item_name_hm'] == report_dataframe['item_name_hm_db'])
    home_type_match = (report_dataframe['item_type_hm'] == report_dataframe['item_type_hm_db'])

    # Set 'Valid' or 'Invalid' based on name and type match
    report_dataframe['st_db_all'] = 'Invalid'  # Default to 'Invalid'
    report_dataframe.loc[repo_name_match & repo_type_match & home_name_match & home_type_match, 'st_db_all'] = 'Valid'

    return report_dataframe['st_db_all']


def subsystem_docs(report_dataframe):
    for index, row in report_dataframe.iterrows():
        if (row['item_name_rp_di'] == row['item_name_rp_db'] and
            row['item_type_rp_di'] == row['item_type_rp_db'] and
            row['item_name_hm_di'] == row['item_name_hm_db'] and
            row['item_type_hm_di'] == row['item_type_hm_db']):
            report_dataframe.at[index, 'st_docs'] = 'Valid'
        else:
            report_dataframe.at[index, 'st_docs'] = 'Invalid'
    
    return report_dataframe['st_docs']

def dot_structure_status(report_dataframe):

    # Repo-only logic
    report_dataframe.loc[(report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] != ''), 'st_main'] = 'Repo-only'

    # Home-only logic
    report_dataframe.loc[(report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == ''), 'st_main'] = 'Home-only'

    # Full Match logic
    report_dataframe.loc[
        (report_dataframe['item_name_hm'] != '') & 
        (report_dataframe['item_name_rp'] != '') & 
        (
            ((report_dataframe['item_type_rp'] == 'file') & (report_dataframe['item_type_hm'] == 'file_sym')) |
            ((report_dataframe['item_type_rp'] == 'folder') & (report_dataframe['item_type_hm'] == 'folder_sym'))
        ), 
        'st_main'
    ] = 'Full Match'

    # Default to Mismatch
    report_dataframe.loc[report_dataframe['item_name_hm'].isnull() & report_dataframe['item_name_rp'].isnull(), 'st_main'] = 'Mismatch'

    return report_dataframe['st_main']

    # Default to Mismatch
    report_dataframe.loc[(report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == ''), 'st_main'] = 'Mismatch'

    return report_dataframe['st_main']


def calc_final_merge_status(row, doc_comparison, fs_comparison):
    """Helper function to determine final merge status based on doc and FS checks."""

    # Use Booleans for match conditions
    names_match_docs = doc_comparison['names_match']
    types_match_docs = doc_comparison['types_match']
    names_match_fs = fs_comparison['names_match_fs']
    types_match_fs = fs_comparison['types_match_fs']

    # Full Match condition (both names and types match in the doc comparison)
    if names_match_docs and types_match_docs and names_match_fs and types_match_fs:
        return 'Full Match'
    
    # Check for specific FS conditions
    if not names_match_fs and types_match_fs:
        return 'Sym Overwritten'
    if names_match_fs and not types_match_fs:
        return 'New Home Item'
    if not names_match_fs and not types_match_fs:
        return 'Home Only'
    if names_match_fs and not types_match_fs:
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

def field_merge_2(df):

    return df

def field_merge_3(df):

    return df

