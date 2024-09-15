import pandas as pd


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


