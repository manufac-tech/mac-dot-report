import pandas as pd

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
    report_dataframe.loc[name_match & type_mismatch, 'st_main'] = 'NO FS MATCH-T'

    return report_dataframe['st_alert']

    import pandas as pd

def alert_in_doc_not_fs(report_dataframe):
    """
    Function to check for entries that exist in the documentation (YAML or CSV)
    but do not exist in the filesystem.
    Updates 'st_alert' with 'In Doc Not FS' based on the conditions.
    """

    # Check for entries that exist in the documentation but not in the filesystem
    in_doc_not_fs = (
        (report_dataframe['item_name_rp_di'] != '') & (report_dataframe['item_name_rp'] == '') & (report_dataframe['item_name_hm'] == '')
    ) | (
        (report_dataframe['item_name_hm_di'] != '') & (report_dataframe['item_name_rp'] == '') & (report_dataframe['item_name_hm'] == '')
    )

    # Set 'In Doc Not FS' if the condition is met
    report_dataframe.loc[in_doc_not_fs, 'st_alert'] = 'In Doc Not FS'
    report_dataframe.loc[in_doc_not_fs, 'st_main'] = 'NO FS MATCH-N'

    return report_dataframe['st_alert']