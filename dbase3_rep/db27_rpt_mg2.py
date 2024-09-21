import pandas as pd

from .db28_rpt_mg3 import write_st_alert_value

def check_no_fs_match(report_dataframe, valid_types_repo, valid_types_home):
    """
    Check for items with no filesystem match by name or type.
    """
    # NO FS MATCH-N (Name) logic
    no_fs_match_n = (
        (report_dataframe['item_name_rp_di'] != '') & (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == '')
    ) | (
        (report_dataframe['item_name_hm_di'] != '') & (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == '')
    )
    report_dataframe.loc[no_fs_match_n, 'dot_struc'] = 'no_fs_N'

    # NO FS MATCH-T (Type) logic
    name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_hm'])
    type_mismatch = (
        (report_dataframe['item_type_rp'].isin(valid_types_repo['file']) & (report_dataframe['item_type_hm'] != valid_types_home['file'])) |
        (report_dataframe['item_type_rp'].isin(valid_types_repo['folder']) & (report_dataframe['item_type_hm'] != valid_types_home['folder']))
    )
    no_fs_match_t = name_match & type_mismatch
    report_dataframe.loc[no_fs_match_t, 'dot_struc'] = 'no_fs_T'

    # Update st_alert field with "FS-Doc Mismatch" for no_fs_match_n and no_fs_match_t
    for index in report_dataframe[no_fs_match_n | no_fs_match_t].index:
        report_dataframe = write_st_alert_value(report_dataframe, index, 'FS-Doc Mismatch')

    return report_dataframe

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
    # Check if item names match between repo and home
    name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_hm'])

    # Check if types differ in a way that suggests a symlink has been overwritten
    type_mismatch = (
        (report_dataframe['item_type_rp'] == 'file') & (report_dataframe['item_type_hm'] != 'file_sym')
    ) | (
        (report_dataframe['item_type_rp'] == 'folder') & (report_dataframe['item_type_hm'] != 'folder_sym')
    )

    # Append 'SymLink Overwrite' if names match but types differ as described above
    for index in report_dataframe[name_match & type_mismatch].index:
        report_dataframe = write_st_alert_value(report_dataframe, index, 'SymLink Overwrite')

    return report_dataframe

def alert_in_doc_not_fs(report_dataframe):
    condition = (report_dataframe['item_name_hm'].notna()) & (report_dataframe['item_name_rp'].isna())
    
    # Debug: Print the condition results
    # print("Condition (item_name_hm.notna() & item_name_rp.isna()):")
    # print(condition)
    
    for index in report_dataframe[condition].index:
        report_dataframe = write_st_alert_value(report_dataframe, index, 'New Home Item')
    
    return report_dataframe