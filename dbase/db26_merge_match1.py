import pandas as pd

from .db27_merge_match2 import (subsystem_docs, subsystem_db_all, alert_sym_overwrite, alert_in_doc_not_fs)


def field_match_master(report_dataframe):
    """
    Perform full matching on the report_dataframe by calling various subsystems.
    Updates the DataFrame with status columns: dot_struc, st_docs, st_db_all, st_alert.
    """
    try:
        # Perform structure matches (full match, home only, repo only)
        report_dataframe['dot_struc'] = dot_structure_status(report_dataframe)
    except Exception as e:
        print(f"Error in dot_structure_status: {e}")
        report_dataframe['dot_struc'] = ''

    try:
        # Perform CSV/YAML match
        report_dataframe['st_docs'] = subsystem_docs(report_dataframe)
    except Exception as e:
        print(f"Error in subsystem_docs: {e}")
        report_dataframe['st_docs'] = ''

    try:
        # Perform DotBot All status
        report_dataframe['st_db_all'] = subsystem_db_all(report_dataframe)
    except Exception as e:
        print(f"Error in subsystem_db_all: {e}")
        report_dataframe['st_db_all'] = ''

    try:
        # Perform sym overwrite alert
        report_dataframe['st_alert'] = alert_sym_overwrite(report_dataframe)
    except Exception as e:
        print(f"Error in alert_sym_overwrite: {e}")
        report_dataframe['st_alert'] = ''

    try:
        # Perform alert for entries in docs but not in filesystem
        report_dataframe['st_alert'] = alert_in_doc_not_fs(report_dataframe)
    except Exception as e:
        print(f"Error in alert_in_doc_not_fs: {e}")
        report_dataframe['st_alert'] = ''
    
    return report_dataframe

def check_full_match(report_dataframe, valid_types_repo, valid_types_home):
    """
    Check for full matches between repo and home items.
    """
    report_dataframe.loc[
        (report_dataframe['item_name_hm'] != '') & 
        (report_dataframe['item_name_rp'] != '') & 
        (
            ((report_dataframe['item_type_rp'].isin(valid_types_repo['file'])) & (report_dataframe['item_type_hm'] == valid_types_home['file'])) |
            ((report_dataframe['item_type_rp'].isin(valid_types_repo['folder'])) & (report_dataframe['item_type_hm'] == valid_types_home['folder']))
        ), 
        'dot_struc'
    ] = 'rp>hm'
    return report_dataframe

def check_home_repo_only(report_dataframe):
    """
    Check for items that are only in the repo or only in the home.
    """
    # Repo-only logic
    report_dataframe.loc[(report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] != ''), 'dot_struc'] = 'rp_only'

    # Home-only logic
    report_dataframe.loc[(report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == ''), 'dot_struc'] = 'hm_only'
    
    return report_dataframe

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
    return report_dataframe

def dot_structure_status(report_dataframe):
    """
    Determine the structure status of items in the report_dataframe.
    """
    # Define valid types for repo and home
    valid_types_repo = {
        'file': ['file', 'file_alias'],
        'folder': ['folder', 'folder_alias']
    }
    valid_types_home = {
        'file': 'file_sym',
        'folder': 'folder_sym'
    }

    # Perform checks
    report_dataframe = check_full_match(report_dataframe, valid_types_repo, valid_types_home)
    report_dataframe = check_home_repo_only(report_dataframe)
    report_dataframe = check_no_fs_match(report_dataframe, valid_types_repo, valid_types_home)

    return report_dataframe['dot_struc']