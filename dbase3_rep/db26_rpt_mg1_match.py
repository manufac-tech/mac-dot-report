import pandas as pd

from .db27_rpt_mg2 import (check_no_fs_match, subsystem_docs, subsystem_db_all, alert_sym_overwrite, alert_in_doc_not_fs)
from .db28_rpt_mg3 import write_st_alert_value


def field_match_master(report_dataframe):
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
        report_dataframe = alert_sym_overwrite(report_dataframe)
    except Exception as e:
        print(f"Error in alert_sym_overwrite: {e}")

    try:
        # Perform alert for entries in docs but not in filesystem
        report_dataframe = alert_in_doc_not_fs(report_dataframe)
    except Exception as e:
        print(f"Error in alert_in_doc_not_fs: {e}")

    try:
        # Check for repo-only items
        report_dataframe = check_repo_only(report_dataframe)
    except Exception as e:
        print(f"Error in check_repo_only: {e}")

    try:
        # Check for home-only items and update st_alert field
        report_dataframe = check_home_only(report_dataframe)
    except Exception as e:
        print(f"Error in check_home_only: {e}")

    return report_dataframe


def check_full_match(report_dataframe, valid_types_repo, valid_types_home):
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

def check_repo_only(report_dataframe):
    # Repo-only logic
    repo_only_condition = (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] != '')
    report_dataframe.loc[repo_only_condition, 'dot_struc'] = 'rp_only'
    
    # Update st_alert field for repo-only items
    report_dataframe.loc[repo_only_condition, 'st_alert'] = 'Repo Only'
    
    return report_dataframe

def check_home_only(report_dataframe):
    # Home-only logic
    home_only_condition = (report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == '')
    report_dataframe.loc[home_only_condition, 'dot_struc'] = 'hm_only'
    
    
    # Update st_alert field for home-only items
    for index, row in report_dataframe[home_only_condition].iterrows():
        if ((row['item_name_hm'] == row['item_name_hm_di']) and 
            (row['item_type_hm'] == row['item_type_hm_di'])):
            report_dataframe.at[index, 'st_alert'] = 'Home Only'
        else:
            report_dataframe.at[index, 'st_alert'] = 'New Home Item'
    
    return report_dataframe

def dot_structure_status(report_dataframe):
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
    report_dataframe = check_repo_only(report_dataframe)
    report_dataframe = check_home_only(report_dataframe)
    report_dataframe = check_no_fs_match(report_dataframe, valid_types_repo, valid_types_home)

    return report_dataframe['dot_struc']