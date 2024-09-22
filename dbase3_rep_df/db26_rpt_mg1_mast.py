import pandas as pd

from .db27_rpt_mg2_alert import (
    field_match_2_alert,
    check_no_fs_match, 
    alert_sym_overwrite, 
    alert_in_doc_not_fs
)
from .db28_rpt_mg3_oth import (
    write_st_alert_value,
    field_match_3_subsys
)

def field_match_master(report_dataframe):
    report_dataframe = field_match_1_structure(report_dataframe)
    report_dataframe = field_match_3_subsys(report_dataframe)
    report_dataframe = field_match_2_alert(report_dataframe)
    return report_dataframe


def field_match_1_structure(report_dataframe):
    # Confirm Full System Match. rp: Items, hm: Symlinks, and both YAML & CSV matching FS rp/hm.
    valid_types_repo = {'file': ['file', 'file_alias'], 'folder': ['folder', 'folder_alias']}
    valid_types_home = {'file': 'file_sym', 'folder': 'folder_sym'}

    report_dataframe = check_full_match(report_dataframe, valid_types_repo, valid_types_home)

    try: # Check for Repo-only items
        report_dataframe = check_repo_only(report_dataframe)
    except Exception as e:
        print(f"Error in check_repo_only: {e}")

    try: # Check for Home-only items. If New Home Item (ALERT).
        report_dataframe = check_home_only(report_dataframe)
    except Exception as e:
        print(f"Error in check_home_only: {e}")

    try: # Check for no file system match
        report_dataframe = check_no_fs_match(report_dataframe, valid_types_repo, valid_types_home)
    except Exception as e:
        print(f"Error in check_no_fs_match: {e}")

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
    
    return report_dataframe

def check_home_only(report_dataframe):
    # Home-only logic
    home_only_condition = (report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == '')
    report_dataframe.loc[home_only_condition, 'dot_struc'] = 'hm_only'
    
    # Update st_alert field for home-only items
    for index, row in report_dataframe[home_only_condition].iterrows():
        if not ((row['item_name_hm'] == row['item_name_hm_di']) and 
                (row['item_type_hm'] == row['item_type_hm_di'])):
            report_dataframe = write_st_alert_value(report_dataframe, index, 'New Home Item')
    
    return report_dataframe