import pandas as pd

from db5_global.db52_dtype_dict import f_types_vals, get_valid_item_types

from .db23_rpt_mg2_alert import (
    field_match_2_alert, fm_fm_alert_sym_overwrite, check_name_consistency, doc_no_fs_merge_logic,
    check_doc_names_no_fs,
)
from .db24_rpt_mg3_oth import write_st_alert_value, field_match_3_subsys
from .db25_mrg_match import get_field_merge_rules
from .db26_status import detect_status_master
# resolve_fields_master


# from db5_global.db52_dtype_dict.py import get_valid_item_types

def field_match_master(report_dataframe): # TO DELETE
    field_merge_rules_dyna = {}  # Initialize dynamic conditions dictionary

    # report_dataframe = field_match_1_structure(report_dataframe)
    # report_dataframe = field_match_2_alert(report_dataframe, field_merge_rules_dyna)
    # report_dataframe = field_match_3_subsys(report_dataframe)
    
    # report_dataframe = check_doc_names_no_fs(report_dataframe, field_merge_rules_dyna)

    # report_dataframe = detect_status_master(report_dataframe, status_checks_config)


    field_merge_rules = get_field_merge_rules(report_dataframe, field_merge_rules_dyna)

    return report_dataframe, field_merge_rules


def field_match_1_structure(report_dataframe):
    # valid_types_repo, valid_types_home = get_valid_item_types()

    # # Confirm Full System Match. rp: Items, hm: Symlinks, and both YAML & CSV matching FS rp/hm.
    # report_dataframe = check_full_match(report_dataframe, valid_types_repo, valid_types_home)

    # try:  # Check for Repo-only items
    #     report_dataframe = check_repo_only(report_dataframe)
    # except Exception as e:
    #     print(f"Error in check_repo_only: {e}")

    # try:  # Check for Home-only items. If New Home Item (ALERT).
    #     report_dataframe = check_home_only(report_dataframe)
    # except Exception as e:
    #     print(f"Error in check_home_only: {e}")


    return report_dataframe


def check_full_match(report_dataframe, valid_types_repo, valid_types_home):
    # report_dataframe.loc[
    #     (report_dataframe['item_name_hm'] != '') &
    #     (report_dataframe['item_name_rp'] != '') &
    #     (
    #         ((report_dataframe['item_type_rp'].isin(valid_types_repo['file'])) & (report_dataframe['item_type_hm'] == valid_types_home['file'])) |
    #         ((report_dataframe['item_type_rp'].isin(valid_types_repo['folder'])) & (report_dataframe['item_type_hm'] == valid_types_home['folder']))
    #     ),
    #     'dot_struc'
    # ] = 'rp>hm'
    return report_dataframe


def check_repo_only(report_dataframe):
    # # Repo-only logic
    # repo_only_condition = (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] != '')
    # report_dataframe.loc[repo_only_condition, 'dot_struc'] = 'rp'

    return report_dataframe


def check_home_only(report_dataframe):
    # # Home-only logic
    # home_only_condition = (report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == '')
    # report_dataframe.loc[home_only_condition, 'dot_struc'] = 'hm'

    # # Update st_alert field for home-only items
    # for index, row in report_dataframe[home_only_condition].iterrows():
    #     if not ((row['item_name_hm'] == row['item_name_hm_cf']) and
    #             (row['item_type_hm'] == row['item_type_hm_cf'])):
    #         report_dataframe = write_st_alert_value(report_dataframe, index, 'New Home Item')

    return report_dataframe


