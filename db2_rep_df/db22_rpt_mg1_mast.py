import pandas as pd

from db5_global.db52_dtype_dict import f_types_vals, get_valid_item_types

from .db23_rpt_mg2_alert import (
    field_match_2_alert, fm_fm_alert_sym_overwrite, check_name_consistency, doc_no_fs_merge_logic,
    check_doc_names_no_fs,
)
from .db24_rpt_mg3_oth import write_st_alert_value, field_match_3_subsys
from .db25_rpt_mg5_finish import get_field_merge_rules, detect_status_master
# resolve_fields_master


# from db5_global.db52_dtype_dict.py import get_valid_item_types

def field_match_master(report_dataframe):
    field_merge_rules_dyna = {}  # Initialize dynamic conditions dictionary

    # report_dataframe = field_match_1_structure(report_dataframe)
    # report_dataframe = field_match_2_alert(report_dataframe, field_merge_rules_dyna)
    # report_dataframe = field_match_3_subsys(report_dataframe)
    
    # report_dataframe = check_doc_names_no_fs(report_dataframe, field_merge_rules_dyna)

    # report_dataframe = detect_status_master(report_dataframe, status_checks_config)


    field_merge_rules = get_field_merge_rules(report_dataframe, field_merge_rules_dyna)

    return report_dataframe, field_merge_rules


def field_match_1_structure(report_dataframe):
    valid_types_repo, valid_types_home = get_valid_item_types()

    # Confirm Full System Match. rp: Items, hm: Symlinks, and both YAML & CSV matching FS rp/hm.
    report_dataframe = check_full_match(report_dataframe, valid_types_repo, valid_types_home)

    try:  # Check for Repo-only items
        report_dataframe = check_repo_only(report_dataframe)
    except Exception as e:
        print(f"Error in check_repo_only: {e}")

    try:  # Check for Home-only items. If New Home Item (ALERT).
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
    report_dataframe.loc[repo_only_condition, 'dot_struc'] = 'rp'

    return report_dataframe


def check_home_only(report_dataframe):
    # Home-only logic
    home_only_condition = (report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == '')
    report_dataframe.loc[home_only_condition, 'dot_struc'] = 'hm'

    # Update st_alert field for home-only items
    for index, row in report_dataframe[home_only_condition].iterrows():
        if not ((row['item_name_hm'] == row['item_name_hm_cf']) and
                (row['item_type_hm'] == row['item_type_hm_cf'])):
            report_dataframe = write_st_alert_value(report_dataframe, index, 'New Home Item')

    return report_dataframe


# def detect_status_master(report_dataframe, config):
#     for index, row in report_dataframe.iterrows():
#         # Apply the DotBot status check based on the configuration
#         dotbot_rule = config['subsys_dotbot']
#         match_logic = dotbot_rule['match_logic']
        
#         # Debugging: Print the row being processed
#         print(f"Processing row {index}: {row.to_dict()}")
        
#         if match_logic(row):
#             row['st_db_all'] = dotbot_rule['output']['st_db_all']
#             row['st_alert'] = dotbot_rule['output']['st_alert']
#             # Debugging: Print the match result
#             print(f"Row {index} matched: st_db_all set to {row['st_db_all']}")
#         else:
#             row['st_db_all'] = 'x'
#             row['st_alert'] = 'DotBot mismatch'
#             # Debugging: Print the mismatch result
#             print(f"Row {index} did not match: st_db_all set to {row['st_db_all']}")
    
#     return report_dataframe

# status_checks_config = {
#     'subsys_dotbot': {
#         'input_fields': ['item_name_rp_db', 'item_name_hm_db', 'item_name_rp', 'item_name_hm'],
#         'match_logic': lambda row: (
#             (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp'])) or
#             (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp'])
#         ) and (
#             (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm'])) or
#             (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm'])
#         ),
#         'output': {
#             'st_db_all': 'o',  # Success case
#             'st_alert': None   # No alert if matched
#         }
#     }
# }