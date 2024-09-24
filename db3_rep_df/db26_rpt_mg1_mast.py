import pandas as pd

from db1_main_df.db03_dtype_dict import f_types_vals, get_valid_item_types

from .db27_rpt_mg2_alert import field_match_2_alert, alert_sym_overwrite
from .db28_rpt_mg3_oth import write_st_alert_value, field_match_3_subsys

def field_match_master(report_dataframe):
    report_dataframe = field_match_1_structure(report_dataframe)
    report_dataframe = field_match_2_alert(report_dataframe)
    report_dataframe = field_match_3_subsys(report_dataframe)

    return report_dataframe


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

    # Check for document names without corresponding file system names
    # report_dataframe = check_doc_names_no_fs(report_dataframe)

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
    print("Checking for Repo-only items...")
    # Repo-only logic
    repo_only_condition = (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] != '')
    report_dataframe.loc[repo_only_condition, 'dot_struc'] = 'rp_only'

    return report_dataframe


def check_home_only(report_dataframe):
    print("Checking for Home-only items...")
    # Home-only logic
    home_only_condition = (report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == '')
    report_dataframe.loc[home_only_condition, 'dot_struc'] = 'hm_only'

    # Update st_alert field for home-only items
    for index, row in report_dataframe[home_only_condition].iterrows():
        if not ((row['item_name_hm'] == row['item_name_hm_di']) and
                (row['item_type_hm'] == row['item_type_hm_di'])):
            report_dataframe = write_st_alert_value(report_dataframe, index, 'New Home Item')

    return report_dataframe



def get_conditions_actions(report_dataframe):  # Match status read from status fields, and acted upon
    full_match = {
        'Full Match': {
            'condition': report_dataframe['dot_struc'] == 'rp>hm',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'unique_id': 'unique_id_rp',
                'sort_out': 30
            }
        }
    }

    full_match_not_tracked_by_git = {
        'Full Match Not Tracked by Git': {
            'condition': (report_dataframe['dot_struc'] == 'rp>hm') & (report_dataframe['git_rp'] == False),
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'unique_id': 'unique_id_rp',
                'sort_out': 35
            }
        }
    }

    repo_only = {
        'Repo Only': {
            'condition': report_dataframe['dot_struc'] == 'rp_only',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': None,
                'item_type_home': None,
                'unique_id': 'unique_id_rp',
                'sort_out': 15
            }
        }
    }

    home_only = {
        'Home Only': {
            'condition': report_dataframe['dot_struc'] == 'hm_only',
            'actions': {
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'item_name_repo': None,
                'item_type_repo': None,
                'unique_id': 'unique_id_hm',
                'sort_out': 11
            }
        }
    }

    new_home_item = {
        'New Home Item': {
            'condition': report_dataframe['st_alert'] == 'New Home Item',
            'actions': {
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'item_name_repo': None,
                'item_type_repo': None,
                'unique_id': 'unique_id_hm',
                'sort_out': 20
            }
        }
    }

    symlink_overwrite = {
        'SymLink Overwrite': {
            'condition': report_dataframe['st_alert'] == 'SymLink Overwrite',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'unique_id': 'unique_id_rp',
                'sort_out': 16
            }
        }
    }

    in_doc_not_fs = {
        'In Doc Not FS': {
            'condition': report_dataframe['st_misc'] == 'doc_no_fs',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': None,
                'item_type_home': None,
                'unique_id': 'unique_id_rp',
                'sort_out': 25
            }
        }
    }

    # Combine all sections into the final dictionary
    conditions_actions = {}
    conditions_actions.update(full_match)
    conditions_actions.update(full_match_not_tracked_by_git)
    conditions_actions.update(repo_only)
    conditions_actions.update(home_only)
    conditions_actions.update(new_home_item)
    conditions_actions.update(symlink_overwrite)
    # conditions_actions.update(in_doc_not_fs)  # Uncomment this line to include the new condition

    return conditions_actions