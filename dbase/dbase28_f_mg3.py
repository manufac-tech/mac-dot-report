import pandas as pd

def consolidate_fields(report_dataframe):
    """
    Consolidates item_name_repo, item_type_repo, item_name_home, item_type_home, and unique_id based on match statuses.
    Sets 'st_misc' to 'D' for each match where the target fields are updated with correct data.
    """

    # Full Match case
    full_match_condition = report_dataframe['st_main'] == 'Full Match'
    report_dataframe.loc[full_match_condition, 'item_name_repo'] = report_dataframe['item_name_rp']
    report_dataframe.loc[full_match_condition, 'item_type_repo'] = report_dataframe['item_type_rp']
    report_dataframe.loc[full_match_condition, 'item_name_home'] = report_dataframe['item_name_hm']
    report_dataframe.loc[full_match_condition, 'item_type_home'] = report_dataframe['item_type_hm']
    report_dataframe.loc[full_match_condition, 'unique_id'] = report_dataframe['unique_id_rp']
    report_dataframe.loc[full_match_condition, 'st_misc'] = 'D'  # Mark as updated with correct data

    # Home-only case
    home_only_condition = report_dataframe['st_main'] == 'Home-only'
    report_dataframe.loc[home_only_condition, 'item_name_home'] = report_dataframe['item_name_hm']
    report_dataframe.loc[home_only_condition, 'item_type_home'] = report_dataframe['item_type_hm']
    report_dataframe.loc[home_only_condition, 'item_name_repo'] = None
    report_dataframe.loc[home_only_condition, 'item_type_repo'] = None
    report_dataframe.loc[home_only_condition, 'unique_id'] = report_dataframe['unique_id_hm']
    report_dataframe.loc[home_only_condition, 'st_misc'] = 'D'  # Mark as updated with correct data

    # Repo-only case
    repo_only_condition = report_dataframe['st_main'] == 'Repo-only'
    report_dataframe.loc[repo_only_condition, 'item_name_repo'] = report_dataframe['item_name_rp']
    report_dataframe.loc[repo_only_condition, 'item_type_repo'] = report_dataframe['item_type_rp']
    report_dataframe.loc[repo_only_condition, 'item_name_home'] = None
    report_dataframe.loc[repo_only_condition, 'item_type_home'] = None
    report_dataframe.loc[repo_only_condition, 'unique_id'] = report_dataframe['unique_id_rp']
    report_dataframe.loc[repo_only_condition, 'st_misc'] = 'D'  # Mark as updated with correct data

    # SymLink Overwrite case
    sym_overwrite_condition = report_dataframe['st_alert'] == 'SymLink Overwrite'
    report_dataframe.loc[sym_overwrite_condition, 'item_name_repo'] = report_dataframe['item_name_rp']
    report_dataframe.loc[sym_overwrite_condition, 'item_type_repo'] = report_dataframe['item_type_rp']
    report_dataframe.loc[sym_overwrite_condition, 'item_name_home'] = report_dataframe['item_name_hm']
    report_dataframe.loc[sym_overwrite_condition, 'item_type_home'] = report_dataframe['item_type_hm']
    report_dataframe.loc[sym_overwrite_condition, 'unique_id'] = report_dataframe['unique_id_rp']
    report_dataframe.loc[sym_overwrite_condition, 'st_misc'] = 'D'  # Mark as updated with correct data

    # New Home Item case
    new_home_item_condition = report_dataframe['st_alert'] == 'New Home Item'
    report_dataframe.loc[new_home_item_condition, 'item_name_home'] = report_dataframe['item_name_hm']
    report_dataframe.loc[new_home_item_condition, 'item_type_home'] = report_dataframe['item_type_hm']
    report_dataframe.loc[new_home_item_condition, 'item_name_repo'] = None
    report_dataframe.loc[new_home_item_condition, 'item_type_repo'] = None
    report_dataframe.loc[new_home_item_condition, 'unique_id'] = report_dataframe['unique_id_hm']
    report_dataframe.loc[new_home_item_condition, 'st_misc'] = 'D'  # Mark as updated with correct data

    # In Doc Not FS case
    in_doc_not_fs_condition = report_dataframe['st_alert'] == 'In Doc Not FS'

    # Check if item exists in dotbot.yaml but not in the file system
    in_doc_not_fs_db_condition = in_doc_not_fs_condition & report_dataframe['item_name_rp_db'].notna()
    report_dataframe.loc[in_doc_not_fs_db_condition, 'item_name_repo'] = report_dataframe['item_name_rp_db']
    report_dataframe.loc[in_doc_not_fs_db_condition, 'item_type_repo'] = report_dataframe['item_type_rp_db']
    report_dataframe.loc[in_doc_not_fs_db_condition, 'item_name_home'] = report_dataframe['item_name_hm_db']
    report_dataframe.loc[in_doc_not_fs_db_condition, 'item_type_home'] = report_dataframe['item_type_hm_db']
    report_dataframe.loc[in_doc_not_fs_db_condition, 'unique_id'] = report_dataframe['unique_id_db']

    # Check if item exists in dot-info.csv but not in the file system
    in_doc_not_fs_di_condition = in_doc_not_fs_condition & report_dataframe['item_name_rp_di'].notna()
    report_dataframe.loc[in_doc_not_fs_di_condition, 'item_name_repo'] = report_dataframe['item_name_rp_di']
    report_dataframe.loc[in_doc_not_fs_di_condition, 'item_type_repo'] = report_dataframe['item_type_rp_di']
    report_dataframe.loc[in_doc_not_fs_di_condition, 'item_name_home'] = report_dataframe['item_name_hm_di']
    report_dataframe.loc[in_doc_not_fs_di_condition, 'item_type_home'] = report_dataframe['item_type_hm_di']
    report_dataframe.loc[in_doc_not_fs_di_condition, 'unique_id'] = report_dataframe['unique_id_di']

    report_dataframe.loc[in_doc_not_fs_condition, 'st_misc'] = 'D'  # Mark as updated with correct data

    return report_dataframe