import pandas as pd

from .db31_rpt_mg6_fsup import remove_consolidated_columns


def consolidate_fields(report_dataframe, field_merge_rules):
    """
    Consolidates item_name_repo, item_type_repo, item_name_home, item_type_home, and unique_id based on match statuses.
    Sets 'st_misc' to 'x' if any unique ID gets copied to the actual unique_id.
    """

    blank_symbol = ''

    # Apply the conditions and actions from the combined dictionary
    for key, value in field_merge_rules.items():
        condition = value['condition']
        actions = value['actions']
        for target_field, source_field in actions.items():
            if target_field == 'sort_out':
                report_dataframe.loc[condition, target_field] = source_field
            elif target_field == 'st_alert':
                report_dataframe.loc[condition, target_field] = source_field
            elif source_field is not None:
                report_dataframe.loc[condition, target_field] = report_dataframe[source_field]
            else:
                report_dataframe.loc[condition, target_field] = pd.NA

    # Call the new function to remove unnecessary columns
    report_dataframe = remove_consolidated_columns(report_dataframe)

    return report_dataframe
    

def apply_dynamic_consolidation(report_dataframe):
    """
    Applies dynamic consolidation based on the match_dict field.
    """
    for index, row in report_dataframe.iterrows():
        match_dict = row['match_dict']
        
        # Example logic for consolidating fields using match_dict column
        if match_dict.get('check_doc_names_no_fs', {}).get('item_name_rp_db', False):
            report_dataframe.at[index, 'item_name_repo'] = row['item_name_rp']
            report_dataframe.at[index, 'item_type_repo'] = row['item_type_rp']
        else:
            report_dataframe.at[index, 'item_name_repo'] = pd.NA
            report_dataframe.at[index, 'item_type_repo'] = pd.NA

        if match_dict.get('check_doc_names_no_fs', {}).get('item_name_hm_db', False):
            report_dataframe.at[index, 'item_name_home'] = row['item_name_hm']
            report_dataframe.at[index, 'item_type_home'] = row['item_type_hm']
        else:
            report_dataframe.at[index, 'item_name_home'] = pd.NA
            report_dataframe.at[index, 'item_type_home'] = pd.NA

        report_dataframe.at[index, 'unique_id'] = row['unique_id_rp']
        report_dataframe.at[index, 'sort_out'] = 25

    return report_dataframe

def get_field_merge_rules(report_dataframe, dynamic_conditions):
    field_merge_rules = {
        'full_match': {
            'condition': report_dataframe['dot_struc'] == 'rp>hm',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'unique_id': 'unique_id_rp',
                'sort_out': 30,
                # 'st_alert': 'Full Match'
            }
        },
        'full_match_not_tracked_by_git': {
            'condition': (report_dataframe['dot_struc'] == 'rp>hm') & (report_dataframe['git_rp'] == False),
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'unique_id': 'unique_id_rp',
                'sort_out': 35,
                # 'st_alert': 'Full Match Not Tracked by Git'
            }
        },
        'repo_only': {
            'condition': report_dataframe['dot_struc'] == 'rp_only',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': None,
                'item_type_home': None,
                'unique_id': 'unique_id_rp',
                'sort_out': 15,
                # 'st_alert': 'Repo Only'
            }
        },
        'home_only': {
            'condition': report_dataframe['dot_struc'] == 'hm_only',
            'actions': {
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'item_name_repo': None,
                'item_type_repo': None,
                'unique_id': 'unique_id_hm',
                'sort_out': 11,
                # 'st_alert': 'Home Only'
            }
        },
        'new_home_item': {
            'condition': report_dataframe['st_alert'] == 'New Home Item',
            'actions': {
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'item_name_repo': None,
                'item_type_repo': None,
                'unique_id': 'unique_id_hm',
                'sort_out': 20,
                # 'st_alert': 'New Home Item'
            }
        },
        'symlink_overwrite': {
            'condition': report_dataframe['st_alert'] == 'SymLink Overwrite',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'unique_id': 'unique_id_rp',
                'sort_out': 16,
                # 'st_alert': 'SymLink Overwrite'
            }
        },

        # 'in_doc_not_fs': {
        #     'condition': report_dataframe['st_misc'] == 'doc_no_fs',
        #     'actions': {
        #         'item_name_repo': 'item_name_rp',
        #         'item_type_repo': 'item_type_rp',
        #         'item_name_home': None,
        #         'item_type_home': None,
        #         'unique_id': 'unique_id_rp',
        #         'sort_out': 25,
        #         # Optional action to set st_alert field
        #         'st_alert': 'In Doc Not FS'
        #     }
        # }
    }

    return field_merge_rules

