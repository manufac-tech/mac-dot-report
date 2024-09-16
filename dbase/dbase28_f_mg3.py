import pandas as pd

def consolidate_fields(report_dataframe):
    """
    Consolidates item_name_repo, item_type_repo, item_name_home, item_type_home, and unique_id based on match statuses.
    Sets 'st_misc' to 'x' if any unique ID gets copied to the actual unique_id.
    """

    # Define the conditions and corresponding actions
    conditions_actions = {
        'Full Match': {
            'condition': report_dataframe['st_main'] == 'Full Match',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'unique_id': 'unique_id_rp'
            }
        },
        'Home-only': {
            'condition': report_dataframe['st_main'] == 'Home-only',
            'actions': {
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'item_name_repo': None,
                'item_type_repo': None,
                'unique_id': 'unique_id_hm'
            }
        },
        'Repo-only': {
            'condition': report_dataframe['st_main'] == 'Repo-only',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': None,
                'item_type_home': None,
                'unique_id': 'unique_id_rp'
            }
        },
        'SymLink Overwrite': {
            'condition': report_dataframe['st_alert'] == 'SymLink Overwrite',
            'actions': {
                'item_name_repo': 'item_name_rp',
                'item_type_repo': 'item_type_rp',
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'unique_id': 'unique_id_rp'
            }
        },
        'New Home Item': {
            'condition': report_dataframe['st_alert'] == 'New Home Item',
            'actions': {
                'item_name_home': 'item_name_hm',
                'item_type_home': 'item_type_hm',
                'item_name_repo': None,
                'item_type_repo': None,
                'unique_id': 'unique_id_hm'
            }
        },
        'In Doc Not FS (dotbot.yaml)': {
            'condition': (report_dataframe['st_alert'] == 'In Doc Not FS') & report_dataframe['item_name_rp_db'].notna(),
            'actions': {
                'item_name_repo': 'item_name_rp_db',
                'item_type_repo': 'item_type_rp_db',
                'item_name_home': 'item_name_hm_db',
                'item_type_home': 'item_type_hm_db',
                'unique_id': 'unique_id_db'
            }
        },
        'In Doc Not FS (dot-info.csv)': {
            'condition': (report_dataframe['st_alert'] == 'In Doc Not FS') & report_dataframe['item_name_rp_di'].notna(),
            'actions': {
                'item_name_repo': 'item_name_rp_di',
                'item_type_repo': 'item_type_rp_di',
                'item_name_home': 'item_name_hm_di',
                'item_type_home': 'item_type_hm_di',
                'unique_id': 'unique_id_di'
            }
        }
    }

    # Apply the conditions and actions
    for key, value in conditions_actions.items():
        condition = value['condition']
        actions = value['actions']
        for target_field, source_field in actions.items():
            if source_field is not None:
                report_dataframe.loc[condition, target_field] = report_dataframe[source_field]
            else:
                report_dataframe.loc[condition, target_field] = None
        # report_dataframe.loc[condition, 'st_misc'] = 'x'  # Mark as updated with correct data

    # Remove extra unique_id fields
    report_dataframe.drop(columns=['unique_id_rp', 'unique_id_hm', 'unique_id_db', 'unique_id_di'], inplace=True)

    # Remove source fields for name and type
    report_dataframe.drop(columns=[
        'item_name_rp', 'item_type_rp', 'item_name_hm', 'item_type_hm',
        'item_name_rp_db', 'item_type_rp_db', 'item_name_hm_db', 'item_type_hm_db',
        'item_name_rp_di', 'item_type_rp_di', 'item_name_hm_di', 'item_type_hm_di',
        'item_name', 'item_type'
    ], inplace=True)

    return report_dataframe