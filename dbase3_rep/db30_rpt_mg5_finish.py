import pandas as pd

from .db31_rpt_mg6_fsup import check_repo_only, check_home_only, remove_consolidated_columns

def consolidate_fields(report_dataframe):
    """
    Consolidates item_name_repo, item_type_repo, item_name_home, item_type_home, and unique_id based on match statuses.
    Sets 'st_misc' to 'x' if any unique ID gets copied to the actual unique_id.
    """

    # Get the conditions and corresponding actions
    conditions_actions = get_conditions_actions(report_dataframe)

    # blank_symbol = 'x'
    # blank_symbol = '|____'
    # blank_symbol = '_____'
    blank_symbol = ''
    # blank_symbol = '-'
    # Apply the conditions and actions
    for key, value in conditions_actions.items():
        condition = value['condition']
        actions = value['actions']
        for target_field, source_field in actions.items():
            if target_field == 'sort_out':
                report_dataframe.loc[condition, target_field] = source_field
            elif source_field is not None:
                report_dataframe.loc[condition, target_field] = report_dataframe[source_field]
            else:
                report_dataframe.loc[condition, target_field] = blank_symbol

    # Call the new function to remove unnecessary columns
    report_dataframe = remove_consolidated_columns(report_dataframe)

    return report_dataframe

def get_conditions_actions(report_dataframe): # Match status read from status fields, and acted upon
    return {
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
        },
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
        },
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
        },
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
        },
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
        },
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
        },
        'In Doc Not FS (dotbot.yaml)': {
            'condition': report_dataframe['st_alert'] == 'In Doc Not FS (dotbot.yaml)',
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


# def check_repo_only(report_dataframe):
#     # Repo-only logic
#     repo_only_condition = (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] != '')
#     report_dataframe.loc[repo_only_condition, 'dot_struc'] = 'rp_only'
    
#     # Debug print for repo-only condition
#     print("🟨 Repo-only items:")
#     print(report_dataframe[repo_only_condition][['item_name_hm', 'item_name_rp', 'dot_struc']])
    
#     # Update st_alert field for repo-only items
#     report_dataframe.loc[repo_only_condition, 'st_alert'] = 'Repo Only'
    
#     return report_dataframe

# def check_home_only(report_dataframe):
#     # Home-only logic
#     home_only_condition = (report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == '')
#     report_dataframe.loc[home_only_condition, 'dot_struc'] = 'hm_only'
    
#     # Debug print for home-only condition
#     print("Home-only items:")
#     print(report_dataframe[home_only_condition][['item_name_hm', 'item_name_rp', 'dot_struc']])
    
#     # Update st_alert field for home-only items
#     for index, row in report_dataframe[home_only_condition].iterrows():
#         if ((row['item_name_hm'] == row['item_name_hm_di']) and 
#             (row['item_type_hm'] == row['item_type_hm_di'])):
#             report_dataframe.at[index, 'st_alert'] = 'Home Only'
#         else:
#             report_dataframe.at[index, 'st_alert'] = 'New Home Item'
    
#     return report_dataframe

    

# def remove_consolidated_columns(report_dataframe):
#     # Remove extra unique_id fields
#     columns_to_remove = ['unique_id_rp', 'unique_id_hm', 'unique_id_db', 'unique_id_di']
#     columns_to_remove = [col for col in columns_to_remove if col in report_dataframe.columns]
#     report_dataframe.drop(columns=columns_to_remove, inplace=True)

#     # Remove source fields for name and type
#     columns_to_remove = [
#         'item_name_rp', 'item_type_rp', 'item_name_hm', 'item_type_hm',
#         'item_name_rp_db', 'item_type_rp_db', 'item_name_hm_db', 'item_type_hm_db',
#         'item_name_rp_di', 'item_type_rp_di', 'item_name_hm_di', 'item_type_hm_di',
#         'item_name', 'item_type'
#     ]
#     columns_to_remove = [col for col in columns_to_remove if col in report_dataframe.columns]
#     report_dataframe.drop(columns=columns_to_remove, inplace=True)

#     return report_dataframe