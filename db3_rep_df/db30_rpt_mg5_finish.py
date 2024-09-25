import pandas as pd

from .db28_rpt_mg3_oth import write_st_alert_value
from .db31_rpt_mg6_fsup import remove_consolidated_columns

def consolidate_fields(report_dataframe, field_merge_rules):
    # Apply field merge rules to update DataFrame based on conditions and actions
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

    # TEMP: Ensure blank item names are filled with appropriate values
    report_dataframe = fix_blank_item_names(report_dataframe)

    # Remove unnecessary columns after consolidation
    report_dataframe = remove_consolidated_columns(report_dataframe)

    return report_dataframe

def fix_blank_item_names(df):
    # Ensure 'item_name' column exists
    if 'item_name' not in df.columns:
        raise KeyError("'item_name' column is missing from the DataFrame")

    # Copy item_name to item_name_home and item_name_repo if they are blank
    for index, row in df.iterrows():
        if pd.isna(row['item_name_home']) or row['item_name_home'] == '':
            df.at[index, 'item_name_home'] = row['item_name']
            df = write_st_alert_value(df, index, "TMP: ITEM_NAME")
        if pd.isna(row['item_name_repo']) or row['item_name_repo'] == '':
            df.at[index, 'item_name_repo'] = row['item_name']
            df = write_st_alert_value(df, index, "TMP: ITEM_NAME")
    
    return df

def get_field_merge_rules(report_dataframe, field_merge_rules_dyna):
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

