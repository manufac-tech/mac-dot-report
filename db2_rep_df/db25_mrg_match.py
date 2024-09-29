import pandas as pd

from .db24_rpt_mg3_oth import write_st_alert_value
from .db26_status_config import get_status_checks_config
from .db28_term_disp import remove_consolidated_columns

def consolidate_fields(report_dataframe, field_merge_rules):
    pass
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
    pass
    return field_merge_rules

def detect_status_master(report_dataframe):
    # Get the configuration dictionary
    config = get_status_checks_config()
    
    for index, row in report_dataframe.iterrows():
        # Skip rows that have already been processed
        if report_dataframe.at[index, 'm_status_result']:
            continue
        
        for subsystem, rules in config.items():
            # Extract input fields and match logic
            match_logic = rules['match_logic'](row)
            
            # Apply the match result to the output fields
            if match_logic:
                report_dataframe.at[index, 'm_status_result'] = True
                for field, value in rules['output'].items():
                    # If value is None, do not overwrite the field
                    if value is not None:
                        if field == 'st_alert':
                            report_dataframe = write_st_alert_value(report_dataframe, index, value)
                        else:
                            report_dataframe.loc[index, field] = value
            else:
                for field, value in rules['failure_output'].items():
                    # If value is None, do not overwrite the field
                    if value is not None:
                        if field == 'st_alert':
                            report_dataframe = write_st_alert_value(report_dataframe, index, value)
                        else:
                            report_dataframe.loc[index, field] = value
    
    return report_dataframe


def resolve_fields_master(report_dataframe):
    
    config = get_resolve_fields_config() # Get the configuration dictionary for field propagation

    for index, row in report_dataframe.iterrows():
        # Initialize m_consol_result to False
        report_dataframe.at[index, 'm_consol_result'] = False

        # For each row, process each change key in the config
        for change_key, rules in config.items():
            match_logic = rules['match_logic'](row)

            # If the match logic is successful, apply the actions for both repo and home
            if match_logic:
                # Perform actions for Repo fields
                if rules['actions']['copy_name_repo'] is not None:
                    report_dataframe.at[index, 'item_name_repo'] = row[rules['actions']['copy_name_repo']]
                if rules['actions']['copy_type_repo'] is not None:
                    report_dataframe.at[index, 'item_type_repo'] = row[rules['actions']['copy_type_repo']]
                
                # Perform actions for Home fields
                if rules['actions']['copy_name_home'] is not None:
                    report_dataframe.at[index, 'item_name_home'] = row[rules['actions']['copy_name_home']]
                if rules['actions']['copy_type_home'] is not None:
                    report_dataframe.at[index, 'item_type_home'] = row[rules['actions']['copy_type_home']]

                # Copy unique_id (from repo in this case)
                if rules['actions']['copy_unique_id'] is not None:
                    report_dataframe.at[index, 'unique_id'] = row[rules['actions']['copy_unique_id']]

                # Log the change into m_consol_dict for tracking
                m_consol_dict = {
                    change_key: {
                        'source_fields': {
                            'repo_name': rules['actions']['copy_name_repo'],
                            'home_name': rules['actions']['copy_name_home']
                        },
                        'values': {
                            'repo_value': row[rules['actions']['copy_name_repo']] if rules['actions']['copy_name_repo'] is not None else None,
                            'home_value': row[rules['actions']['copy_name_home']] if rules['actions']['copy_name_home'] is not None else None
                        },
                        'actions': 'populated_repo_home'
                    }
                }
                report_dataframe.at[index, 'm_consol_dict'] = m_consol_dict
                # Set m_consol_result to True if any action is performed
                report_dataframe.at[index, 'm_consol_result'] = True

    return report_dataframe

def get_resolve_fields_config():
    return {
        # Typical Match Case - Repo and Home names and types match
        'merge_act01_set_primary_fields_typical': {
            'input_fields': {
                'repo_name': 'item_name_rp',
                'home_name': 'item_name_hm',
                'repo_type': 'item_type_rp',
                'home_type': 'item_type_hm'
            },
            'match_logic': lambda row: (
                not pd.isna(row['item_name_rp']) and
                not pd.isna(row['item_name_hm']) and
                row['item_name_rp'] == row['item_name_hm'] and
                ((row['item_type_rp'] in ['file', 'file_alias'] and row['item_type_hm'] == 'file_sym') or
                 (row['item_type_rp'] in ['folder', 'folder_alias'] and row['item_type_hm'] == 'folder_sym'))
            ),
            'actions': {
                'copy_name_repo': 'item_name_rp',
                'copy_type_repo': 'item_type_rp',
                'copy_name_home': 'item_name_hm',
                'copy_type_home': 'item_type_hm',
                'copy_unique_id': 'unique_id_rp',
            },
            'status_update': None  # No status updates in this case
        },

        # Repo Only Case
        'merge_act02_set_repo_only': {
            'input_fields': {
                'repo_name': 'item_name_rp',
                'repo_type': 'item_type_rp',
                'home_name': 'item_name_hm',
                'home_type': 'item_type_hm'
            },
            'match_logic': lambda row: (
                not pd.isna(row['item_name_rp']) and
                not pd.isna(row['item_type_rp']) and
                pd.isna(row['item_name_hm']) and
                pd.isna(row['item_type_hm'])
            ),
            'actions': {
                'copy_name_repo': 'item_name_rp',  # Copy Repo name to item_name_repo
                'copy_type_repo': 'item_type_rp',  # Copy Repo type to item_type_repo
                'copy_name_home': None,
                'copy_type_home': None,
                'copy_unique_id': 'unique_id_rp',  # Copy Repo unique_id to unique_id
            },
            'status_update': None  # No status updates in this case
        },

        # Home Only Case
        'merge_act03_set_home_only': {
            'input_fields': {
                'repo_name': 'item_name_rp',
                'repo_type': 'item_type_rp',
                'home_name': 'item_name_hm',
                'home_type': 'item_type_hm'
            },
            'match_logic': lambda row: (
                pd.isna(row['item_name_rp']) and
                pd.isna(row['item_type_rp']) and
                not pd.isna(row['item_name_hm']) and
                not pd.isna(row['item_type_hm'])
            ),
            'actions': {
                'copy_name_repo': None,
                'copy_type_repo': None,
                'copy_name_home': 'item_name_hm',
                'copy_type_home': 'item_type_hm',
                'copy_unique_id': 'unique_id_hm'
            },
            'status_update': None
        },
    }