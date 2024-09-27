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
        for subsystem, rules in config.items():
            # Extract input fields and match logic
            match_logic = rules['match_logic'](row)
            
            # Apply the match result to the output fields
            if match_logic:
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
    # Get the configuration dictionary for field propagation
    config = get_resolve_fields_config()

    for index, row in report_dataframe.iterrows():
        # For each row, process each change key in the config
        for change_key, rules in config.items():
            match_logic = rules['match_logic'](row)

            # If the match logic is successful, apply the actions
            if match_logic:
                # Perform actions as defined in the configuration
                report_dataframe.at[index, 'item_name_repo'] = row[rules['actions']['copy_name']]
                report_dataframe.at[index, 'item_type_repo'] = row[rules['actions']['copy_type']]
                report_dataframe.at[index, 'unique_id'] = row[rules['actions']['copy_unique_id']]

                # Log the change into match_dict for tracking
                match_dict = {
                    change_key: {
                        'source_field': rules['actions']['copy_name'],
                        'value': row[rules['actions']['copy_name']],
                        'action': 'populated_repo'
                    }
                }
                report_dataframe.at[index, 'match_dict'] = match_dict

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
                row['item_name_rp'] == row['item_name_hm'] and
                ((row['item_type_rp'] in ['file', 'file_alias'] and row['item_type_hm'] == 'file_sym') or
                 (row['item_type_rp'] in ['folder', 'folder_alias'] and row['item_type_hm'] == 'folder_sym'))
            ),
            'actions': {
                'copy_name': 'item_name_rp',  # Copy Repo name to item_name_repo
                'copy_type': 'item_type_rp',  # Copy Repo type to item_type_repo
                'copy_unique_id': 'unique_id_rp',  # Copy Repo unique_id to unique_id
            },
            'status_update': None  # No status updates in this case
        }
    }
