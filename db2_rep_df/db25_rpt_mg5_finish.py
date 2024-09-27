import pandas as pd

from .db24_rpt_mg3_oth import write_st_alert_value
from .db26_rpt_mg6_fsup import remove_consolidated_columns

def consolidate_fields(report_dataframe, field_merge_rules):
    # Apply field merge rules to update DataFrame based on conditions and actions
    # for key, value in field_merge_rules.items():
    #     condition = value['condition']
    #     actions = value['actions']
    #     for target_field, source_field in actions.items():
    #         if target_field == 'sort_out':
    #             report_dataframe.loc[condition, target_field] = source_field
    #         elif target_field == 'st_alert':
    #             report_dataframe.loc[condition, target_field] = source_field
    #         elif source_field is not None:
    #             report_dataframe.loc[condition, target_field] = report_dataframe[source_field]
    #         else:
    #             report_dataframe.loc[condition, target_field] = pd.NA

    # TEMP: Ensure blank item names are filled with appropriate values
    # report_dataframe = fix_blank_item_names(report_dataframe)

    # Remove unnecessary columns after consolidation
    # report_dataframe = remove_consolidated_columns(report_dataframe)
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
    field_merge_rules = {
        # 'full_match': {
        #     'condition': report_dataframe['dot_struc'] == 'rp>hm',
        #     'actions': {
        #         'item_name_repo': 'item_name_rp',
        #         'item_type_repo': 'item_type_rp',
        #         'item_name_home': 'item_name_hm',
        #         'item_type_home': 'item_type_hm',
        #         'unique_id': 'unique_id_rp',
        #         'sort_out': 30,
        #         # 'st_alert': 'Full Match'
        #     }
        # },
        # 'full_match_not_tracked_by_git': {
        #     'condition': (report_dataframe['dot_struc'] == 'rp>hm') & (report_dataframe['git_rp'] == False),
        #     'actions': {
        #         'item_name_repo': 'item_name_rp',
        #         'item_type_repo': 'item_type_rp',
        #         'item_name_home': 'item_name_hm',
        #         'item_type_home': 'item_type_hm',
        #         'unique_id': 'unique_id_rp',
        #         'sort_out': 35,
        #         'st_alert': 'Full Match Not Tracked by Git'
        #     }
        # },
        # 'repo_only': {
        #     'condition': report_dataframe['dot_struc'] == 'rp',
        #     'actions': {
        #         'item_name_repo': 'item_name_rp',
        #         'item_type_repo': 'item_type_rp',
        #         'item_name_home': None,
        #         'item_type_home': None,
        #         'unique_id': 'unique_id_rp',
        #         'sort_out': 15,
        #         # 'st_alert': 'Repo Only'
        #     }
        # },
        # 'home_only': {
        #     'condition': report_dataframe['dot_struc'] == 'hm',
        #     'actions': {
        #         'item_name_home': 'item_name_hm',
        #         'item_type_home': 'item_type_hm',
        #         'item_name_repo': None,
        #         'item_type_repo': None,
        #         'unique_id': 'unique_id_hm',
        #         'sort_out': 11,
        #         # 'st_alert': 'Home Only'
        #     }
        # },
        # 'new_home_item': {
        #     'condition': report_dataframe['st_alert'] == 'New Home Item',
        #     'actions': {
        #         'item_name_home': 'item_name_hm',
        #         'item_type_home': 'item_type_hm',
        #         'item_name_repo': None,
        #         'item_type_repo': None,
        #         'unique_id': 'unique_id_hm',
        #         'sort_out': 20,
        #         'st_alert': 'New Home Item'
        #     }
        # },
        # 'symlink_overwrite': {
        #     'condition': report_dataframe['st_alert'] == 'SymLink Overwrite',
        #     'actions': {
        #         'item_name_repo': 'item_name_rp',
        #         'item_type_repo': 'item_type_rp',
        #         'item_name_home': 'item_name_hm',
        #         'item_type_home': 'item_type_hm',
        #         'unique_id': 'unique_id_rp',
        #         'sort_out': 16,
        #         'st_alert': 'SymLink Overwrite'
        #     }
        # },

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


def get_status_checks_config():
    return {
        # Subsystem - DotBot
        'subsys_dotbot': {
            'input_fields': ['item_name_rp_db', 'item_name_hm_db', 'item_name_rp', 'item_name_hm'],
            'match_logic': lambda row: (
                (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp'])) or
                (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp'])
            ) and (
                (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm'])) or
                (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm'])
            ),
            'output': {
                'st_db_all': '-',  # Success case
                'st_alert': None   # No alert if matched
            },
            'failure_output': {
                'st_db_all': 'n',  # Failure case
                'st_alert': 'DotBot mismatch'
            }
        },

        # Subsystem - Docs (Checks if DotBot yaml and dotrep_config.csv match each other)
        'subsys_docs': {
            'input_fields': ['item_name_rp_db', 'item_name_rp_cf', 'item_name_hm_db', 'item_name_hm_cf'],
            'match_logic': lambda row: (
                (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp_cf'])) or
                (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp_cf']) and row['item_name_rp_db'] == row['item_name_rp_cf'])
            ) and (
                (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm_cf'])) or
                (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm_cf']) and row['item_name_hm_db'] == row['item_name_hm_cf'])
            ),
            'output': {
                'st_docs': '-',  # Success case
                'st_alert': None   # No alert if matched
            },
            'failure_output': {
                'st_docs': 'n',  # Failure case
                'st_alert': 'Docs mismatch'
            }
        },

        # Alert - Doc but no FS (Checks if any name exists in the document but not in the file system)
        'alert_doc_no_fs': {
            'input_fields': ['item_name_rp_db', 'item_name_hm_db', 'item_name_rp_cf', 'item_name_hm_cf', 'item_name_rp', 'item_name_hm'],
            'match_logic': lambda row: (
                any([pd.notna(row['item_name_rp_db']), pd.notna(row['item_name_hm_db']), pd.notna(row['item_name_rp_cf']), pd.notna(row['item_name_hm_cf'])]) and
                pd.isna(row['item_name_rp']) and pd.isna(row['item_name_hm'])
            ),
            'output': {
                'st_misc': 'doc_no_fs',  # Alert case
                'st_alert': 'Doc No FS'
            },
            'failure_output': {
                'st_misc': None,  # No alert needed
                'st_alert': None   # No alert if matched
            }
        }
    }

    # def resolve_fields_master(report_dataframe): 
    #     pass

