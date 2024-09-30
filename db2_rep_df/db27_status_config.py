import pandas as pd

def get_status_checks_config():
    return {
        # Full Match Criteria
        'full_match': {
            'input_fields': ['item_name_rp_db', 'item_name_hm_db', 'item_name_rp_cf', 'item_name_hm_cf', 'item_name_rp', 'item_name_hm', 'item_type_rp', 'item_type_hm'],
            'match_logic': lambda row: (
                (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp_cf']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp_cf'] == row['item_name_rp'])
            ) and (
                (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm_cf']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm_cf'] == row['item_name_hm'])
            ) and (
                (row['item_type_rp'] in ['file', 'file_alias'] and row['item_type_hm'] == 'file_sym') or
                (row['item_type_rp'] in ['folder', 'folder_alias'] and row['item_type_hm'] == 'folder_sym')
            ),
            'output': {
                'st_alert': None,
                'dot_struc': 'rp>hm'  # Match case
            },
            'failure_output': {
                'st_alert': None,
                'dot_struc': None
            }
        },

        # Repo Only
        'repo_only': {
            'input_fields': ['item_name_rp_db', 'item_name_rp_cf', 'item_name_rp'],
            'match_logic': lambda row: (
                (pd.notna(row['item_name_rp_db']) or pd.notna(row['item_name_rp_cf']) or pd.notna(row['item_name_rp']))
            ),
            'output': {
                'st_alert': None,
                'dot_struc': 'rp'
            },
            'failure_output': {
                'st_alert': None,
                'dot_struc': None
            }
        },

        # # Home Only
        # 'home_only': {
        #     'input_fields': ['item_name_hm_db', 'item_name_hm_cf', 'item_name_hm'],
        #     'match_logic': lambda row: (
        #         (pd.notna(row['item_name_hm_db']) or pd.notna(row['item_name_hm_cf']) or pd.notna(row['item_name_hm']))
        #     ),
        #     'output': {
        #         'st_alert': None,
        #         'dot_struc': 'hm'
        #     },
        #     'failure_output': {
        #         'st_alert': None,
        #         'dot_struc': None
        #     }
        # },

        # # Subsystem - DotBot
        # 'subsys_dotbot': {
        #     'input_fields': ['item_name_rp_db', 'item_name_hm_db', 'item_name_rp', 'item_name_hm'],
        #     'match_logic': lambda row: (
        #         (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp'])) or
        #         (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp'])
        #     ) and (
        #         (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm'])) or
        #         (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm'])
        #     ),
        #     'output': {
        #         'st_db_all': '-',  # Success case
        #         'st_alert': None,  # No alert if matched
        #         'dot_struc': 'rp>hm'  # Match case
        #     },
        #     'failure_output': {
        #         'st_db_all': 'n',  # Failure case
        #         # 'st_alert': 'DotBot mismatch',
        #         'st_alert': None,
        #         'dot_struc': None  # No match case
        #     }
        # },

        # # Subsystem - Docs (Checks if DotBot yaml and dotrep_config.csv match each other)
        # 'subsys_docs': {
        #     'input_fields': ['item_name_rp_db', 'item_name_rp_cf', 'item_name_hm_db', 'item_name_hm_cf'],
        #     'match_logic': lambda row: (
        #         (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp_cf'])) or
        #         (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp_cf']) and row['item_name_rp_db'] == row['item_name_rp_cf'])
        #     ) and (
        #         (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm_cf'])) or
        #         (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm_cf']) and row['item_name_hm_db'] == row['item_name_hm_cf'])
        #     ),
        #     'output': {
        #         'st_docs': '-',  # Success case
        #         'st_alert': None,  # No alert if matched
        #         'dot_struc': 'rp>hm'  # Match case
        #     },
        #     'failure_output': {
        #         'st_docs': 'n',  # Failure case
        #         # 'st_alert': 'Docs mismatch',
        #         'dot_struc': None  # No match case
        #     }
        # },

        # # Alert - Doc but no FS (Checks if any name exists in the document but not in the file system)
        # 'alert_doc_no_fs': {
        #     'input_fields': ['item_name_rp_db', 'item_name_hm_db', 'item_name_rp_cf', 'item_name_hm_cf', 'item_name_rp', 'item_name_hm'],
        #     'match_logic': lambda row: (
        #         any([pd.notna(row['item_name_rp_db']), pd.notna(row['item_name_hm_db']), pd.notna(row['item_name_rp_cf']), pd.notna(row['item_name_hm_cf'])]) and
        #         pd.isna(row['item_name_rp']) and pd.isna(row['item_name_hm'])
        #     ),
        #     'output': {
        #         'st_misc': 'doc_no_fs',  # Match case
        #         'st_alert': None,
        #         'dot_struc': None
        #     },
        #     'failure_output': {
        #         'st_misc': None,
        #         'st_alert': None,
        #         'dot_struc': None
        #     }
        # }
    }