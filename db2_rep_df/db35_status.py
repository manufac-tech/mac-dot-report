# import pandas as pd
# # from .db38_status_config import get_status_checks_config


# def detect_status_master(report_dataframe):
#     pass
# #     status_checks_config = get_status_checks_config()
# #     # print("Status Checks Configuration:", status_checks_config)  # Debug: Print the configuration

# #     for index, row in report_dataframe.iterrows():
# #         # print(f"Processing row {index}: {row.to_dict()}")  # Debug: Print the row being processed

# #         for check_name, check_config in status_checks_config.items():
# #             match_logic = check_config['match_logic']
# #             output = check_config['output']
# #             failure_output = check_config['failure_output']

# #             match_result = match_logic(row)
# #             # print(f"Check: {check_name}, Match Result: {match_result}")  # Debug: Print the match result

# #             if match_result:
# #                 dot_struc_value = output.get('dot_struc')
# #             else:
# #                 dot_struc_value = failure_output.get('dot_struc')

# #             if dot_struc_value is not None and pd.isna(report_dataframe.at[index, 'dot_struc']):
# #                 # print(f"Updating dot_struc to {dot_struc_value} for row {index}")  # Debug: Print the update
# #                 report_dataframe.at[index, 'dot_struc'] = dot_struc_value

#     return report_dataframe

# def get_status_checks_config():
#     pass
# #     return {
# #         # Full Match Criteria
# #         'full_match': {
# #             'input_fields': ['item_name_rp_db', 'item_name_hm_db', 'item_name_rp_cf', 'item_name_hm_cf', 'item_name_rp', 'item_name_hm', 'item_type_rp', 'item_type_hm'],
# #             'match_logic': lambda row: (
# #                 (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp_cf']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp_cf'] == row['item_name_rp'])
# #             ) and (
# #                 (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm_cf']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm_cf'] == row['item_name_hm'])
# #             ) and (
# #                 (row['item_type_rp'] in ['file', 'file_alias'] and row['item_type_hm'] == 'file_sym') or
# #                 (row['item_type_rp'] in ['folder', 'folder_alias'] and row['item_type_hm'] == 'folder_sym')
# #             ),
# #             'output': {
# #                 'st_alert': None,
# #                 'dot_struc': 'rp>hm'  # Match case
# #             },
# #             'failure_output': {
# #                 'st_alert': None,
# #                 'dot_struc': None
# #             }
# #         },

# #         # Repo Only
# #         'repo_only': {
# #             'input_fields': ['item_name_rp_db', 'item_name_rp_cf', 'item_name_rp'],
# #             'match_logic': lambda row: (
# #                 (pd.notna(row['item_name_rp_db']) or pd.notna(row['item_name_rp_cf']) or pd.notna(row['item_name_rp'])) and
# #                 (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm_cf']) and pd.isna(row['item_name_hm']))
# #             ),
# #             'output': {
# #                 'st_alert': None,
# #                 'dot_struc': 'rp'
# #             },
# #             'failure_output': {
# #                 'st_alert': None,
# #                 'dot_struc': None
# #             }
# #         },

# #         # Home Only
# #         'home_only': {
# #             'input_fields': ['item_name_hm_db', 'item_name_hm_cf', 'item_name_hm'],
# #             'match_logic': lambda row: (
# #                 (pd.notna(row['item_name_hm_db']) or pd.notna(row['item_name_hm_cf']) or pd.notna(row['item_name_hm'])) and
# #                 (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp_cf']) and pd.isna(row['item_name_rp']))
# #             ),
# #             'output': {
# #                 'st_alert': None,
# #                 'dot_struc': 'hm'
# #             },
# #             'failure_output': {
# #                 'st_alert': None,
# #                 'dot_struc': None
# #             }
# #         },