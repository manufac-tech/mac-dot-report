import pandas as pd

from db5_global.db52_dtype_dict import f_types_vals

from .db21_make_df_r_sup import insert_blank_rows, reorder_dfr_cols_perm
from .db22_rpt_mg1_mast import field_match_master
from .db25_rpt_mg5_finish import consolidate_fields
from .db26_rpt_mg6_fsup import reorder_dfr_cols_for_cli

def build_report_dataframe(main_df_dict):
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    # Define new columns and their data types with default values
    new_columns = {
        'item_name_repo': f_types_vals['item_name_repo'],
        'item_type_repo': f_types_vals['item_type_repo'],
        'item_name_home': f_types_vals['item_name_home'],
        'item_type_home': f_types_vals['item_type_home'],
        'sort_out': f_types_vals['sort_out'],
        'st_docs': f_types_vals['st_docs'],
        'st_alert': f_types_vals['st_alert'],
        'dot_struc': f_types_vals['dot_struc'],
        'st_db_all': f_types_vals['st_db_all'],
        'st_misc': f_types_vals['st_misc'],
        'match_dict': f_types_vals['match_dict'],
    }

    # Create new columns with appropriate data types and default values
    for column, properties in new_columns.items():
        dtype = properties['dtype']
        default_value = properties['default']
        report_dataframe[column] = pd.Series([default_value] * len(report_dataframe), dtype=dtype)

    # Initialize 'sort_out' column with -1
    report_dataframe['sort_out'] = report_dataframe['sort_out'].fillna(-1)

    # Apply field matching and consolidation
    # report_dataframe, field_merge_rules = field_match_master(report_dataframe)
    # report_dataframe = consolidate_fields(report_dataframe, field_merge_rules).copy()

    report_dataframe = post_build_nan_replace(report_dataframe)

    report_dataframe = sort_filter_report_df(report_dataframe, unhide_hidden=False)
    report_dataframe = insert_blank_rows(report_dataframe)
    # report_dataframe = reorder_dfr_cols_perm(report_dataframe) # REMOVING COLUMNS - UNWAANTED
    
    # Reorder columns for CLI display
    report_dataframe = reorder_dfr_cols_for_cli(
        report_dataframe,
        show_all_fields=False,
        show_main_fields=True,
        show_status_fields=False,
    )

    numeric_cols = report_dataframe.select_dtypes(include=['Int64']).columns  # Identify numeric columns for reconversion in line below
    report_dataframe[numeric_cols] = report_dataframe[numeric_cols].astype('Int64')  # RECONVERTS NUMERIC COLUMNS (blank line workaround) TO INT64

    # Print the current columns of the DataFrame
    print("Current columns in the DataFrame:", report_dataframe.columns.tolist())

    # Apply the detect_status_master function with the status_checks_config
    report_dataframe = detect_status_master(report_dataframe) # TEMP TEST
    return report_dataframe

def post_build_nan_replace(df):
    # Replace NaN values in string columns with empty strings
    string_columns = df.select_dtypes(include=['object', 'string']).columns
    df[string_columns] = df[string_columns].fillna('')

    # Replace NaN values in numeric columns with 0 or another appropriate value
    numeric_columns = df.select_dtypes(include=['number']).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Replace NaN values in boolean columns with False
    boolean_columns = df.select_dtypes(include=['bool']).columns
    df[boolean_columns] = df[boolean_columns].fillna(False)

    # Replace NaN values in nullable integer columns with 0
    nullable_int_columns = df.select_dtypes(include=['Int64', 'Int32', 'Int16']).columns
    df[nullable_int_columns] = df[nullable_int_columns].fillna(0)

    # Replace NaN values in nullable boolean columns with False
    nullable_bool_columns = df.select_dtypes(include=['boolean']).columns
    df[nullable_bool_columns] = df[nullable_bool_columns].fillna(False)

    # Replace NaN values in all other columns with appropriate defaults
    df = df.fillna('')

    return df

def sort_filter_report_df(df, unhide_hidden):
    df = df[df['no_show_cf'] == False].copy()  # Filter out rows where 'no_show_cf' is set to True
    if unhide_hidden:
        df['secondary_sort_key'] = df['git_rp'].apply(lambda x: 1 if x == False else 0)

    # Create a new column for the secondary sort key based on git_rp
    df['secondary_sort_key'] = df['git_rp'].apply(lambda x: 1 if x == False else 0)

    # The tertiary sort key is the original sort order
    df['tertiary_sort_key'] = df['sort_orig']
    
    # Sort the DataFrame by 'sort_out', 'secondary_sort_key', and 'tertiary_sort_key'
    df = df.sort_values(by=['sort_out', 'secondary_sort_key', 'tertiary_sort_key'], ascending=[True, True, True])
    
    # Drop the temporary sort key columns
    df = df.drop(columns=['secondary_sort_key', 'tertiary_sort_key'])
    
    return df

# def detect_status_master(report_dataframe):
#     for index, row in report_dataframe.iterrows():
#         # DotBot matching logic
#         match_logic = (
#             (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp'])) or
#             (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp'])
#         ) and (
#             (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm'])) or
#             (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm'])
#         )
        
#         # Apply the match result to the 'st_db_all' and 'st_alert' fields
#         if match_logic:
#             row['st_db_all'] = 'o'  # Success case
#             row['st_alert'] = None   # No alert if matched
#             print(f"Row {index} matched: st_db_all set to {row['st_db_all']}")
#         else:
#             row['st_db_all'] = 'x'  # Failure case
#             row['st_alert'] = 'DotBot mismatch'
#             print(f"Row {index} did not match: st_db_all set to {row['st_db_all']}")
    
    # return report_dataframe


# def detect_status_master(report_dataframe):
#     for index, row in report_dataframe.iterrows():
#         # Instead of actual logic, simply set both fields to "TEST"
#         report_dataframe.loc[index, 'st_db_all'] = 'TEST'
#         report_dataframe.loc[index, 'st_alert'] = 'TEST'
        
#         # Debugging: Print the test result
#         print(f"Row {index}: st_db_all and st_alert set to 'TEST'")
    
#     return report_dataframe


# def detect_status_master(report_dataframe, config):
#     for index, row in report_dataframe.iterrows():
#         # Apply the DotBot status check based on the configuration
#         dotbot_rule = config['subsys_dotbot']
#         match_logic = dotbot_rule['match_logic']
        
#         # Debugging: Print the row being processed
#         print(f"Processing row {index}: {row.to_dict()}")
        
#         if match_logic(row):
#             report_dataframe.loc[index, 'st_db_all'] = dotbot_rule['output']['st_db_all']
#             report_dataframe.loc[index, 'st_alert'] = dotbot_rule['output']['st_alert']
#             # Debugging: Print the match result
#             print(f"Row {index} matched: st_db_all set to {report_dataframe.loc[index, 'st_db_all']}")
#         else:
#             report_dataframe.loc[index, 'st_db_all'] = 'x'
#             report_dataframe.loc[index, 'st_alert'] = 'DotBot mismatch'
#             # Debugging: Print the mismatch result
#             print(f"Row {index} did not match: st_db_all set to {report_dataframe.loc[index, 'st_db_all']}")
    
#     return report_dataframe

# status_checks_config = {
#     'subsys_dotbot': {
#         'input_fields': ['item_name_rp_db', 'item_name_hm_db', 'item_name_rp', 'item_name_hm'],
#         'match_logic': lambda row: (
#             (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp'])) or
#             (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp'])
#         ) and (
#             (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm'])) or
#             (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm'])
#         ),
#         'output': {
#             'st_db_all': 'o',  # Success case
#             'st_alert': None   # No alert if matched
#         }
#     }
# }

# def detect_status_master(report_dataframe):
#     for index, row in report_dataframe.iterrows():
#         # DotBot matching logic
#         match_logic = (
#             (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp'])) or
#             (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp'])
#         ) and (
#             (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm'])) or
#             (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm'])
#         )
        
#         # Apply the match result to the 'st_db_all' and 'st_alert' fields
#         if match_logic:
#             row['st_db_all'] = 'o'  # Success case
#             row['st_alert'] = 'TEST'   # No alert if matched
#             print(f"Row {index} matched: st_db_all set to {row['st_db_all']}")
#         else:
#             row['st_db_all'] = 'x'  # Failure case
#             row['st_alert'] = 'DotBot mismatch'
#             print(f"Row {index} did not match: st_db_all set to {row['st_db_all']}")
    
#     return report_dataframe

# def detect_status_master(report_dataframe):
#     for index, row in report_dataframe.iterrows():
#         # DotBot matching logic
#         match_logic = (
#             (pd.isna(row['item_name_rp_db']) and pd.isna(row['item_name_rp'])) or
#             (not pd.isna(row['item_name_rp_db']) and not pd.isna(row['item_name_rp']) and row['item_name_rp_db'] == row['item_name_rp'])
#         ) and (
#             (pd.isna(row['item_name_hm_db']) and pd.isna(row['item_name_hm'])) or
#             (not pd.isna(row['item_name_hm_db']) and not pd.isna(row['item_name_hm']) and row['item_name_hm_db'] == row['item_name_hm'])
#         )
        
#         # Apply the match result to the 'st_db_all' and 'st_alert' fields
#         if match_logic:
#             report_dataframe.loc[index, 'st_db_all'] = 'o'  # Success case
#             report_dataframe.loc[index, 'st_alert'] = 'TEST'   # No alert if matched
#         else:
#             report_dataframe.loc[index, 'st_db_all'] = 'x'  # Failure case
#             report_dataframe.loc[index, 'st_alert'] = 'DotBot mismatch'
    
#     return report_dataframe




def detect_status_master(report_dataframe):
    # Get the configuration dictionary
    config = get_status_checks_config()
    
    for index, row in report_dataframe.iterrows():
        for subsystem, rules in config.items():
            # Extract input fields and match logic
            input_fields = rules['input_fields']
            match_logic = rules['match_logic'](row)
            
            # Apply the match result to the output fields
            if match_logic:
                for field, value in rules['output'].items():
                    report_dataframe.loc[index, field] = value
            else:
                for field, value in rules['failure_output'].items():
                    report_dataframe.loc[index, field] = value
    
    return report_dataframe

def get_status_checks_config():
    return {
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
                'st_db_all': 'o',  # Success case
                'st_alert': None   # No alert if matched
            },
            'failure_output': {
                'st_db_all': 'x',  # Failure case
                'st_alert': 'DotBot mismatch'
            }
        }
    }