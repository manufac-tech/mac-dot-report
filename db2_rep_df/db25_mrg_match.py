import pandas as pd

from .db24_rpt_mg3_oth import write_st_alert_value
from .db27_status_config import get_status_checks_config
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





# def resolve_fields_master(report_dataframe): 
#     pass

