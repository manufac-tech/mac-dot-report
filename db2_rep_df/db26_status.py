import pandas as pd

from .db24_rpt_mg3_oth import write_st_alert_value
from .db27_status_config import get_status_checks_config
from .db40_term_disp import remove_consolidated_columns



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