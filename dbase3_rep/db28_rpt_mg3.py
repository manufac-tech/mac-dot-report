import pandas as pd

def write_st_alert_value(report_dataframe, index, new_status):
    current_status = report_dataframe.at[index, 'st_alert']
    if pd.isna(current_status) or current_status == '':
        report_dataframe.at[index, 'st_alert'] = new_status
    else:
        report_dataframe = handle_mult_st_alerts_TEMP(report_dataframe, index, new_status)
    return report_dataframe

def handle_mult_st_alerts_TEMP(report_dataframe, index, new_status):
    current_status = report_dataframe.at[index, 'st_alert']
    
    # Check if new_status is already in current_status
    if new_status not in current_status:
        report_dataframe.at[index, 'st_alert'] = f"🔴 {current_status}, {new_status}"
    return report_dataframe
