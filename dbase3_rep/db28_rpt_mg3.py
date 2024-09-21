import pandas as pd

def write_st_alert_value(report_dataframe, index, new_status):
  print("🟪 write_st_alert_value")
  current_status = report_dataframe.at[index, 'st_alert']
  if pd.isna(current_status) or current_status == '':
      report_dataframe.at[index, 'st_alert'] = new_status
  else:
      report_dataframe = handle_mult_st_alerts_TEMP(report_dataframe, index, new_status)
  return report_dataframe

def handle_mult_st_alerts_TEMP(report_dataframe, index, new_status):
    current_status = report_dataframe.at[index, 'st_alert']
    report_dataframe.at[index, 'st_alert'] = f"🔴 {current_status}, {new_status}"
    return report_dataframe
