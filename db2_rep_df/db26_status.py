import pandas as pd
from .db27_status_config import get_status_checks_config


def detect_status_master(report_dataframe):
    status_checks_config = get_status_checks_config()
    # print("Status Checks Configuration:", status_checks_config)  # Debug: Print the configuration

    for index, row in report_dataframe.iterrows():
        # print(f"Processing row {index}: {row.to_dict()}")  # Debug: Print the row being processed

        for check_name, check_config in status_checks_config.items():
            match_logic = check_config['match_logic']
            output = check_config['output']
            failure_output = check_config['failure_output']

            match_result = match_logic(row)
            # print(f"Check: {check_name}, Match Result: {match_result}")  # Debug: Print the match result

            if match_result:
                dot_struc_value = output.get('dot_struc')
            else:
                dot_struc_value = failure_output.get('dot_struc')

            if dot_struc_value is not None:
                # print(f"Updating dot_struc to {dot_struc_value} for row {index}")  # Debug: Print the update
                report_dataframe.at[index, 'dot_struc'] = dot_struc_value

    return report_dataframe