import pandas as pd

from .dbase02_main_df import build_main_dataframe
from .dbase21_rep_df import build_report_dataframe

pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.set_option('display.width', None)  # Set the display width to None
pd.set_option('display.max_colwidth', None)  # Set the max column width to None

def build_full_output_dict():
    output_df_dict = {}

    # Call to DB2: Build the full main dataframe
    full_main_dataframe = build_main_dataframe()

    # Insert full_main_dataframe into the dictionary
    output_df_dict['full_main_dataframe'] = full_main_dataframe

    # Print the entire full_main_dataframe to the console (commented out for now)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
    #     print("\nFull Main DataFrame:\n", full_main_dataframe)

    # Call to DB21: Build the report dataframe
    report_dataframe = build_report_dataframe(output_df_dict)

    # Print the entire report_dataframe to the console
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        print("\nReport DataFrame:\n", report_dataframe)

    # Insert report_dataframe into the dictionary (commented out for now)
    # output_df_dict['report_dataframe'] = report_dataframe

    return output_df_dict