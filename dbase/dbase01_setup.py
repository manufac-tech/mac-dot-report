import pandas as pd

from .dbase02_main_df import build_main_dataframe
from .dbase21_rep_df import build_report_dataframe
from .dbase18_org import reorder_columns_main

# Set pandas display options globally
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def build_full_output_dict():
    output_df_dict = {}

    full_main_dataframe = build_main_dataframe()
    output_df_dict['full_main_dataframe'] = full_main_dataframe
    print("\nFull Main DataFrame:\n", full_main_dataframe)

    report_dataframe = build_report_dataframe(output_df_dict)
    output_df_dict['report_dataframe'] = report_dataframe
    # print("\nReport DataFrame:\n", report_dataframe)

    return output_df_dict