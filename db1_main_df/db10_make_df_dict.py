import pandas as pd

from .db11_make_main_df import build_main_dataframe
from db2_rep_df.db20_make_rpt_df import build_report_dataframe

# Set pandas display options globally
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 30)

def build_full_output_dict():
    output_df_dict = {}

    full_main_dataframe = build_main_dataframe()
    output_df_dict['full_main_dataframe'] = full_main_dataframe
    # print("\n FROM DB00: Full Main DataFrame:\n", full_main_dataframe)

    report_dataframe = build_report_dataframe(output_df_dict)
    output_df_dict['report_dataframe'] = report_dataframe
    # print("\n FROM DB00: Report DataFrame:\n", report_dataframe)

    return output_df_dict