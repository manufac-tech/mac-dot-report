import pandas as pd

# from .dbase01_setup import build_main_dataframe
from .dbase26_f_merge import (
    field_merge_1,
    field_merge_2,
    field_merge_3
)


def build_report_dataframe(main_df_dict):
    """Create the report dataframe based on a copy of the full_main_dataframe."""
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    # Apply the field consolidation (the field_merge functions)
    # report_dataframe = field_merge_1(report_dataframe)
    # report_dataframe = field_merge_2(report_dataframe)
    # report_dataframe = field_merge_3(report_dataframe)

    return report_dataframe