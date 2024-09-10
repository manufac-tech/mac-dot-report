import pandas as pd

# from .dbase01_setup import build_main_dataframe
from .dbase18_org import reorder_columns_rep
from .dbase26_f_merge import (
    compare_documents,
)


def build_report_dataframe(main_df_dict):
    """Create the report dataframe based on a copy of the full_main_dataframe."""
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    # Copy existing fields to the new target fields
    report_dataframe['item_name_home'] = report_dataframe['item_name_hm']
    report_dataframe['item_type_home'] = report_dataframe['item_type_hm']
    report_dataframe['item_name_repo'] = report_dataframe['item_name_rp']
    report_dataframe['item_type_repo'] = report_dataframe['item_type_rp']

    # Add status fields for error flagging
    report_dataframe['r_status_1'] = ''  # Flag for name match/mismatch
    report_dataframe['r_status_2'] = ''  # Flag for type match/mismatch
    report_dataframe['r_status_3'] = ''  # Flag for other inconsistencies

    # Apply the field consolidation (the field_merge functions)
    report_dataframe = compare_documents(report_dataframe)

    # Reorder columns for the report DataFrame
    report_dataframe = reorder_columns_rep(report_dataframe)

    # report_dataframe = report_dataframe[report_dataframe['r_status_1'] != 'Name Match']
    # report_dataframe = report_dataframe.sort_values(by='r_status_1')

    return report_dataframe

