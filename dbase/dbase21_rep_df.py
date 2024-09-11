import pandas as pd

# from .dbase01_setup import build_main_dataframe
from .dbase18_org import reorder_columns_rep
from .dbase26_f_merge import (
    field_merge_main
)


def build_report_dataframe(main_df_dict):
    """Create the report dataframe based on a copy of the full_main_dataframe."""
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    # Handle NaN values globally
    report_dataframe = handle_nan_values(report_dataframe)

    # Copy existing fields to the new target fields
    report_dataframe['item_name_home'] = report_dataframe['item_name_hm']
    report_dataframe['item_type_home'] = report_dataframe['item_type_hm']
    report_dataframe['item_name_repo'] = report_dataframe['item_name_rp']
    report_dataframe['item_type_repo'] = report_dataframe['item_type_rp']

    # Add status fields for error flagging
    report_dataframe['doc_status'] = ''
    report_dataframe['fs_status'] = ''
    report_dataframe['r_status_3'] = ''
    report_dataframe['final_status'] = ''

    # Apply the field consolidation (the field_merge_main function)
    report_dataframe = field_merge_main(report_dataframe)  # Call the new function here

    # Reorder columns for the report DataFrame
    report_dataframe = reorder_columns_rep(report_dataframe)

    return report_dataframe


def handle_nan_values(report_dataframe):
    for column in report_dataframe.columns:
        if pd.api.types.is_integer_dtype(report_dataframe[column]):
            # Fill NaN values with a specific integer or pd.NA for Int64 dtype
            report_dataframe[column] = report_dataframe[column].fillna(pd.NA)
        else:
            # Fill NaN values with an empty string for other dtypes
            report_dataframe[column] = report_dataframe[column].fillna('')
    return report_dataframe