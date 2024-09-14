import pandas as pd

# from .dbase01_setup import build_main_dataframe
from .dbase18_org import reorder_columns_rep
from .dbase26_f_merge import (
    field_merge_main,
    perform_full_matching
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

    # Add status fields for tracking matching results and system status
    report_dataframe['st_docs'] = ''
    report_dataframe['st_alert'] = ''
    report_dataframe['st_main'] = ''
    report_dataframe['st_db_all'] = ''

    # Initialize dictionary fields with empty dictionaries
    report_dataframe['fm_doc_match'] = [{} for _ in range(len(report_dataframe))]
    report_dataframe['fm_fs_match'] = [{} for _ in range(len(report_dataframe))]
    report_dataframe['fm_merge_summary'] = [{} for _ in range(len(report_dataframe))]

    # Apply the field consolidation (the field_merge_main function)
    # report_dataframe = field_merge_main(report_dataframe)  # Call the new function here

    report_dataframe = perform_full_matching(report_dataframe)  # Updated to call new matching logic

    report_dataframe = filter_no_show_rows(report_dataframe) # Filter out rows w 'no_show_di' = True

    # Reorder columns for the report DataFrame with new argument names
    report_dataframe = reorder_columns_rep(
        report_dataframe,
        show_field_merge=True,         # Set to True or False as needed
        show_unique_ids=False,          # Set to True or False as needed
        show_field_merge_dicts=True,   # Set to True or False as needed
        show_final_output=False         # Set to True or False as needed
    )

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

def filter_no_show_rows(report_dataframe):
    """Filter out rows where 'no_show_di' is set to True."""
    return report_dataframe[report_dataframe['no_show_di'] == False].copy()