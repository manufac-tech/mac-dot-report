import pandas as pd

# from .dbase01_setup import build_main_dataframe
from .dbase18_org import reorder_columns_rep
from .dbase26_f_mg1 import (
    field_match_master
)
from .dbase28_f_mg3 import (
    consolidate_fields
)


def build_report_dataframe(main_df_dict):
    """Create the report dataframe based on a copy of the full_main_dataframe."""
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    print(report_dataframe[['item_name_rp', 'item_type_rp', 'no_show_di']].tail(10))

    # Handle NaN values globally
    # report_dataframe = handle_nan_values(report_dataframe)

    # Create new target fields
    report_dataframe['item_name_repo'] = ''
    report_dataframe['item_type_repo'] = ''
    report_dataframe['item_name_home'] = ''
    report_dataframe['item_type_home'] = ''

    # Re-apply blank handling to the newly copied fields
    report_dataframe = handle_nan_values(report_dataframe)  # Ensure blank handling is applied

    # Add status fields for tracking matching results and system status
    report_dataframe['st_docs'] = ''
    report_dataframe['st_alert'] = ''
    report_dataframe['st_main'] = ''
    report_dataframe['st_db_all'] = ''
    report_dataframe['st_misc'] = ''

    # Initialize dictionary fields with empty dictionaries
    report_dataframe['fm_doc_match'] = [{} for _ in range(len(report_dataframe))]
    report_dataframe['fm_fs_match'] = [{} for _ in range(len(report_dataframe))]
    report_dataframe['fm_merge_summary'] = [{} for _ in range(len(report_dataframe))]

    # Apply the field consolidation (the field_merge_main function)
    # report_dataframe = field_merge_main(report_dataframe)  # Call the new function here

    report_dataframe = field_match_master(report_dataframe)  # Updated to call new matching logic

    # Consolidate name, type, and unique_id fields based on the matching logic
    report_dataframe = consolidate_fields(report_dataframe)  # <--- Call the consolidation function here

    # report_dataframe = filter_no_show_rows(report_dataframe) # Filter out rows w 'no_show_di' = True

    # Reorder columns for the report DataFrame with new argument names
    report_dataframe = reorder_columns_rep(
        report_dataframe,
        show_field_merge=True,         # Set to True or False as needed
        show_unique_ids=True,          # Set to True or False as needed
        show_field_merge_dicts=False,   # Set to True or False as needed
        show_final_output=True         # Set to True or False as needed
    )

    return report_dataframe


def handle_nan_values(df):
    """
    Replace NaN values in the DataFrame with appropriate defaults.
    """
    # Replace NaN values in string columns with empty strings
    string_columns = df.select_dtypes(include=['object']).columns
    df[string_columns] = df[string_columns].fillna('')

    # Replace NaN values in numeric columns with 0 or another appropriate value
    numeric_columns = df.select_dtypes(include=['number']).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Replace NA values in all columns with appropriate defaults
    df = df.fillna('')

    return df

def filter_no_show_rows(report_dataframe):
    """Filter out rows where 'no_show_di' is set to True."""
    return report_dataframe[report_dataframe['no_show_di'] == False].copy()