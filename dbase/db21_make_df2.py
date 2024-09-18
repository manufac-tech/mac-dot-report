import pandas as pd

# from .db01_setup import build_main_dataframe
from .db18_org import reorder_dfr_cols_for_cli, reorder_dfr_cols_perm
from .db26_merge_match1 import (
    field_match_master
)
from .db28_merge_update import (
    consolidate_fields
)


def build_report_dataframe(main_df_dict):
    """Create the report dataframe based on a copy of the full_main_dataframe."""
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    # Handle NaN values globally
    report_dataframe = handle_nan_values(report_dataframe)

    # Create new target fields
    report_dataframe['item_name_repo'] = ''
    report_dataframe['item_type_repo'] = ''
    report_dataframe['item_name_home'] = ''
    report_dataframe['item_type_home'] = ''

    report_dataframe['sort_out'] = pd.Series(dtype='Int64')  # Create sort_out column
    report_dataframe['sort_out'] = report_dataframe['sort_out'].fillna(-1)

    # Re-apply blank handling to the newly copied fields
    report_dataframe = handle_nan_values(report_dataframe)  # Ensure blank handling is applied

    # Add status fields for tracking matching results and system status
    report_dataframe['st_docs'] = ''
    report_dataframe['st_alert'] = ''
    report_dataframe['dot_struc'] = ''
    report_dataframe['st_db_all'] = ''
    report_dataframe['st_misc'] = ''

    # report_dataframe = field_merge_main(report_dataframe) # field merge/consolidation

    report_dataframe = field_match_master(report_dataframe)

    # Consolidate name, type, and unique_id fields based on the matching logic
    report_dataframe = consolidate_fields(report_dataframe)
    report_dataframe = filter_no_show_rows(report_dataframe)

    report_dataframe = reorder_dfr_cols_perm(report_dataframe) # Reorder columns perm

    report_dataframe = sort_report_df_rows(report_dataframe) # Sort rows permanently

    report_dataframe = reorder_dfr_cols_for_cli( # Reorder columns for CLI display
        report_dataframe,
        show_all_fields=False,
        show_final_output=True,
        show_field_merge=False,
        show_field_merge_dicts=False
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

def sort_report_df_rows(df):
    """
    Sort the DataFrame by 'sort_out' and then by 'sort_orig'.
    """
    df = df.sort_values(by=['sort_out', 'sort_orig'], ascending=[True, True])
    return df