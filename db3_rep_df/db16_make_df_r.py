import pandas as pd

from db1_main_df.db14_org import reorder_dfr_cols_for_cli, reorder_dfr_cols_perm
from .db17_make_df_r_sup import insert_blank_rows
from .db26_rpt_mg1_mast import field_match_master
from .db30_rpt_mg5_finish import consolidate_fields
from db1_main_df.db03_dtype_dict import f_types_vals

def build_report_dataframe(main_df_dict):
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    # Handle NaN values globally # ⭕️ BLANK HANDLING 
    # report_dataframe = handle_nan_values(report_dataframe)

    # Define new columns and their data types with default values
    new_columns = {
        'item_name_repo': f_types_vals['item_name_repo'],
        'item_type_repo': f_types_vals['item_type_repo'],
        'item_name_home': f_types_vals['item_name_home'],
        'item_type_home': f_types_vals['item_type_home'],
        'sort_out': f_types_vals['sort_out'],
        'st_docs': f_types_vals['st_docs'],
        'st_alert': f_types_vals['st_alert'],
        'dot_struc': f_types_vals['dot_struc'],
        'st_db_all': f_types_vals['st_db_all'],
        'st_misc': f_types_vals['st_misc']
    }

    # Create new columns with appropriate data types and default values
    for column, properties in new_columns.items():
        dtype = properties['dtype']
        default_value = properties['default']  # Updated from 'default' to 'default'
        report_dataframe[column] = pd.Series([default_value] * len(report_dataframe), dtype=dtype)

    # Initialize 'sort_out' column with -1
    report_dataframe['sort_out'] = report_dataframe['sort_out'].fillna(-1)

    # Re-apply blank handling to the newly copied fields # ⭕️ BLANK HANDLING 
    report_dataframe = handle_nan_values(report_dataframe)  # Ensure blank handling is applied

    # Apply field matching and consolidation
    report_dataframe = field_match_master(report_dataframe)
    report_dataframe = consolidate_fields(report_dataframe).copy()

    report_dataframe = sort_filter_report_df(report_dataframe, unhide_hidden=False)

    report_dataframe = insert_blank_rows(report_dataframe)

    report_dataframe = reorder_dfr_cols_perm(report_dataframe)

    # Reorder columns for CLI display
    report_dataframe = reorder_dfr_cols_for_cli(
        report_dataframe,
        show_all_fields=False,
        show_main_fields=True,
        show_status_fields=False,
    )

    return report_dataframe

def handle_nan_values(df): # ⭕️ BLANK HANDLING 
    # Replace NaN values in string columns with empty strings
    string_columns = df.select_dtypes(include=['object', 'string']).columns
    df[string_columns] = df[string_columns].fillna('')

    # Replace NaN values in numeric columns with 0 or another appropriate value
    numeric_columns = df.select_dtypes(include=['number']).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Replace NA values in all columns with appropriate defaults
    df = df.fillna('')
    # pass
    return df

def sort_filter_report_df(df, unhide_hidden):
    # df = df[df['no_show_di'] == False].copy()  # Filter out rows where 'no_show_di' is set to True
    if unhide_hidden:
        df['secondary_sort_key'] = df['git_rp'].apply(lambda x: 1 if x == False else 0)

    # Create a new column for the secondary sort key based on git_rp
    df['secondary_sort_key'] = df['git_rp'].apply(lambda x: 1 if x == False else 0)

    # The tertiary sort key is the original sort order
    df['tertiary_sort_key'] = df['sort_orig']
    
    # Sort the DataFrame by 'sort_out', 'secondary_sort_key', and 'tertiary_sort_key'
    df = df.sort_values(by=['sort_out', 'secondary_sort_key', 'tertiary_sort_key'], ascending=[True, True, True])
    
    # Drop the temporary sort key columns
    df = df.drop(columns=['secondary_sort_key', 'tertiary_sort_key'])
    
    return df