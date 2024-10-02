import pandas as pd
import numpy as np

from db5_global.db52_dtype_dict import f_types_vals

from .db21_make_df_r_sup import insert_blank_rows, reorder_dfr_cols_perm
from .db22_match_reg import detect_full_domain_match
from .db23_match_alert import detect_alerts

from .db40_term_disp import reorder_dfr_cols_for_cli

# Opt-in to the future behavior for downcasting
pd.set_option('future.no_silent_downcasting', True)

def build_report_dataframe(main_df_dict):
    report_dataframe = main_df_dict['full_main_dataframe'].copy()
    report_dataframe = add_report_fields(report_dataframe)

    # report_dataframe = insert_blank_rows(report_dataframe)
    report_dataframe = reorder_dfr_cols_perm(report_dataframe)

    report_dataframe = detect_full_domain_match(report_dataframe)
    report_dataframe = detect_alerts(report_dataframe) # NEW

    # report_dataframe = resolve_fields_master(report_dataframe)
    report_dataframe = post_build_nan_replace(report_dataframe)
    report_dataframe = sort_filter_report_df(
        report_dataframe, 
        hide_no_shows=False, 
        hide_full_matches=False, 
        hide_full_and_only=False,
        )
    
    # Print the result to the console
    # print("🟪 DEBUG: Full Report DataFrame")
    # print(report_dataframe[['item_name_repo', 'item_type_repo', 'item_name_home', 'item_type_home', 'm_consol_dict']])
    
    report_dataframe = reorder_dfr_cols_for_cli( # Reorder columns for CLI display
        report_dataframe,
        show_all_fields=False,
        show_main_fields=False,
        show_status_fields=False,
        show_setup_group=True,
    )

    return report_dataframe


def add_report_fields(report_dataframe):
    df = report_dataframe
    new_columns = { # Define the new columns to add
        'item_name_repo': f_types_vals['item_name_repo'],
        'item_type_repo': f_types_vals['item_type_repo'],
        'item_name_home': f_types_vals['item_name_home'],
        'item_type_home': f_types_vals['item_type_home'],
        'sort_out': f_types_vals['sort_out'],
        'st_docs': f_types_vals['st_docs'],
        'st_alert': f_types_vals['st_alert'],
        'dot_struc': f_types_vals['dot_struc'],
        'st_db_all': f_types_vals['st_db_all'],
        'st_misc': f_types_vals['st_misc'],
        'm_status_dict': f_types_vals['m_status_dict'],
        'm_consol_dict': f_types_vals['m_consol_dict'],
        'm_status_result': f_types_vals['m_status_result'],
        'm_consol_result': f_types_vals['m_consol_result'],
        'st_match_emo': f_types_vals['st_match_emo'],
    }
    
    for column, properties in new_columns.items(): # Create the new columns ( + types & vals)
        dtype = properties['dtype']
        default_value = properties['default']
        df[column] = pd.Series([default_value] * len(df), dtype=dtype)
    
    df['sort_out'] = df['sort_out'].fillna(-1) # Initialize 'sort_out' column with -1


    return df


def post_build_nan_replace(df): # Replace NaN vals
    for column in df.columns:
        if column in f_types_vals:
            default_value = f_types_vals[column]['default']
            df[column] = df[column].fillna(default_value)
        else:
            print(f"Column '{column}' not found in dictionary")
    return df

    # First, attempt to fill NaN values using the dictionary
    df = fill_na_with_defaults(df)

    return df



def filter_report_df(df, hide_no_shows, hide_full_matches, hide_full_and_only):
    if hide_no_shows:
        df = df[df['no_show_cf'] == False].copy()
    if hide_full_matches:
        df = df[~df['dot_struc'].str.contains('rp>hm', na=False)].copy()
    if hide_full_and_only:
        df = df[~df['dot_struc'].str.contains('rp>hm|rp|hm', na=False)].copy()
    return df

def sort_report_df(df):
    df['secondary_sort_key'] = df['git_rp'].apply(lambda x: 1 if x == False else 0)
    df['tertiary_sort_key'] = df['sort_orig']
    df = df.sort_values(by=['sort_out', 'secondary_sort_key', 'tertiary_sort_key'], ascending=[True, True, True])
    df = df.drop(columns=['secondary_sort_key', 'tertiary_sort_key'])
    return df

def sort_filter_report_df(df, hide_no_shows, hide_full_matches, hide_full_and_only):
    df = filter_report_df(df, hide_no_shows, hide_full_matches, hide_full_and_only)
    df = sort_report_df(df)
    return df