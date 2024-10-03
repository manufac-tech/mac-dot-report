


import pandas as pd
from db5_global.db52_dtype_dict import f_types_vals

def insert_blank_rows(df):
    """
    NOTE: Temporarily converted numeric columns to 'object' type for display purposes to allow insertion of empty strings.
    This affects data types until conversion back to 'Int64' after display.
    """
    # Convert numeric columns to object just before adding blank rows
    numeric_cols = df.select_dtypes(include=['Int64', 'float']).columns
    df[numeric_cols] = df[numeric_cols].astype('object')
    
    # Get unique sort_out values
    unique_sort_out_values = df['sort_out'].unique()
    
    # Create a list to hold the new rows
    new_rows = []
    
    # Iterate through unique sort_out values
    for i, value in enumerate(unique_sort_out_values):
        # Get the rows with the current sort_out value
        group = df[df['sort_out'] == value]
        
        # Append the group to the new rows list
        new_rows.append(group)
        
        # Create a blank row with the correct data types (empty strings for all fields)
        blank_row = {}
        for col in df.columns:
            if col in f_types_vals:
                dtype = f_types_vals[col]['dtype']
                if dtype in ['object', 'string']:
                    blank_row[col] = ''  # Empty string for string columns
                elif dtype in ['Int64', 'float', 'boolean']:
                    blank_row[col] = ''  # Empty string for numeric/boolean columns too (now object dtype)
                else:
                    blank_row[col] = ''  # Fallback case
            else:
                blank_row[col] = ''  # Default for unhandled columns

        blank_row = pd.Series(blank_row, index=df.columns)
        
        # Append the blank row only if it's not the last group
        if i < len(unique_sort_out_values) - 1:
            new_rows.append(pd.DataFrame([blank_row]))
    
    # Concatenate the new rows into a new DataFrame
    new_df = pd.concat(new_rows, ignore_index=True)
    
    return new_df

def filter_report_df(df, hide_no_shows, hide_full_matches, hide_full_and_only, show_mstat_f, show_mstat_t):
    if hide_no_shows:
        df = df[df['no_show_cf'] == False].copy()
    if hide_full_matches:
        df = df[~df['dot_struc'].str.contains('rp>hm', na=False)].copy()
    if hide_full_and_only:
        df = df[~df['dot_struc'].str.contains('rp>hm|rp|hm', na=False)].copy()
    if show_mstat_f:
        df = df[df['m_status_result'] == False].copy()  # Corrected to filter boolean values
    if show_mstat_t:
        df = df[df['m_status_result'] == True].copy()   # Corrected to filter boolean values
    return df

def sort_report_df(df):
    df.loc[:, 'secondary_sort_key'] = df['git_rp'].apply(lambda x: 1 if x == False else 0)
    df.loc[:, 'tertiary_sort_key'] = df['sort_orig']
    df = df.sort_values(by=['sort_out', 'secondary_sort_key', 'tertiary_sort_key'], ascending=[True, True, True])
    df = df.drop(columns=['secondary_sort_key', 'tertiary_sort_key'])
    return df

def sort_filter_report_df(df, hide_no_shows, hide_full_matches, hide_full_and_only, show_mstat_f, show_mstat_t):
    df = filter_report_df(df, hide_no_shows, hide_full_matches, hide_full_and_only, show_mstat_f, show_mstat_t)
    df = sort_report_df(df)
    return df