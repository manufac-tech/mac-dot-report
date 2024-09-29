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

def reorder_dfr_cols_perm(df): # Defines both order and PRESENCE of columns
    desired_order = [
        # 'item_name',
        'st_alert', 'item_name_home', 'item_type_home', 'item_name_repo', 'item_type_repo', 'git_rp',
        'cat_1_cf', 'cat_1_name_cf', 'cat_2_cf', 'comment_cf',
        'dot_struc_cf',
        'dot_struc', 'st_db_all', 'st_docs', 'st_misc',
        'sort_orig', 'sort_out',
        'no_show_cf', 'unique_id',

        # Catch-all for any remaining columns
        'item_name', 'item_type', 'item_name_rp', 'item_type_rp', 'item_name_hm', 'item_type_hm', 
        'item_name_hm_db', 'item_type_hm_db', 'item_name_rp_db', 'item_type_rp_db', 
        'item_name_rp_cf', 'item_type_rp_cf', 'item_name_hm_cf', 'item_type_hm_cf', 
        'unique_id_rp', 'unique_id_db', 'unique_id_hm', 'unique_id_cf',

        'm_consol_dict',
        'm_status_dict',
        'm_status_result',
        'm_consol_result'
    ]

    # Ensure all columns in desired_order are in the DataFrame
    for col in desired_order:
        if col not in df.columns:
            print(f"Warning: Column {col} not found in DataFrame")
    
    # Reorder columns
    df = df[desired_order]
    
    return df