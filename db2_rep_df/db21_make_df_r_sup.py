import pandas as pd
from db5_global.db52_dtype_dict import f_types_vals

def insert_blank_rows(df):
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
        
        # Create a blank row with the correct data types
        blank_row = {}
        for col in df.columns:
            if col in f_types_vals:
                dtype = f_types_vals[col]['dtype']
                default_value = f_types_vals[col]['default']
                if dtype in ['object', 'string']:
                    blank_row[col] = ''  # Empty string for string columns
                elif dtype == 'bool':
                    blank_row[col] = False  # False for boolean columns
                elif dtype in ['Int64', 'float']:
                    blank_row[col] = 0  # 0 for numeric columns
                else:
                    blank_row[col] = ''  # Fallback case
            else:
                blank_row[col] = ''  # Default for unhandled columns

        blank_row = pd.Series(blank_row)
        
        # Append the blank row only if it's not the last group
        if i < len(unique_sort_out_values) - 1:
            new_rows.append(pd.DataFrame([blank_row]))
    
    # Concatenate the new rows into a new DataFrame
    new_df = pd.concat(new_rows, ignore_index=True)
    
    return new_df

def reorder_dfr_cols_perm(df):
    # Define the desired column order based on the provided fields
    desired_order = [
        # 'item_name',
        'st_alert', 'item_name_home', 'item_type_home', 'item_name_repo', 'item_type_repo', 'git_rp', 
        'cat_1_di', 'cat_1_name_di', 'cat_2_di', 'comment_di',
        'dot_struc_di',
        'dot_struc', 'st_db_all', 'st_docs', 'st_misc',
        'sort_orig', 'sort_out',
        'no_show_di', 'unique_id',
        'match_dict'
    ]
    
    # Ensure all columns in desired_order are in the DataFrame
    for col in desired_order:
        if col not in df.columns:
            print(f"Warning: Column {col} not found in DataFrame")
    
    # Reorder columns
    df = df[desired_order]
    
    return df