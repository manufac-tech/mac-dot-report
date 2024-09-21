import pandas as pd
from dbase1_main.db03_dtype_dict import field_types, field_types_with_defaults


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
            if field_types.get(col) == 'string':
                blank_row[col] = ''
            elif field_types.get(col) == 'boolean':
                blank_row[col] = ''
            elif field_types.get(col) in ['Int64', 'float']:
                blank_row[col] = ''
            else:
                blank_row[col] = ''
        
        blank_row = pd.Series(blank_row)
        
        # Append the blank row only if it's not the last group
        if i < len(unique_sort_out_values) - 1:
            new_rows.append(pd.DataFrame([blank_row]))
    
    # Concatenate the new rows into a new DataFrame
    new_df = pd.concat(new_rows, ignore_index=True)
    
    return new_df
