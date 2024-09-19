import pandas as pd

# from .db01_setup import build_main_dataframe
from dbase1_main.db14_org import reorder_dfr_cols_for_cli, reorder_dfr_cols_perm
from .db26_merge_match1 import field_match_master
from .db28_merge_update import consolidate_fields
from dbase1_main.db03_dtype_dict import field_types  # Import the field_types dictionary

def build_report_dataframe(main_df_dict):
    report_dataframe = main_df_dict['full_main_dataframe'].copy()

    # Handle NaN values globally
    report_dataframe = handle_nan_values(report_dataframe)

    # Define new columns and their data types
    new_columns = {
        'item_name_repo': field_types['item_name_repo'],
        'item_type_repo': field_types['item_type_repo'],
        'item_name_home': field_types['item_name_home'],
        'item_type_home': field_types['item_type_home'],
        'sort_out': field_types['sort_out'],
        'st_docs': field_types['st_docs'],
        'st_alert': field_types['st_alert'],
        'dot_struc': field_types['dot_struc'],
        'st_db_all': field_types['st_db_all'],
        'st_misc': field_types['st_misc']
    }

    # Create new columns with appropriate data types
    for column, dtype in new_columns.items():
        report_dataframe[column] = pd.Series(dtype=dtype)

    # Initialize 'sort_out' column with -1
    report_dataframe['sort_out'] = report_dataframe['sort_out'].fillna(-1)

    # Re-apply blank handling to the newly copied fields
    report_dataframe = handle_nan_values(report_dataframe)  # Ensure blank handling is applied

    # Apply field matching and consolidation
    report_dataframe = field_match_master(report_dataframe)
    report_dataframe = consolidate_fields(report_dataframe)
    report_dataframe = sort_filter_report_df(report_dataframe)  # Filter and sort rows

    report_dataframe = insert_blank_rows(report_dataframe) # Insert blank rows

    report_dataframe = reorder_dfr_cols_perm(report_dataframe)  # Reorder & sort cols perm

    # Reorder columns for CLI display
    report_dataframe = reorder_dfr_cols_for_cli(
        report_dataframe,
        show_all_fields=False,
        show_final_output=True,
        show_field_merge=False,
        show_field_merge_dicts=False
    )

    return report_dataframe

def handle_nan_values(df):
    # Replace NaN values in string columns with empty strings
    string_columns = df.select_dtypes(include=['object']).columns
    df[string_columns] = df[string_columns].fillna('')

    # Replace NaN values in numeric columns with 0 or another appropriate value
    numeric_columns = df.select_dtypes(include=['number']).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Replace NA values in all columns with appropriate defaults
    df = df.fillna('')

    return df

def sort_filter_report_df(df):
    # Filter out rows where 'no_show_di' is set to True
    df = df[df['no_show_di'] == False].copy()
    
    # Create a new column for the secondary sort key
    df['secondary_sort_key'] = df.apply(lambda row: row['sort_orig'] if row['sort_out'] == 30 else row['dot_struc_di'], axis=1)
    
    # Sort the DataFrame by 'sort_out' and then by the new secondary sort key
    df = df.sort_values(by=['sort_out', 'secondary_sort_key'], ascending=[True, True])
    
    # Drop the temporary secondary sort key column
    df = df.drop(columns=['secondary_sort_key'])
    
    return df

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