import pandas as pd

from .db03_dtype_dict import field_types  # Import the field_types dictionary

# Add the unique ID generation code
current_unique_id = 1

def get_next_unique_id():
    global current_unique_id
    unique_id = current_unique_id
    current_unique_id += 1
    return unique_id

def print_data_types(df, title):
    print(f"{title} (Data Types):")
    data_types_df = df.apply(lambda col: col.map(lambda x: type(x).__name__))
    print(data_types_df)
    print("\n" * 2)
    
    print(f"{title} (Actual Values):")
    actual_values_df = df
    print(actual_values_df)
    print("\n" * 2)

def print_actual_data_types(df, title):
    print(f"{title} (Actual Data Types):")
    print(df.dtypes)
    print("\n" * 2)

def handle_missing_values(df, field_types):
    for column, dtype in field_types.items():
        if column in df.columns:
            if dtype == 'Int64':
                df[column] = df[column].fillna(pd.NA)
            elif dtype == 'string':
                df[column] = df[column].fillna("")
            # Add more rules for other data types as needed
    return df

def enforce_data_types(df, field_types):
    for column, dtype in field_types.items():
        if column in df.columns:
            df[column] = df[column].astype(dtype)
    return df

def df_merge_1_setup(main_df, home_df, dotbot_df, dot_info_df, print_df):
    left_merge_field = 'item_name'  # Only declared once; it remains the "left input" for all merges

    # Print data types before any merge
    print_data_types(main_df, "Before any merge")
    print_actual_data_types(main_df, "Before any merge")

    # Handle missing values before the first merge
    # main_df = handle_missing_values(main_df, field_types)
    # home_df = handle_missing_values(home_df, field_types)

    # First merge: repo and home
    right_merge_field = 'item_name_hm'
    main_df = df_merge_2_actual(main_df, home_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge1(main_df)

    # Manually set the data types after the first merge
    # main_df = replace_string_blanks(main_df)
    # main_df = enforce_data_types(main_df, field_types)

    # Print data types after corrections
    print_data_types(main_df, "After first merge (repo and home)")
    print_actual_data_types(main_df, "After first merge (repo and home)")

    # Handle missing values before the second merge
    # dotbot_df = handle_missing_values(dotbot_df, field_types)

    # Second merge: repo+home and dotbot R+H
    right_merge_field = 'item_name_rp_db'
    main_df = df_merge_2_actual(main_df, dotbot_df, left_merge_field, right_merge_field)  # Merge the DataFrames

    # Manually set the data types after the second merge
    main_df = replace_string_blanks(main_df)
    main_df = enforce_data_types(main_df, field_types)

    # Print data types after corrections
    print_data_types(main_df, "After second merge (repo+home and dotbot R+H)")
    print_actual_data_types(main_df, "After second merge (repo+home and dotbot R+H)")

    # Handle missing values before the third merge
    dot_info_df = handle_missing_values(dot_info_df, field_types)

    # Third merge: repo+home+dotbot and dot_info R+H
    right_merge_field = 'item_name_rp_di'
    main_df = df_merge_2_actual(main_df, dot_info_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge3(main_df)

    # Manually set the data types after the third merge
    main_df = replace_string_blanks(main_df)
    main_df = enforce_data_types(main_df, field_types)

    # Print data types after corrections
    print_data_types(main_df, "After third merge (repo+home+dotbot and dot_info R+H)")
    print_actual_data_types(main_df, "After third merge (repo+home+dotbot and dot_info R+H)")

    return main_df

def df_merge_2_actual(main_df, input_df, left_merge_field, right_merge_field, merge_type='outer'):
    # Perform the merge operation
    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how=merge_type
        ).copy()

        # Apply the blank replacement after the merge
        # merged_dataframe = replace_string_blanks(merged_dataframe)

        # Enforce data types on key columns
        # merged_dataframe = enforce_data_types(merged_dataframe, field_types)

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe

def replace_string_blanks(df):
    for column in df.columns:  # Iterates through each column in the DataFrame
        if column in field_types:  # Checks if the column is in the field_types dictionary
            dtype = field_types[column]  # Retrieves the expected data type for the column
            if dtype == 'string':  # Checks if the data type is 'string'
                # Convert everything to string to ensure we can replace all forms of NA
                df[column] = df[column].astype(str)
                # Replace all variations of NA, including case-insensitive matches
                df[column] = df[column].str.replace(r'(?i)^<na>$', '', regex=True)
                df[column] = df[column].str.replace(r'(?i)^nan$', '', regex=True)
                df[column] = df[column].str.replace(r'(?i)^none$', '', regex=True)
                # Fill remaining NaN values with empty string
                df[column] = df[column].fillna('')
            elif dtype == 'bool':
                # Fill NaN values in boolean columns with False
                df[column] = df[column].fillna(False)
            elif dtype == 'int':
                # Fill NaN values in integer columns with 0
                df[column] = df[column].fillna(0).astype(int)
            elif dtype == 'float':
                # Fill NaN values in float columns with 0.0
                df[column] = df[column].fillna(0.0).astype(float)
            elif dtype == 'Int64':
                # Convert to float first to handle NaN, then to Int64
                df[column] = df[column].astype(float).fillna(pd.NA).astype('Int64')
            # Add more data types as needed
    return df

def consolidate_post_merge1(main_df): # MERGE REQUIREMENT: Copies the priority name to item_name
    def consolidate(row):
        if pd.notna(row['item_name_rp']) and row['item_name_rp'] != "":
            return row['item_name_rp']
        elif pd.notna(row['item_name_hm']) and row['item_name_hm'] != "":
            return row['item_name_hm']
        else:
            return row['item_name']  # If both are null, keep the original value

    main_df['item_name'] = main_df.apply(consolidate, axis=1)
    return main_df

def consolidate_post_merge3(main_df): # MERGE REQUIREMENT: Copies the priority name to item_name
    def consolidate(row):
        if pd.notna(row['item_name_rp_di']) and row['item_name_rp_di'] != "":
            return row['item_name_rp_di']
        elif pd.notna(row['item_name_hm_di']) and row['item_name_hm_di'] != "":
            return row['item_name_hm_di']
        else:
            return row['item_name']  # If both are null, keep the original value

    main_df['item_name'] = main_df.apply(consolidate, axis=1)
    return main_df