import pandas as pd
import logging

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
    data_types_df = df.apply(lambda col: col.map(lambda x: type(x).__name__)).head()
    print(data_types_df)
    print("\n" * 2)
    
    print(f"{title} (Actual Values):")
    actual_values_df = df.head()
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_convert_to_int64(series):
    logger.info(f"Converting column: {series.name}")
    try:
        before = series.copy()
        converted = series.astype(float).fillna(pd.NA).astype('Int64')
        after = converted.copy()
        
        # Log before and after states for debugging
        logger.debug(f"Before conversion: {before.head()}\nTypes: {before.apply(type).value_counts()}")
        logger.debug(f"After conversion: {after.head()}\nTypes: {after.apply(type).value_counts()}")
        
        if not (after.apply(type) == pd.Int64Dtype()).all():
            logger.warning(f"Column {series.name} not fully converted to Int64.")
        
        return converted
    except ValueError as e:
        logger.error(f"ValueError encountered converting {series.name}: {e}")
        problematic_rows = series[pd.to_numeric(series, errors='coerce').isna()]
        logger.error(f"Problematic rows for {series.name}:\n{problematic_rows}")
        raise

def df_merge_1_setup(main_df, home_df, dotbot_df, dot_info_df, print_df):
    pd.options.display.float_format = '{:.1f}'.format
    left_merge_field = 'item_name'  # Only declared once; it remains the "left input" for all merges

    print_data_types(main_df, "Before any merge")
    print_actual_data_types(main_df, "Before any merge")

    # Ensure consistent data types before merging
    main_df = enforce_data_types(main_df, field_types)
    home_df = enforce_data_types(home_df, field_types)
    dotbot_df = enforce_data_types(dotbot_df, field_types)
    dot_info_df = enforce_data_types(dot_info_df, field_types)

    # First merge: repo and home
    right_merge_field = 'item_name_hm'
    try:
        main_df = pd.merge(
            main_df, home_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how='outer'
        ).copy()
    except Exception as e:
        raise RuntimeError(f"Error during first merge: {e}")

    # Handle missing values and enforce data types
    main_df = handle_missing_values(main_df, field_types)
    main_df = enforce_data_types(main_df, field_types)

    # Apply conversion to all relevant columns
    int64_columns = ['unique_id', 'unique_id_rp', 'unique_id_hm', 'unique_id_di']  # Adjust as necessary
    for col in int64_columns:
        if col in main_df.columns:
            main_df[col] = safe_convert_to_int64(main_df[col])

    # Replace NaN with pd.NA and convert back to Int64
    for col in int64_columns:
        if col in main_df.columns:
            main_df[col] = main_df[col].replace({float('nan'): pd.NA}).astype('Int64')

    # Inspect DataFrame state after the first merge
    inspect_df(main_df, "first merge")

    print_data_types(main_df, "After first merge (repo and home)")
    print_actual_data_types(main_df, "After first merge (repo and home)")

    return main_df

def df_merge_2_actual(main_df, input_df, left_merge_field, right_merge_field, merge_type='outer'):
    pass

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

def inspect_df(df, step):
    logger.info(f"DataFrame state after {step}")
    logger.info(df.dtypes)
    for col in df.columns:
        if 'unique_id' in col:
            logger.info(f"{col} types: {df[col].apply(type).value_counts()}")