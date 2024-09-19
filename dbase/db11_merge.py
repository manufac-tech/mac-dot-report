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

def df_merge_sequence(main_df, home_df, dotbot_df, dot_info_df, print_df):
    left_merge_field = 'item_name' # Only declared once; it remains the "left input" for all merges

    # First merge: repo and home
    right_merge_field = 'item_name_hm'
    main_df = df_merge(main_df, home_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge1(main_df)

    # Second merge: repo+home and dotbot R+H
    right_merge_field = 'item_name_rp_db'
    main_df = df_merge(main_df, dotbot_df, left_merge_field, right_merge_field)  # Merge the DataFrames

    # Third merge: repo+home+dotbot and dot_info R+H
    right_merge_field = 'item_name_rp_di'
    main_df = df_merge(main_df, dot_info_df, left_merge_field, right_merge_field)  # Merge the DataFrames
    main_df = consolidate_post_merge3(main_df)

    return main_df

def df_merge(main_df, input_df, left_merge_field, right_merge_field, merge_type='outer'):
    main_df_build_hist = {"df1": main_df.copy(), "df2": input_df.copy()}

    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how=merge_type
        ).copy()

        merged_dataframe = replace_string_blanks(merged_dataframe)

        main_df_build_hist["df3"] = merged_dataframe.copy()
        # print_main_df_build_hist(main_df_build_hist) # Print the build history

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe


def replace_string_blanks(df):
    for column in df.columns:
        # Handle string columns
        if pd.api.types.is_string_dtype(df[column]):
            df[column] = df[column].astype(str)
            # Replace variations of NA, including case-insensitive matches
            df[column] = df[column].str.replace(r'(?i)^<na>$', '', regex=True)
            df[column] = df[column].str.replace(r'(?i)^nan$', '', regex=True)
            df[column] = df[column].str.replace(r'(?i)^none$', '', regex=True)
            # Fill remaining NaN values with empty string
            df[column] = df[column].fillna('')

        # Handle Int64 columns
        elif pd.api.types.is_integer_dtype(df[column]) or pd.api.types.is_dtype_equal(df[column].dtype, "Int64"):
            # Replace NaN or pd.NA with 0 for Int64 columns
            df[column] = df[column].fillna(0).astype('Int64')

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

def print_main_df_build_hist(df_dict):
    # Initialize a static counter
    if not hasattr(print_main_df_build_hist, "merge_counter"):
        print_main_df_build_hist.merge_counter = 1

    for key, df in df_dict.items():
        if key == "df1":
            value_title = f"🟩 Main DataFrame (Before Merge) - Merge {print_main_df_build_hist.merge_counter} - First 5 Rows"
            type_title = f"Main DataFrame (Before Merge) - Merge {print_main_df_build_hist.merge_counter} - Data Types (First 5 Rows)"
        elif key == "df2":
            value_title = f"Input DataFrame (Before Merge) - Merge {print_main_df_build_hist.merge_counter} - First 5 Rows"
            type_title = f"Input DataFrame (Before Merge) - Merge {print_main_df_build_hist.merge_counter} - Data Types (First 5 Rows)"
        elif key == "df3":
            value_title = f"Merged DataFrame (After Merge) - Merge {print_main_df_build_hist.merge_counter} - First 5 Rows"
            type_title = f"Merged DataFrame (After Merge) - Merge {print_main_df_build_hist.merge_counter} - Data Types (First 5 Rows)"

        print(f"{value_title}:")
        # print(df.head(5))
        print(df)
        print("\n")

        print(f"{type_title}:")
        # print(df.dtypes.head(5))
        print(df.dtypes)
        print("\n" * 2)

    # Increment the counter after processing the DataFrames
    print_main_df_build_hist.merge_counter += 1