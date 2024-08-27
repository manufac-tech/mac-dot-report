import numpy as np
import pandas as pd
import logging

def validate_df_dict_current_and_main(input_df_dict_section, main_df_dict, df_name):
    # Ensure the 'dataframe' key points to an actual DataFrame
    if isinstance(input_df_dict_section['dataframe'], dict):
        input_df_dict_section['dataframe'] = pd.DataFrame(input_df_dict_section['dataframe'])

    if isinstance(main_df_dict['dataframe'], dict):
        main_df_dict['dataframe'] = pd.DataFrame(main_df_dict['dataframe'])

    # Extract the DataFrame from the dictionary section
    current_input_df = input_df_dict_section['dataframe']
    main_dataframe = main_df_dict['dataframe']

    # Now you can access the columns correctly
    name_field = input_df_dict_section['name_field']
    type_field = input_df_dict_section['type_field']

    # Extract the unique_id field from the columns of the current_input_df
    unique_id_field = [col for col in current_input_df.columns if 'unique_id' in col][0]

    required_columns = [name_field, type_field, unique_id_field]
    
    # Validate columns in current_input_df
    for col in required_columns:
        if col not in current_input_df.columns:
            raise KeyError(f"Missing required column '{col}' in DataFrame section '{df_name}'")

    # Validate data types in current_input_df
    if not pd.api.types.is_string_dtype(current_input_df[name_field]):
        raise TypeError(f"Field '{name_field}' in '{df_name}' should be of type string.")
    if not pd.api.types.is_string_dtype(current_input_df[type_field]):
        raise TypeError(f"Field '{type_field}' in '{df_name}' should be of type string.")
    if not pd.api.types.is_int64_dtype(current_input_df[unique_id_field]):
        raise TypeError(f"Field '{unique_id_field}' in '{df_name}' should be of type Int64.")

    # Create a universal config dictionary for validate_values
    config = {
        name_field: {
            'valid_types': [str],
            'ensure_not_null': True,
        },
        type_field: {
            'valid_types': ['file', 'folder', 'file_sym', 'folder_sym', 'alias', 'unknown'],
            'ensure_not_null': True,
        },
        unique_id_field: {
            'valid_types': [pd.Int64Dtype()],
            'ensure_not_null': True,
        }
    }
    
    # Call validate_values with the universal config
    main_dataframe = validate_values(main_dataframe, config)
    
    # Print the validation passed message with the section name
    # print(f"Validation passed for DataFrame section: {df_name}")
    
    # Update the main_df_dict with the validated DataFrame
    main_df_dict['dataframe'] = main_dataframe
    
    return main_df_dict

def validate_values(df, config):
    # Manually check for 'item_name' column to ensure it is non-null and of type string
    if 'item_name' in df.columns:
        if df['item_name'].isnull().any():
            logging.error(f"Some rows in 'item_name' have missing or empty values.")
            df = df[df['item_name'].notnull() & (df['item_name'] != '')]
        if not pd.api.types.is_string_dtype(df['item_name']):
            logging.error(f"Field 'item_name' should be of type string.")
    
    # Manually check for 'item_type' column to ensure it contains valid types
    valid_item_types = ['file', 'folder', 'file_sym', 'folder_sym', 'alias', 'unknown']
    if 'item_type' in df.columns:
        invalid_values = df['item_type'][~df['item_type'].isin(valid_item_types)]
        if not invalid_values.empty:  # Corrected: Access .empty as a property
            logging.warning(f"Invalid values found in 'item_type': {invalid_values.tolist()}")

    # Manually check for 'unique_id' column to ensure it is of type Int64
    if 'unique_id' in df.columns:
        if not pd.api.types.is_int64_dtype(df['unique_id']):
            logging.error(f"Field 'unique_id' should be of type Int64.")
    
    return df