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
    print(f"Validation passed for DataFrame section: {df_name}")
    
    # Update the main_df_dict with the validated DataFrame
    main_df_dict['dataframe'] = main_dataframe
    
    return main_df_dict

def validate_values(df, config): # USED BY BOTH: dbase/dbase08_validate.py and dbase/dbase07_load_tp.py
    """
    Validate and correct values in the DataFrame based on provided configuration.

    Args:
        df (DataFrame): The DataFrame to validate and correct.
        config (dict): A dictionary containing the validation and correction rules.

    Returns:
        DataFrame: The DataFrame with corrected values.
    """
    for column, rules in config.items():
        if 'valid_types' in rules:
            valid_types = rules['valid_types']
            invalid_values = df[column][~df[column].isin(valid_types)]
            if not invalid_values.empty:
                logging.warning(f"Invalid values found in column '{column}': {invalid_values.tolist()}")
        
        if 'fillna' in rules:
            df[column] = df[column].fillna(rules['fillna'])
        
        if 'ensure_not_null' in rules and rules['ensure_not_null']:
            if df[column].isnull().any() or (df[column] == '').any():
                logging.error(f"Some rows in column '{column}' have missing or empty values. These rows will be removed.")
                df = df[df[column].notnull() & (df[column] != '')]
        
        if 'assign_sequence' in rules and rules['assign_sequence']:
            df[column] = np.arange(1, len(df) + 1)
    
    return df