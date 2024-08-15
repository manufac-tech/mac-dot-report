import pandas as pd
import logging
import pandas as pd

import pandas as pd

def validate_dataframes(df1, df2):
    required_columns = ['unique_id', 'item_name', 'item_type']
    for col in required_columns:
        if col not in df1.columns:
            raise KeyError(f"Missing required column in df1: {col}")
        if col not in df2.columns:
            raise KeyError(f"Missing required column in df2: {col}")
    
    # Validate data types
    if not pd.api.types.is_string_dtype(df1['item_name']):
        raise TypeError(f"Field 'item_name' should be of type string.")
    if not pd.api.types.is_string_dtype(df2['item_name']):
        raise TypeError(f"Field 'item_name' should be of type string.")
    if not pd.api.types.is_string_dtype(df1['item_type']):
        raise TypeError(f"Field 'item_type' should be of type string.")
    if not pd.api.types.is_string_dtype(df2['item_type']):
        raise TypeError(f"Field 'item_type' should be of type string.")
    if not pd.api.types.is_numeric_dtype(df1['unique_id']):
        raise TypeError(f"Field 'unique_id' should be of type numeric.")
    if not pd.api.types.is_numeric_dtype(df2['unique_id']):
        raise TypeError(f"Field 'unique_id' should be of type numeric.")

    # Additional validation logic can be added here as needed
    
    print("DataFrame validation passed.")

    return df1, df2

def validate_values(df, config):
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
    
    logging.debug("Value validation and correction complete.")
    return df