
import pandas as pd
import logging
import numpy as np

def validate_dataframes(config, *dataframes):
    required_columns = ['item_name', 'item_type', 'unique_id']
    
    for df in dataframes:
        # Check for required columns
        for col in required_columns:
            if col not in df.columns:
                raise KeyError(f"Missing required column: {col}")

        # Validate data types
        if not pd.api.types.is_string_dtype(df['item_name']):
            raise TypeError(f"Field 'item_name' should be of type string.")
        if not pd.api.types.is_string_dtype(df['item_type']):
            raise TypeError(f"Field 'item_type' should be of type string.")
        if not pd.api.types.is_numeric_dtype(df['unique_id']):
            raise TypeError(f"Field 'unique_id' should be of type numeric.")

        # Call validate_values to handle NaN and other value corrections
        df = validate_values(df, config)

    print("db8️⃣ DataFrame validation passed.")
    return dataframes

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
