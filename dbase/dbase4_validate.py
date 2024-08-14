import logging
import numpy as np

def validate_dataframes(dot_items_df, template_df):
    """
    Validate and correct data types for both the dot items and template DataFrames.
    Args:
        dot_items_df (DataFrame): DataFrame containing dot items from the file system.
        template_df (DataFrame): DataFrame containing template data from the CSV.
    Returns:Tuple[DataFrame, DataFrame]: The validated and corrected dot items and template DataFrames.
    """
    dot_items_expected_types = {
        "fs_item_name": 'object',
        "fs_item_type": 'object',  
        "fs_unique_id": 'Int64'
    }
    
    template_expected_types = {
        "tp_item_name": 'object',
        "tp_item_type": 'object',
        "tp_cat_1": 'object',
        "tp_cat_1_name": 'object',
        "tp_comment": 'object',
        "tp_cat_2": 'object',
        "no_show": 'bool',
        "original_order": 'Int64',
        "tp_unique_id": 'Int64'
    }
    
    def validate_data_types(df, expected_types):
        """Helper function to validate and correct data types in a single DataFrame."""
        logging.info("Validating data types...")

        for column, expected_type in expected_types.items():
            # Check for data type mismatches and correct if needed
            if df[column].dtype != expected_type:
                try:
                    df[column] = df[column].astype(expected_type)
                    logging.info(f"Converted {column} to {expected_type}")
                except ValueError as e:
                    logging.warning(f"Failed to convert {column} to {expected_type}: {e}")

            # Validate specific allowed values in item_type columns if requested
            if column in ['fs_item_type', 'tp_item_type', 'item_type']:
                valid_values = ['folder', 'file', '[NO FILETYPE]']
                invalid_values = df[column][~df[column].isin(valid_values)]
                if not invalid_values.empty:
                    logging.warning(f"Invalid values found in column '{column}': {invalid_values.tolist()}")

        logging.debug("Data type validation complete.")
        return df

    # Apply the validation and correction to both DataFrames
    dot_items_df = validate_data_types(dot_items_df, dot_items_expected_types)
    template_df = validate_data_types(template_df, template_expected_types)
    
    return dot_items_df, template_df

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