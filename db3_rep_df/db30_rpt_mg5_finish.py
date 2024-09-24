import pandas as pd

from .db31_rpt_mg6_fsup import remove_consolidated_columns
from .db26_rpt_mg1_mast import get_conditions_actions

def check_name_consistency(row):
    # Collect all non-NaN names
    names = [name for name in [row['item_name_rp_db'], row['item_name_hm_db'], 
                               row['item_name_rp_di'], row['item_name_hm_di']] if pd.notna(name)]
    
    # Check for conflict
    if len(names) <= 1:
        return False
    return len(set(names)) > 1  # True if names are different, False otherwise

def merge_logic(row):
    # For repo, we prioritize db over di
    if pd.notna(row['item_name_rp_db']):
        row['item_name_repo'] = row['item_name_rp_db']
    else:
        row['item_name_repo'] = row['item_name_rp_di']
    
    # For home, we prioritize db over di
    if pd.notna(row['item_name_hm_db']):
        row['item_name_home'] = row['item_name_hm_db']
    else:
        row['item_name_home'] = row['item_name_hm_di']
    
    return row

def check_doc_names_no_fs(df):
    # Check for document names without corresponding file system names
    doc_names_no_fs_condition = (
        ((df['item_name_rp_db'].notna()) | (df['item_name_hm_db'].notna()) |
         (df['item_name_rp_di'].notna()) | (df['item_name_hm_di'].notna())) &
        (df['item_name_rp'].isna()) & (df['item_name_hm'].isna())
    )

    # Set the st_misc field to 'doc_no_fs' for any row that matches the condition
    df.loc[doc_names_no_fs_condition, 'st_misc'] = 'doc_no_fs'

    # Check name consistency and update st_docs field
    df.loc[doc_names_no_fs_condition, 'st_docs'] = df[doc_names_no_fs_condition].apply(
        lambda row: 'error' if check_name_consistency(row) else 'ok', axis=1
    )

    # Apply merge logic to populate item_name_repo and item_name_home
    df = df.apply(merge_logic, axis=1)

    return df

def consolidate_fields(report_dataframe):
    """
    Consolidates item_name_repo, item_type_repo, item_name_home, item_type_home, and unique_id based on match statuses.
    Sets 'st_misc' to 'x' if any unique ID gets copied to the actual unique_id.
    """

    # blank_symbol = 'x'
    # blank_symbol = '|____'
    # blank_symbol = '_____'
    blank_symbol = ''
    # blank_symbol = '-'
    
    # Get the conditions and corresponding actions from the old system
    conditions_actions = get_conditions_actions(report_dataframe)

    # Apply the conditions and actions from the old system
    for key, value in conditions_actions.items():
        condition = value['condition']
        actions = value['actions']
        for target_field, source_field in actions.items():
            if target_field == 'sort_out':
                report_dataframe.loc[condition, target_field] = source_field
            elif source_field is not None:
                report_dataframe.loc[condition, target_field] = report_dataframe[source_field]
            else:
                report_dataframe.loc[condition, target_field] = pd.NA

    # Apply dynamic consolidation based on match_dict
    # report_dataframe = apply_dynamic_consolidation(report_dataframe)

    # INSERT "check_doc_names_no_fs()" function call here
    report_dataframe = check_doc_names_no_fs(report_dataframe)

    # Call the new function to remove unnecessary columns
    report_dataframe = remove_consolidated_columns(report_dataframe)


    return report_dataframe

def apply_dynamic_consolidation(report_dataframe):
    """
    Applies dynamic consolidation based on the match_dict field.
    """
    for index, row in report_dataframe.iterrows():
        match_dict = row['match_dict']
        
        # Example logic for consolidating fields using match_dict column
        if match_dict.get('check_doc_names_no_fs', {}).get('item_name_rp_db', False):
            report_dataframe.at[index, 'item_name_repo'] = row['item_name_rp']
            report_dataframe.at[index, 'item_type_repo'] = row['item_type_rp']
        else:
            report_dataframe.at[index, 'item_name_repo'] = pd.NA
            report_dataframe.at[index, 'item_type_repo'] = pd.NA

        if match_dict.get('check_doc_names_no_fs', {}).get('item_name_hm_db', False):
            report_dataframe.at[index, 'item_name_home'] = row['item_name_hm']
            report_dataframe.at[index, 'item_type_home'] = row['item_type_hm']
        else:
            report_dataframe.at[index, 'item_name_home'] = pd.NA
            report_dataframe.at[index, 'item_type_home'] = pd.NA

        report_dataframe.at[index, 'unique_id'] = row['unique_id_rp']
        report_dataframe.at[index, 'sort_out'] = 25

    return report_dataframe