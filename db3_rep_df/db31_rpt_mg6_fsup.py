import pandas as pd



def remove_consolidated_columns(report_dataframe):
    # Remove extra unique_id fields


    columns_to_remove = ['unique_id_rp', 'unique_id_hm', 'unique_id_db', 'unique_id_di']
    columns_to_remove = [col for col in columns_to_remove if col in report_dataframe.columns]
    report_dataframe.drop(columns=columns_to_remove, inplace=True)

    # Remove source fields for name and type
    columns_to_remove = [
        'item_name_rp', 'item_type_rp', 'item_name_hm', 'item_type_hm',
        'item_name_rp_db', 'item_type_rp_db', 'item_name_hm_db', 'item_type_hm_db',
        'item_name_rp_di', 'item_type_rp_di', 'item_name_hm_di', 'item_type_hm_di',
        'item_name', 'item_type'
    ]
    columns_to_remove = [col for col in columns_to_remove if col in report_dataframe.columns]

    report_dataframe.drop(columns=columns_to_remove, inplace=True) # THIS IS WHERE WE LOSE THE .zsh_sessions_TEST_ITEM🔵

    return report_dataframe