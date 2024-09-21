import pandas as pd

def check_repo_only(report_dataframe):
    # Repo-only logic
    repo_only_condition = (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] != '')
    report_dataframe.loc[repo_only_condition, 'dot_struc'] = 'rp_only'
    
    # Debug print for repo-only condition
    print("🟨 Repo-only items:")
    print(report_dataframe[repo_only_condition][['item_name_hm', 'item_name_rp', 'dot_struc']])
    
    # Update st_alert field for repo-only items
    report_dataframe.loc[repo_only_condition, 'st_alert'] = 'Repo Only'
    
    return report_dataframe

def check_home_only(report_dataframe):
    # Home-only logic
    home_only_condition = (report_dataframe['item_name_hm'] != '') & (report_dataframe['item_name_rp'] == '')
    report_dataframe.loc[home_only_condition, 'dot_struc'] = 'hm_only'
    
    # Debug print for home-only condition
    print("Home-only items:")
    print(report_dataframe[home_only_condition][['item_name_hm', 'item_name_rp', 'dot_struc']])
    
    # Update st_alert field for home-only items
    for index, row in report_dataframe[home_only_condition].iterrows():
        if ((row['item_name_hm'] == row['item_name_hm_di']) and 
            (row['item_type_hm'] == row['item_type_hm_di'])):
            report_dataframe.at[index, 'st_alert'] = 'Home Only'
        else:
            report_dataframe.at[index, 'st_alert'] = 'New Home Item'
    
    return report_dataframe

    

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
    report_dataframe.drop(columns=columns_to_remove, inplace=True)

    return report_dataframe