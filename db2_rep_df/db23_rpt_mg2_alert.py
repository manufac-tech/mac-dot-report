import pandas as pd
# from colorama import init, Fore, Style
from db5_global.db52_dtype_dict import f_types_vals, get_valid_item_types
from .db24_rpt_mg3_oth import write_st_alert_value
from .db25_rpt_mg5_finish import get_field_merge_rules

def field_match_2_alert(report_dataframe, field_merge_rules_dyna):
    
    valid_types_repo, valid_types_home = get_valid_item_types()

    try: # Check for Symlink Overwrite condition: rp/hm match but same (actual) type. (ALERT)
        report_dataframe = fm_fm_alert_sym_overwrite(report_dataframe)
    except Exception as e:
        print(f"Error in fm_alert_sym_overwrite: {e}")

    try: # Check for items in any doc, but not in filesystem
        report_dataframe = check_doc_names_no_fs(report_dataframe, field_merge_rules_dyna)
    except Exception as e:
        print(f"Error in check_doc_no_fs: {e}")


    return report_dataframe

def fm_fm_alert_sym_overwrite(report_dataframe):
    # Check if item names match between repo and home
    name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_hm'])

    # Check if home item is not a symlink and repo item is a file or folder
    valid_repo_file = report_dataframe['item_type_rp'] == 'file'
    valid_repo_folder = report_dataframe['item_type_rp'] == 'folder'

    home_is_not_file_sym = report_dataframe['item_type_hm'].notna() & (report_dataframe['item_type_hm'] != 'file_sym')
    home_is_not_folder_sym = report_dataframe['item_type_hm'].notna() & (report_dataframe['item_type_hm'] != 'folder_sym')

    # Check if types differ in a way that suggests a symlink has been overwritten
    type_mismatch = (valid_repo_file & home_is_not_file_sym) | (valid_repo_folder & home_is_not_folder_sym)

    # Combine name match with type mismatch
    condition = name_match & type_mismatch

    # Append 'SymLink Overwrite' if names match but types differ as described above
    for index in report_dataframe[condition].index:
        report_dataframe = write_st_alert_value(report_dataframe, index, 'SymLink Overwrite')

    return report_dataframe

def check_name_consistency(row):
    # Collect all non-NaN names
    names = [name for name in [row['item_name_rp_db'], row['item_name_hm_db'], 
                               row['item_name_rp_di'], row['item_name_hm_di']] if pd.notna(name)]
    
    # Check for conflict
    if len(names) <= 1:
        return 'consistent'
    return 'Multiple Names' if len(set(names)) > 1 else 'consistent'

def doc_no_fs_merge_logic(row):
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

def check_doc_names_no_fs(report_dataframe, field_merge_rules_dyna):
    # Check for document names without corresponding file system names
    doc_names_no_fs_condition = (
        ((report_dataframe['item_name_rp_db'].notna()) | (report_dataframe['item_name_hm_db'].notna()) |
         (report_dataframe['item_name_rp_di'].notna()) | (report_dataframe['item_name_hm_di'].notna())) &
        (report_dataframe['item_name_rp'].isna()) & (report_dataframe['item_name_hm'].isna())
    )

    # Set the st_misc field to 'doc_no_fs' for any row that matches the condition
    report_dataframe.loc[doc_names_no_fs_condition, 'st_misc'] = 'doc_no_fs'

    # Check name consistency and update st_alert field
    for index, row in report_dataframe[doc_names_no_fs_condition].iterrows():
        name_consistency_status = check_name_consistency(row)
        if name_consistency_status == 'Multiple Names':
            report_dataframe = write_st_alert_value(report_dataframe, index, name_consistency_status)

    # Apply merge logic to populate item_name_repo and item_name_home
    report_dataframe = report_dataframe.apply(doc_no_fs_merge_logic, axis=1)

    # Dynamically update the field_merge_rules_dyna dictionary
    for index, row in report_dataframe[doc_names_no_fs_condition].iterrows():
        field_merge_rules_dyna[f'in_doc_not_fs_{index}'] = {
            'condition': report_dataframe.index == index,
            'actions': {
                'item_name_repo': row['item_name_rp_db'] if pd.notna(row['item_name_rp_db']) else row['item_name_rp_di'],
                'item_type_repo': row['item_type_rp_db'] if pd.notna(row['item_type_rp_db']) else row['item_type_rp_di'],
                'item_name_home': row['item_name_hm_db'] if pd.notna(row['item_name_hm_db']) else row['item_name_hm_di'],
                'item_type_home': row['item_type_hm_db'] if pd.notna(row['item_type_hm_db']) else row['item_type_hm_di'],
                'unique_id': row['unique_id_rp'],
                'sort_out': 25,
                'st_alert': 'In Doc Not FS'
            }
        }

    # Print the dynamic conditions to the terminal
    # print("Dynamic Conditions:")
    # for key, value in field_merge_rules_dyna.items():
    #     print(f"{key}: {value}")

    return report_dataframe

