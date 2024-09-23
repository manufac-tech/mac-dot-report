import pandas as pd
from colorama import init, Fore, Style
from db1_main_df.db03_dtype_dict import f_types_vals, get_valid_types
from .db28_rpt_mg3_oth import write_st_alert_value

def field_match_2_alert(report_dataframe):
    
    valid_types_repo, valid_types_home = get_valid_types()

    try: # Check for Symlink Overwrite condition: rp/hm match but same (actual) type. (ALERT)
        report_dataframe = alert_sym_overwrite(report_dataframe)
    except Exception as e:
        print(f"Error in alert_sym_overwrite: {e}")

    # try: # Check for Doc Items with no matching filesystem items (ALERT)
    #     report_dataframe = alert_in_doc_not_fs(report_dataframe)
    # except Exception as e:
    #     print(f"Error in alert_in_doc_not_fs: {e}")

    # try: # Check for no file system match
    #     report_dataframe = check_no_fs_match(report_dataframe, valid_types_repo, valid_types_home)
    # except Exception as e:
    #     print(f"Error in check_no_fs_match: {e}")

    try: # Check for no file system match
        report_dataframe = check_no_fs(report_dataframe)
    except Exception as e:
        print(f"Error in check_no_fs: {e}")

    return report_dataframe

def alert_sym_overwrite(report_dataframe):
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

def check_no_fs(report_dataframe):
    """
    Function to check for items present in YAML/CSV but missing in the filesystem (repo/home).
    Updates 'st_alert' and 'st_misc' fields with the appropriate 'No FS' message.
    Prints the number of rows that match each condition for debugging purposes.
    """

    # Define common conditions for checking presence in YAML/CSV and absence in the filesystem
    in_repo_doc = (report_dataframe['item_name_rp_db'] != "") | (report_dataframe['item_name_rp_di'] != "")
    in_home_doc = (report_dataframe['item_name_hm_db'] != "") | (report_dataframe['item_name_hm_di'] != "")
    missing_in_fs = (report_dataframe['item_name_rp'] == "") & (report_dataframe['item_name_hm'] == "")
    
    # Condition: Check YAML/CSV Repo vs Filesystem (Home/Repo)
    condition_repo = in_repo_doc & missing_in_fs
    print(f"🟡 Condition Repo: {condition_repo.sum()} rows matched")  # Debug output
    for index in report_dataframe[condition_repo].index:
        report_dataframe = write_st_alert_value(report_dataframe, index, 'doc rp - No FS')
        report_dataframe.loc[index, 'st_misc'] = 'di_rp≠FS' if report_dataframe.loc[index, 'item_name_rp_di'] != "" else 'db_rp≠FS'

    # Condition: Check YAML/CSV Home vs Filesystem (Home/Repo)
    condition_home = in_home_doc & missing_in_fs
    print(f"🟡 Condition Home: {condition_home.sum()} rows matched")  # Debug output
    for index in report_dataframe[condition_home].index:
        report_dataframe = write_st_alert_value(report_dataframe, index, 'doc hm - No FS')
        report_dataframe.loc[index, 'st_misc'] = 'di_hm≠FS' if report_dataframe.loc[index, 'item_name_hm_di'] != "" else 'db_hm≠FS'

    # Cross-matching - Check if both repo and home are missing in FS for YAML/CSV
    condition_cross_match = in_repo_doc & in_home_doc & missing_in_fs
    print(f"🟡 Condition Cross-Match: {condition_cross_match.sum()} rows matched")  # Debug output
    for index in report_dataframe[condition_cross_match].index:
        report_dataframe = write_st_alert_value(report_dataframe, index, 'doc rp/hm - No FS')
        report_dataframe.loc[index, 'st_misc'] = 'di_rh≠FS' if report_dataframe.loc[index, 'item_name_rp_di'] != "" else 'db_rh≠FS'

    return report_dataframe


def check_no_fs_match(report_dataframe, valid_types_repo, valid_types_home): # STANDALONE 2 - simply called above.
    pass
    # valid_types_repo, valid_types_home = get_valid_types()

    # # NO FS MATCH-N (Name) logic
    # no_fs_match_n = (
    #     (report_dataframe['item_name_rp_di'] != '') & (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == '')
    # ) | (
    #     (report_dataframe['item_name_hm_di'] != '') & (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == '')
    # )
    # report_dataframe.loc[no_fs_match_n, 'dot_struc'] = 'no_fs_N'

    # # NO FS MATCH-T (Type) logic
    # name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_hm'])
    # type_mismatch = (
    #     (report_dataframe['item_type_rp'].isin(valid_types_repo['file']) & (report_dataframe['item_type_hm'] != valid_types_home['file'])) |
    #     (report_dataframe['item_type_rp'].isin(valid_types_repo['folder']) & (report_dataframe['item_type_hm'] != valid_types_home['folder']))
    # )
    # no_fs_match_t = name_match & type_mismatch
    # report_dataframe.loc[no_fs_match_t, 'dot_struc'] = 'no_fs_T'

    # # Update st_alert field with "FS-Doc Mismatch" for no_fs_match_n and no_fs_match_t
    # for index in report_dataframe[no_fs_match_n | no_fs_match_t].index:
    #     report_dataframe = write_st_alert_value(report_dataframe, index, 'FS-Doc Mismatch')

    return report_dataframe

def alert_in_doc_not_fs(report_dataframe): # STANDALONE 1 - simply called above.
    pass
    # condition = (report_dataframe['item_name_hm'].notna()) & (report_dataframe['item_name_rp'].isna())
    
    # # Debug: Print the condition results
    # # print("Condition (item_name_hm.notna() & item_name_rp.isna()):")
    # # print(condition)
    
    # # WHY IS THIS HERE?
    # for index in report_dataframe[condition].index:
    #     report_dataframe = write_st_alert_value(report_dataframe, index, 'New Home Item')
    
    return report_dataframe

def check_discrepancies(report_dataframe, valid_types_repo, valid_types_home):
    pass
    # NO FS MATCH-N (Name) logic
    # no_fs_match_n = (
    #     (report_dataframe['item_name_rp_di'] != '') & (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == '')
    # ) | (
    #     (report_dataframe['item_name_hm_di'] != '') & (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == '')
    # )
    # report_dataframe.loc[no_fs_match_n, 'dot_struc'] = 'no_fs_N'

    # # NO FS MATCH-T (Type) logic
    # name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_hm'])
    # type_mismatch = (
    #     (report_dataframe['item_type_rp'].isin(valid_types_repo['file']) & (report_dataframe['item_type_hm'] != valid_types_home['file'])) |
    #     (report_dataframe['item_type_rp'].isin(valid_types_repo['folder']) & (report_dataframe['item_type_hm'] != valid_types_home['folder']))
    # )
    # no_fs_match_t = name_match & type_mismatch
    # report_dataframe.loc[no_fs_match_t, 'dot_struc'] = 'no_fs_T'

    # # New Home Item logic (item in home but not in repo)
    # new_home_item = (report_dataframe['item_name_hm'].notna()) & (report_dataframe['item_name_rp'].isna())
    # # report_dataframe.loc[new_home_item, 'dot_struc'] = 'New Home Item'

    # # Update st_alert field with relevant alerts
    # combined_conditions = no_fs_match_n | no_fs_match_t | new_home_item
    # for index in report_dataframe[combined_conditions].index:
    #     if no_fs_match_n.loc[index]:
    #         report_dataframe = write_st_alert_value(report_dataframe, index, f"{Fore.RED}FS-Doc Mismatch{Style.RESET_ALL}")
    #     elif no_fs_match_t.loc[index]:
    #         report_dataframe = write_st_alert_value(report_dataframe, index, f"{Fore.RED}FS-Doc Mismatch{Style.RESET_ALL}")
    #     elif new_home_item.loc[index]:
    #         report_dataframe = write_st_alert_value(report_dataframe, index, f"{Fore.YELLOW}New Home Item{Style.RESET_ALL}")

    return report_dataframe