import pandas as pd
from colorama import init, Fore, Style
from db1_main_df.db03_dtype_dict import field_types_with_defaults, get_valid_types
from .db28_rpt_mg3_oth import write_st_alert_value

# Initialize Colorama
init(autoreset=True)

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
        report_dataframe = check_discrepancies(report_dataframe, valid_types_repo, valid_types_home)
    except Exception as e:
        print(f"Error in check_discrepancies: {e}")

    return report_dataframe

def alert_sym_overwrite(report_dataframe):
    # Check if item names match between repo and home
    name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_hm'])

    # Check if types differ in a way that suggests a symlink has been overwritten
    type_mismatch = (
        (report_dataframe['item_type_rp'] == 'file') & (report_dataframe['item_type_hm'] != 'file_sym')
    ) | (
        (report_dataframe['item_type_rp'] == 'folder') & (report_dataframe['item_type_hm'] != 'folder_sym')
    )

    # Append 'SymLink Overwrite' if names match but types differ as described above
    for index in report_dataframe[name_match & type_mismatch].index:
        report_dataframe = write_st_alert_value(report_dataframe, index, f"{Fore.RED}SymLink Overwrite{Style.RESET_ALL}")

    return report_dataframe

def check_no_fs_match(report_dataframe, valid_types_repo, valid_types_home):
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

def alert_in_doc_not_fs(report_dataframe):
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
    # NO FS MATCH-N (Name) logic
    no_fs_match_n = (
        (report_dataframe['item_name_rp_di'] != '') & (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == '')
    ) | (
        (report_dataframe['item_name_hm_di'] != '') & (report_dataframe['item_name_hm'] == '') & (report_dataframe['item_name_rp'] == '')
    )
    report_dataframe.loc[no_fs_match_n, 'dot_struc'] = 'no_fs_N'

    # NO FS MATCH-T (Type) logic
    name_match = (report_dataframe['item_name_rp'] == report_dataframe['item_name_hm'])
    type_mismatch = (
        (report_dataframe['item_type_rp'].isin(valid_types_repo['file']) & (report_dataframe['item_type_hm'] != valid_types_home['file'])) |
        (report_dataframe['item_type_rp'].isin(valid_types_repo['folder']) & (report_dataframe['item_type_hm'] != valid_types_home['folder']))
    )
    no_fs_match_t = name_match & type_mismatch
    report_dataframe.loc[no_fs_match_t, 'dot_struc'] = 'no_fs_T'

    # New Home Item logic (item in home but not in repo)
    new_home_item = (report_dataframe['item_name_hm'].notna()) & (report_dataframe['item_name_rp'].isna())
    # report_dataframe.loc[new_home_item, 'dot_struc'] = 'New Home Item'

    # Update st_alert field with relevant alerts
    combined_conditions = no_fs_match_n | no_fs_match_t | new_home_item
    for index in report_dataframe[combined_conditions].index:
        if no_fs_match_n.loc[index]:
            report_dataframe = write_st_alert_value(report_dataframe, index, f"{Fore.RED}FS-Doc Mismatch{Style.RESET_ALL}")
        elif no_fs_match_t.loc[index]:
            report_dataframe = write_st_alert_value(report_dataframe, index, f"{Fore.RED}FS-Doc Mismatch{Style.RESET_ALL}")
        elif new_home_item.loc[index]:
            report_dataframe = write_st_alert_value(report_dataframe, index, f"{Fore.YELLOW}New Home Item{Style.RESET_ALL}")

    return report_dataframe