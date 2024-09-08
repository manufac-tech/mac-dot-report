
import pandas as pd
from .dbase16_validate import validate_df_dict_current_and_main

current_unique_id = 1

def get_next_unique_id():
    global current_unique_id
    unique_id = current_unique_id
    current_unique_id += 1
    return unique_id

def field_merge_1_uid(main_df):
    # Case 1: Expected scenario - Home has symlink, Repo has real item, matching item names
    cond_expected = (main_df['item_name'] == main_df['item_name_rp']) & \
                    (main_df['item_type'] != main_df['item_type_rp']) & \
                    ((main_df['item_type'] == 'file_sym') & main_df['item_type_rp'].isin(['file', 'file_alias']) | \
                     (main_df['item_type'] == 'folder_sym') & main_df['item_type_rp'].isin(['folder', 'folder_alias']))
    
    # Update the unique_id to match Repo unique_id
    main_df.loc[cond_expected, 'unique_id'] = main_df['unique_id_rp']
    
    # Status update: Create a descriptive status message (e.g., folder>folder_sym or file>file_sym)
    main_df.loc[cond_expected, 'm_status_1'] = main_df['item_type_rp'] + '>' + main_df['item_type']

    # Case 2: Item exists in Repo but not in Home
    cond_repo_only = main_df['item_name'].isna()
    main_df.loc[cond_repo_only, 'unique_id'] = main_df['unique_id_rp']
    main_df.loc[cond_repo_only, 'm_status_1'] = 'ERR:repo_only'

    # Case 3: Item exists in Home but not in Repo, marked as home_only_temp (temporary until further verification)
    cond_home_only = main_df['item_name_rp'].isna()
    main_df.loc[cond_home_only, 'm_status_1'] = 'home_only_temp'

    # Case 4: Conflict scenario - Exact match of item_name and item_type, flag as conflict
    cond_same = (main_df['item_name'] == main_df['item_name_rp']) & (main_df['item_type'] == main_df['item_type_rp'])
    main_df.loc[cond_same, 'm_status_1'] = 'ERR:type=type'

    # Drop the individual suffixed unique ID fields after consolidation
    main_df.drop(columns=['unique_id_hm', 'unique_id_rp'], inplace=True)

    return main_df

def field_merge_2_uid(main_df):
    # Case 1: Full match - everything matches between YAML, home, and repo
    cond_full_match = (
        (main_df['item_name_hm'] == main_df['item_name_hm_db']) &  # Home item name matches DotBot home item
        (main_df['item_name_rp'] == main_df['item_name_rp_db']) &  # Repo item name matches DotBot repo item
        (main_df['item_type_hm'] == main_df['item_type_hm_db']) &  # Home item type matches DotBot home item type
        (main_df['item_type_rp'] == main_df['item_type_rp_db'])    # Repo item type matches DotBot repo item type
    )

    # Set m_status_2 to 'db=hm_and_rp' where all conditions match
    main_df.loc[cond_full_match, 'm_status_2'] = 'db=hm_and_rp'

    # Case 2: Handle home_only_temp, propagate this status from m_status_1
    cond_home_only_temp = main_df['m_status_1'] == 'home_only_temp'
    main_df.loc[cond_home_only_temp, 'm_status_2'] = 'home_only_temp'

    # Case 3: Error if item exists only in repo
    cond_repo_only = main_df['m_status_1'] == 'ERR:repo_only'
    main_df.loc[cond_repo_only, 'm_status_2'] = 'ERR<m1'

    # Case 4: YAML repo name exists, but repo item is missing in the file system
    cond_unmatched_yaml = (
        (main_df['item_name_rp_db'].notna()) &  # YAML repo name exists
        (main_df['item_name_rp'].isna())        # Repo item is missing in the file system
    )

    # Set error status for unmatched YAML items
    main_df.loc[cond_unmatched_yaml, 'm_status_2'] = 'ERR:unmatched_yaml'

    # Case 5: Type mismatch error between home and repo (e.g., file vs folder)
    cond_type_mismatch = main_df['m_status_1'] == 'ERR:type=type'
    main_df.loc[cond_type_mismatch, 'm_status_2'] = 'ERR<m1'

    # Case 6: YAML source and destination paths do not match
    cond_yaml_src_dest_mismatch = main_df['item_name_hm_db'] != main_df['item_name_rp_db']
    main_df.loc[cond_yaml_src_dest_mismatch, 'm_status_2'] = 'YAML SRC<>DEST'

    # Case 7: File type mismatch between YAML and actual file system
    cond_file_type_mismatch = (
        (main_df['item_type_rp_db'] != main_df['item_type_rp']) |  # Repo file type mismatch
        (main_df['item_type_hm_db'] != main_df['item_type_hm'])    # Home file type mismatch
    )
    main_df.loc[cond_file_type_mismatch, 'm_status_2'] = 'ERR:file_type_mismatch'

    # Drop the individual suffixed unique ID fields after consolidation
    main_df.drop(columns=['unique_id_db'], inplace=True)

    # Set global item_type based on m_status_1 and m_status_2
    cond_valid_type_update = (
        (main_df['m_status_1'].str.contains('>') & (main_df['m_status_2'] == 'db=hm_and_rp'))
    )

    # Update the global item_type using item_type_rp if conditions are met
    main_df.loc[cond_valid_type_update, 'item_type'] = main_df['item_type_rp']

    # For any other case where item_type cannot be determined, set it to 'unknown'
    main_df.loc[~cond_valid_type_update, 'item_type'] = 'unknown'

    return main_df

def field_merge_3_uid(main_df):
    # Case 1: Full match check for item_name and item_type between dot_info and main_dataframe
    cond_full_match = (
        (main_df['item_name'] == main_df['item_name_di']) &  # item_name matches dot_info
        (main_df['item_type'] == main_df['item_type_di'])    # item_type matches dot_info
    )

    # Set m_status_3 to 'di_match' where all conditions match
    main_df.loc[cond_full_match, 'm_status_3'] = 'di_match'

    # Case: Check for home_only_temp status
    cond_home_only_temp = (main_df['m_status_2'] == 'home_only_temp')

    # If the item is marked as 'hm_only' in dot_info, it's a valid dynamic config
    main_df.loc[cond_home_only_temp & (main_df['dot_items_hm'] == 'hm_only'), 'm_status_3'] = 'di_match'

    # If the item is not marked as 'hm_only', it's a new item in the home folder
    main_df.loc[cond_home_only_temp & (main_df['dot_items_hm'].isna()), 'm_status_3'] = 'new_home_only'

    # Case 2: Check for file type mismatch between the dot_info and the main_dataframe
    cond_type_mismatch = (
        (main_df['item_name'] == main_df['item_name_di']) &  # Names match
        (main_df['item_type'].notna()) &                     # Ensure non-NaN types in main_dataframe
        (main_df['item_type_di'].notna()) &                  # Ensure non-NaN types in dot_info
        (main_df['item_type'] != main_df['item_type_di'])    # Types mismatch
    )

    # Set m_status_3 to 'ERR:file_type_mismatch' where the condition is true
    # main_df.loc[cond_type_mismatch, 'm_status_3'] = 'ERR:file_type_mismatch'

    # Case 3: Handle rows where item_name_di is unmatched in the main_dataframe
    cond_unmatched_di = (
        (main_df['item_name_di'].notna()) &  # dot_info item_name exists
        (main_df['item_name'].isna())        # main_dataframe item_name is missing
    )

    # Apply error status for unmatched dot_info items
    main_df.loc[cond_unmatched_di, 'm_status_3'] = 'ERR:unmatched_di'

    # Update the unique_id using the unique_id_di (as it's the only available ID)
    main_df.loc[cond_unmatched_di, 'unique_id'] = main_df['unique_id_di']

    # Case 4: Check for previous error propagation from m_status_2
    cond_prev_error = (
        main_df['m_status_2'].notna() &                            # Ensure non-NaN in m_status_2
        main_df['m_status_2'].str.startswith('ERR')                # Check if m_status_2 contains an error
    )

    # Set m_status_3 to propagate the previous error
    main_df.loc[cond_prev_error, 'm_status_3'] = 'ERR<m2'

    # Handle NA values in original_order (fill with a placeholder like -1)
    main_df['original_order'] = main_df['original_order'].fillna(-1)

    # Drop the individual suffixed unique ID fields after consolidation
    main_df.drop(columns=['unique_id_di'], inplace=True)

    return main_df