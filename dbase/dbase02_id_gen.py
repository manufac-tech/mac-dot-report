
import pandas as pd
from .dbase08_validate import validate_df_dict_current_and_main

current_unique_id = 1

def get_next_unique_id():
    global current_unique_id
    unique_id = current_unique_id
    current_unique_id += 1
    return unique_id

def field_merge_1_uid(main_df):
    # Case 1: Expected scenario - item_name matches, and home has a symlink, while repo has a real item
    cond_expected = (main_df['item_name'] == main_df['item_name_rp']) & \
                    (main_df['item_type'] != main_df['item_type_rp']) & \
                    ((main_df['item_type'] == 'file_sym') & main_df['item_type_rp'].isin(['file', 'file_alias']) | \
                     (main_df['item_type'] == 'folder_sym') & main_df['item_type_rp'].isin(['folder', 'folder_alias']))
    
    # Update the unique_id based on repo's unique_id
    main_df.loc[cond_expected, 'unique_id'] = main_df['unique_id_rp']
    
    # Create a more descriptive status message (e.g., folder>folder_sym or file>file_sym)
    main_df.loc[cond_expected, 'm_status_1'] = main_df['item_type_rp'] + '>' + main_df['item_type']

    # Case 2: Item exists in repo but not in home
    cond_repo_only = main_df['item_name'].isna()
    main_df.loc[cond_repo_only, 'unique_id'] = main_df['unique_id_rp']
    main_df.loc[cond_repo_only, 'm_status_1'] = 'ERR:repo_only'

    # Case 3: Item exists in home but not in repo
    cond_home_only = main_df['item_name_rp'].isna()
    main_df.loc[cond_home_only, 'm_status_1'] = 'ERR:home_only'

    # Case 4: Conflict - Exact match of item_name and item_type should be flagged
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

    # Case 2: Error if items are missing in the file system (home or repo missing)
    cond_fs_missing = (
        (main_df['m_status_1'] == 'ERR:home_only') |  # Error if file exists only in home
        (main_df['m_status_1'] == 'ERR:repo_only')    # Error if file exists only in repo
    )
    
    # Reference the existing error in m_status_1
    main_df.loc[cond_fs_missing, 'm_status_2'] = 'ERR<m1'

    # Case 3: YAML repo name exists, but repo item is missing in the file system
    cond_unmatched_yaml = (
        (main_df['item_name_rp_db'].notna()) &  # YAML repo name exists
        (main_df['item_name_rp'].isna())        # Repo item is missing in the file system
    )

    # Set error status for unmatched YAML items
    main_df.loc[cond_unmatched_yaml, 'm_status_2'] = 'ERR:unmatched_yaml'

    # Case 4: Type mismatch error between home and repo (e.g., file vs folder)
    cond_type_mismatch = main_df['m_status_1'] == 'ERR:type=type'
    main_df.loc[cond_type_mismatch, 'm_status_2'] = 'ERR<m1'

    # Case 5: YAML source and destination paths do not match
    cond_yaml_src_dest_mismatch = main_df['item_name_hm_db'] != main_df['item_name_rp_db']
    main_df.loc[cond_yaml_src_dest_mismatch, 'm_status_2'] = 'YAML SRC<>DEST'

    # Case 6: File type mismatch between YAML and actual file system
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
    # Case 1: Full match check for item_name and item_type between template and main DataFrame
    cond_full_match = (
        (main_df['item_name'] == main_df['item_name_tp']) &  # item_name matches template
        (main_df['item_type'] == main_df['item_type_tp'])    # item_type matches template
    )

    # Set m_status_3 to 'tp=hm_and_rp' where all conditions match
    main_df.loc[cond_full_match, 'm_status_3'] = 'tp_match'

    # Case 2: Check for file type mismatch between the template and the main DataFrame
    cond_type_mismatch = (
        (main_df['item_name'] == main_df['item_name_tp']) &  # Names match
        (main_df['item_type'].notna()) &                     # Ensure non-NaN types in main DataFrame
        (main_df['item_type_tp'].notna()) &                  # Ensure non-NaN types in template
        (main_df['item_type'] != main_df['item_type_tp'])    # Types mismatch
    )

    # Set m_status_3 to 'ERR:file_type_mismatch' where the condition is true
    main_df.loc[cond_type_mismatch, 'm_status_3'] = 'ERR:file_type_mismatch'

    # Case 3: Handle rows where item_name_tp is unmatched in the main DataFrame
    cond_unmatched_tp = (
        (main_df['item_name_tp'].notna()) &  # Template item_name exists
        (main_df['item_name'].isna())        # Main DataFrame item_name is missing
    )

    # Apply error status for unmatched template items
    main_df.loc[cond_unmatched_tp, 'm_status_3'] = 'ERR:unmatched_tp'

    # Update the unique_id using the unique_id_tp (as it's the only available ID)
    main_df.loc[cond_unmatched_tp, 'unique_id'] = main_df['unique_id_tp']

    # Case 4: Check for previous error propagation from m_status_2
    cond_prev_error = (
        main_df['m_status_2'].notna() &                            # Ensure non-NaN in m_status_2
        main_df['m_status_2'].str.startswith('ERR')                # Check if m_status_2 contains an error
    )

    # Set m_status_3 to propagate the previous error
    main_df.loc[cond_prev_error, 'm_status_3'] = 'ERR<m2'

    # Drop the individual suffixed unique ID fields after consolidation
    main_df.drop(columns=['unique_id_tp'], inplace=True)

    return main_df