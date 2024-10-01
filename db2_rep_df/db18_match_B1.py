import pandas as pd
import numpy as np

def normalize_missing_values(df, columns):
    for col in columns:
        # Convert to string and strip whitespace
        df[col] = df[col].astype(str).str.strip()
        # Replace common missing value representations with np.nan
        df[col] = df[col].replace(
            to_replace=['nan', '<NA>', 'NaN', 'None', 'NoneType', ''],
            value=np.nan
        )
    return df

def get_consistent_name(names):
    # Remove NaNs
    names_filtered = [x for x in names if pd.notna(x)]
    # If list is empty after removing NaNs, return None
    if not names_filtered:
        return None
    # Check if all non-NaN values are equal
    if all(x == names_filtered[0] for x in names_filtered):
        return names_filtered[0]  # Return the consistent name
    else:
        return None  # Names are inconsistent within the domain

def apply_matching_logic(repo_name, home_name, repo_name_cf, home_name_cf, repo_name_db, home_name_db):
    # Initialize debug status
    debug_status = ""

    # Apply matching logic based on your requirements
    if repo_name and home_name:
        # Both Repo and Home have consistent names internally
        # Now check if names match between Repo and Home
        if repo_name == home_name:
            # Check for matching YAML entries
            if (repo_name_db == home_name_db == repo_name == home_name):
                dot_struc_value = 'rp>hm'
                debug_status = 'both_full_match'
            else:
                dot_struc_value = None  # YAML entries missing or inconsistent
                debug_status = 'yaml_entries_missing_or_mismatch_for_both'
        else:
            dot_struc_value = None  # Names between Repo and Home do not match
            debug_status = 'names_mismatch_between_repo_and_home'
    elif repo_name and not home_name:
        # Only Repo has consistent names
        # Check that Repo File System and CSV match
        if repo_name == repo_name_cf:
            # YAML entries should be missing
            if pd.isna(repo_name_db) and pd.isna(home_name_db):
                dot_struc_value = 'rp'
                debug_status = 'repo_only_full_match'
            else:
                dot_struc_value = None  # Unexpected YAML entries
                debug_status = 'unexpected_yaml_entries_for_repo_only'
        else:
            dot_struc_value = None  # Names in Repo File System and CSV do not match
            debug_status = 'names_mismatch_in_repo'
    elif home_name and not repo_name:
        # Only Home has consistent names
        # Check that Home File System and CSV match
        if home_name == home_name_cf:
            # YAML entries should be missing
            if pd.isna(repo_name_db) and pd.isna(home_name_db):
                dot_struc_value = 'hm'
                debug_status = 'home_only_full_match'
            else:
                dot_struc_value = None  # Unexpected YAML entries
                debug_status = 'unexpected_yaml_entries_for_home_only'
        else:
            dot_struc_value = None  # Names in Home File System and CSV do not match
            debug_status = 'names_mismatch_in_home'
    else:
        # Neither Repo nor Home have consistent names or are missing
        dot_struc_value = None
        debug_status = 'no_full_match'

    return dot_struc_value, debug_status

def print_debug_info(index, repo_name, home_name, repo_name_db, home_name_db, dot_struc_value, debug_status):
    print(f"Index {index} - repo_name: {repo_name}, home_name: {home_name}, "
          f"repo_name_db: {repo_name_db}, home_name_db: {home_name_db}, "
          f"dot_struc: {dot_struc_value}, st_misc: {debug_status}")

def detect_full_domain_match(report_dataframe):
    # [Normalization code remains the same]

    for index, row in report_dataframe.iterrows():
        try:
            # Extract item names for Repo and Home
            repo_names = [
                row['item_name_rp'],      # Repo File System
                row['item_name_rp_cf'],   # Repo CSV Config
                row['item_name_rp_db']    # Repo YAML Config
            ]
            home_names = [
                row['item_name_hm'],      # Home File System
                row['item_name_hm_cf'],   # Home CSV Config
                row['item_name_hm_db']    # Home YAML Config
            ]

            # Get consistent names within Repo and Home domains
            repo_name = get_consistent_name([row['item_name_rp'], row['item_name_rp_db']])
            repo_name_cf = row['item_name_rp_cf']
            home_name = get_consistent_name([row['item_name_hm'], row['item_name_hm_db']])
            home_name_cf = row['item_name_hm_cf']

            # YAML config names
            repo_name_db = row['item_name_rp_db']
            home_name_db = row['item_name_hm_db']

            # Apply matching logic
            dot_struc_value, debug_status = apply_matching_logic(
                repo_name, home_name, repo_name_cf, home_name_cf, repo_name_db, home_name_db
            )

            # Assign the dot_struc value if applicable
            if dot_struc_value is not None:
                report_dataframe.at[index, 'dot_struc'] = dot_struc_value

            # Update the st_misc field with the debug status
            report_dataframe.at[index, 'st_misc'] = debug_status

            # Print debug information
            print_debug_info(index, repo_name, home_name, repo_name_db, home_name_db, dot_struc_value, debug_status)

        except Exception as e:
            print(f"Error processing index {index}: {e}")

    return report_dataframe