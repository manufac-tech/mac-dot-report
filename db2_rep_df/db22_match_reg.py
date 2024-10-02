import pandas as pd
import numpy as np

from .db23_match_alert import normalize_missing_values, get_consistent_name


DEBUG = False


def make_status_match_log_dict(index, row, repo_name, home_name, repo_name_cf, home_name_cf, repo_name_db, home_name_db, dot_struc_value, m_status_result):
    m_status_dict = {
        "index": index,
        "repo_matches": {"fs": False, "yaml": False, "csv": False},
        "home_matches": {"fs": False, "yaml": False, "csv": False},
        "full_dom_matches": {"repo": False, "home": False, "home_to_repo": False}
    }

    # Update the status dictionary with match results
    m_status_dict["repo_matches"]["fs"] = repo_name == row['item_name_rp']
    m_status_dict["repo_matches"]["yaml"] = repo_name == row['item_name_rp_db']
    m_status_dict["repo_matches"]["csv"] = repo_name == repo_name_cf
    m_status_dict["home_matches"]["fs"] = home_name == row['item_name_hm']
    m_status_dict["home_matches"]["yaml"] = home_name == row['item_name_hm_db']
    m_status_dict["home_matches"]["csv"] = home_name == home_name_cf
    m_status_dict["full_dom_matches"]["repo"] = all(m_status_dict["repo_matches"].values())
    m_status_dict["full_dom_matches"]["home"] = all(m_status_dict["home_matches"].values())
    m_status_dict["full_dom_matches"]["home_to_repo"] = m_status_result

    # Display the dictionary in the console
    print(m_status_dict)

    return m_status_dict


def apply_matching_logic(repo_name, home_name, repo_name_cf, home_name_cf, repo_name_db, home_name_db):
    # Initialize debug status and m_status_result
    debug_status = ""
    m_status_result = False  # Initialize to False
    
    # Apply matching logic based on your requirements
    if repo_name and home_name:
        # Both Repo and Home have consistent names internally
        # Now check if names match between Repo and Home
        if repo_name == home_name:
            # Check for matching YAML entries
            if (repo_name_db == home_name_db == repo_name == home_name):
                dot_struc_value = 'rp>hm'
                debug_status = 'both_full_match'
                m_status_result = True  # Matching status found
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
                m_status_result = True  # Matching status found
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
                m_status_result = True  # Matching status found
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
        m_status_result = False  # Explicitly set to False, though already initialized
    
    return dot_struc_value, debug_status, m_status_result

def print_debug_info(index, repo_name, home_name, repo_name_db, home_name_db, dot_struc_value, debug_status):
    if DEBUG:
        print(f"Index {index} - repo_name: {repo_name}, home_name: {home_name}, "
              f"repo_name_db: {repo_name_db}, home_name_db: {home_name_db}, "
              f"dot_struc: {dot_struc_value}, st_misc: {debug_status}")


def detect_full_domain_match(report_dataframe, filter_full_matches=False, filter_any_matches=False):

    for index, row in report_dataframe.iterrows():
        try:
            # Extract item names for Repo and Home
            repo_names = [
                row['item_name_rp'],      # Repo File System
                row['item_name_rp_db']    # Repo YAML Config
            ]
            home_names = [
                row['item_name_hm'],      # Home File System
                row['item_name_hm_db']    # Home YAML Config
            ]

            # Get consistent names within Repo and Home domains
            repo_name = get_consistent_name(repo_names)
            repo_name_cf = row['item_name_rp_cf']
            home_name = get_consistent_name(home_names)
            home_name_cf = row['item_name_hm_cf']

            # YAML config names
            repo_name_db = row['item_name_rp_db']
            home_name_db = row['item_name_hm_db']

            # Apply matching logic
            dot_struc_value, debug_status, m_status_result = apply_matching_logic(
                repo_name, home_name, repo_name_cf, home_name_cf, repo_name_db, home_name_db
            )

            # Assign the dot_struc value if applicable
            if dot_struc_value is not None:
                report_dataframe.at[index, 'dot_struc'] = dot_struc_value

            # Assign m_status_result to the DataFrame
            report_dataframe.at[index, 'm_status_result'] = m_status_result

            # Create and display the status match log dictionary
            m_status_dict = make_status_match_log_dict(
                index, row, repo_name, home_name, repo_name_cf, home_name_cf,
                repo_name_db, home_name_db, dot_struc_value, m_status_result
            )

            # Assign m_status_dict to the DataFrame
            report_dataframe.at[index, 'm_status_dict'] = m_status_dict

            # Print debug information
            print_debug_info(
                index, repo_name, home_name, repo_name_db, home_name_db,
                dot_struc_value, debug_status
            )

        except Exception as e:
            print(f"Error processing index {index}: {e}")

    # Apply filter if filter_full_matches is True
    if filter_full_matches:
        report_dataframe = report_dataframe[report_dataframe['dot_struc'] != 'rp>hm']

    # Apply filter if filter_any_matches is True
    if filter_any_matches:
        report_dataframe = report_dataframe[~report_dataframe['dot_struc'].isin(['rp>hm', 'rp', 'hm'])]

    return report_dataframe