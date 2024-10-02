import pandas as pd
import numpy as np

DEBUG = True
VERBOSE_DEBUG = False  # Set to True to enable verbose dictionary debug output
USE_EMOJI_DEBUG = False  # Toggle this to switch between emoji and text debug output

def make_status_match_log_dict(index, row, repo_name, home_name, repo_name_cf, home_name_cf, repo_name_db, home_name_db, dot_struc_value, m_status_result, st_match_emo):
    m_status_dict = {
        "index": index,
        "repo_matches": {"fs": False, "yaml": False, "csv": False},
        "home_matches": {"fs": False, "yaml": False, "csv": False},
        "full_dom_matches": {"repo": False, "home": False, "home_to_repo": False},
        "st_match_emo": st_match_emo  # Add the st_match_emo field
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
    if DEBUG:
        print(m_status_dict)

    return m_status_dict

def apply_matching_logic(repo_name_status, home_name_status, repo_name, home_name):
    debug_status = ""
    m_status_result = False  # Initialize to False
    match_debug = ""

    if repo_name_status == "match" and home_name_status == "match":
        if repo_name == home_name:
            dot_struc_value = 'rp>hm'
            debug_status = 'both_full_match'
            m_status_result = True  # Matching status found
            match_debug = f"rp>hm: 🟢🟢" if USE_EMOJI_DEBUG else "rp>hm: OO"
        else:
            dot_struc_value = None
            debug_status = 'names_mismatch_between_repo_and_home'
            match_debug = f"rp>hm: ❌" if USE_EMOJI_DEBUG else "rp>hm: X"
    elif repo_name_status == "match_special" and home_name_status == "none":
        dot_struc_value = 'rp'
        debug_status = 'repo_only_full_match'
        m_status_result = True  # Matching status found
        match_debug = f"rp: 🟡⚫️" if USE_EMOJI_DEBUG else "rp: o_"
    elif repo_name_status == "none" and home_name_status == "match_special":
        dot_struc_value = 'hm'
        debug_status = 'home_only_full_match'
        m_status_result = True  # Matching status found
        match_debug = f"hm: ⚫️🟡" if USE_EMOJI_DEBUG else "hm: _o"
    else:
        dot_struc_value = None
        debug_status = 'no_full_match'
        m_status_result = False  # Explicitly set to False
        match_debug = f"Match: ❌" if USE_EMOJI_DEBUG else "Match: X"

    return dot_struc_value, debug_status, m_status_result, match_debug

def print_debug_info(index, repo_name, home_name, repo_name_db, home_name_db, dot_struc_value, debug_status):
    if DEBUG:
        print(
            f"Index {index} - repo_name: {repo_name}, home_name: {home_name}, "
              f"repo_name_db: {repo_name_db}, home_name_db: {home_name_db}, "
              f"dot_struc: {dot_struc_value}, st_misc: {debug_status}"
              )

def convert_to_text_debug(debug_string):
    return debug_string.replace('⚫️', '_').replace('🟢', 'O').replace('🟡', 'o').replace('❌', 'X').replace('❓', '?')

def detect_full_domain_match(report_dataframe):
    # Initialize the new column
    report_dataframe['st_match_emo'] = ''

    for index, row in report_dataframe.iterrows():
        try:
            # Extract item names and normalize missing values
            repo_values = [
                row['item_name_rp'],
                row['item_name_rp_db'],
                row['item_name_rp_cf']
            ]
            home_values = [
                row['item_name_hm'],
                row['item_name_hm_db'],
                row['item_name_hm_cf']
            ]
            repo_values = [val if pd.notna(val) else None for val in repo_values]
            home_values = [val if pd.notna(val) else None for val in home_values]

            # Determine the first value for Repo and Home
            repo_first = repo_values[0]
            home_first = home_values[0]

            # Initialize debug emojis
            repo_debug_emojis = ''
            home_debug_emojis = ''

            # Check Repo domain matches
            repo_name_status = 'none'
            if repo_first:
                repo_name_status = 'match_special'  # Default to match_special
                for val in repo_values:
                    if val == repo_first and val is not None:
                        repo_debug_emojis += '🟢' if USE_EMOJI_DEBUG else 'O'
                    elif val is None:
                        repo_debug_emojis += '⚫️' if USE_EMOJI_DEBUG else '_'
                    else:
                        repo_debug_emojis += '❌' if USE_EMOJI_DEBUG else 'X'
                        repo_name_status = 'none'  # Mismatch, so set to none
                if all(val == repo_first and val is not None for val in repo_values):
                    repo_name_status = 'match'
            else:
                repo_debug_emojis = '⚫️⚫️⚫️' if USE_EMOJI_DEBUG else '___'

            # Check Home domain matches
            home_name_status = 'none'
            if home_first:
                home_name_status = 'match_special'  # Default to match_special
                for val in home_values:
                    if val == home_first and val is not None:
                        home_debug_emojis += '🟢' if USE_EMOJI_DEBUG else 'O'
                    elif val is None:
                        home_debug_emojis += '⚫️' if USE_EMOJI_DEBUG else '_'
                    else:
                        home_debug_emojis += '❌' if USE_EMOJI_DEBUG else 'X'
                        home_name_status = 'none'  # Mismatch, so set to none
                if all(val == home_first and val is not None for val in home_values):
                    home_name_status = 'match'
            else:
                home_debug_emojis = '⚫️⚫️⚫️' if USE_EMOJI_DEBUG else '___'

            # Apply matching logic
            dot_struc_value, debug_status, m_status_result, match_debug = apply_matching_logic(
                repo_name_status, home_name_status, repo_first, home_first
            )

            # Assign the dot_struc value if applicable
            if dot_struc_value is not None:
                report_dataframe.at[index, 'dot_struc'] = dot_struc_value

            # Assign m_status_result to the DataFrame
            report_dataframe.at[index, 'm_status_result'] = m_status_result

            # Prepare debug output
            debug_output = f"Index {index} - p_rp: {repo_debug_emojis}  p_hm: {home_debug_emojis}  {match_debug}"

            # Prepare st_match_emo content
            st_match_emo_content = f"{repo_debug_emojis} | {home_debug_emojis} | {match_debug.split(': ')[1]}"

            # Convert to text-based debug if USE_EMOJI_DEBUG is False
            if not USE_EMOJI_DEBUG:
                st_match_emo_content = convert_to_text_debug(st_match_emo_content)

            # Assign the emoji debug string to the new column
            report_dataframe.at[index, 'st_match_emo'] = st_match_emo_content

            # Print debug information
            if DEBUG:
                print(debug_output)

                # Optionally, print the verbose dictionary-based debug output
                if VERBOSE_DEBUG:
                    m_status_dict = make_status_match_log_dict(
                        index, row, repo_first, home_first, row['item_name_rp_cf'], row['item_name_hm_cf'],
                        row['item_name_rp_db'], row['item_name_hm_db'], dot_struc_value, m_status_result, st_match_emo_content
                    )
                    print(m_status_dict)

        except Exception as e:
            print(f"Error processing index {index}: {e}")

    return report_dataframe