import pandas as pd
import numpy as np

DEBUG = False  # Set to True to enable verbose dictionary debug output
DISPLAY_MATCH_DETAIL_IN_ST_MISC = False  # Set to True to include debug status in st_misc field

def assign_debug_characters(values, first_value):
    debug_characters = ''
    name_status = 'none'
    if first_value:
        name_status = 'match_special'  # Default to match_special
        for val in values:
            if val == first_value and val is not None:
                debug_characters += 'O'
            elif val is None:
                debug_characters += '_'
            else:
                debug_characters += 'X'
                name_status = 'none'  # Mismatch, so set to none
        if all(val == first_value and val is not None for val in values):
            name_status = 'match'
    else:
        debug_characters = '___'
    return debug_characters, name_status

def apply_matching_logic(repo_name_status, home_name_status, repo_name, home_name):
    debug_status = ""
    m_status_result = False  # Initialize to False
    match_debug = ""

    if repo_name_status == "match" and home_name_status == "match":
        if repo_name == home_name:
            dot_struc_value = 'rp>hm'
            debug_status = 'both_full_match'
            m_status_result = True  # Matching status found
            match_debug = "rp>hm: OO"
        else:
            dot_struc_value = None
            debug_status = 'names_mismatch_between_repo_and_home'
            match_debug = "rp>hm: XX"
    elif repo_name_status == "match_special" and home_name_status == "none":
        dot_struc_value = 'rp'
        debug_status = 'repo_only_full_match'
        m_status_result = True  # Matching status found
        match_debug = "rp: o_"
    elif repo_name_status == "none" and home_name_status == "match_special":
        dot_struc_value = 'hm'
        debug_status = 'home_only_full_match'
        m_status_result = True  # Matching status found
        match_debug = "hm: _o"
    else:
        dot_struc_value = None
        debug_status = 'no_full_match'
        m_status_result = False  # Explicitly set to False
        match_debug = "Match: XX"

    return dot_struc_value, debug_status, m_status_result, match_debug

def make_status_match_log_dict(index, row, repo_name, home_name, repo_name_cf, home_name_cf, repo_name_db, home_name_db, dot_struc_value, m_status_result, st_match_symb):
    """ Creates a dict to encapsulate match logic, separating it from DataFrame updates.
    This allows for temp storage of match states, enabling efficient and modular debugging.
    The dict holds match statuses without altering the DataFrame, providing flexibility for extensions.
    Once finalized, DataFrame updates occur in a single operation, improving performance."""
    
    m_status_dict = {
        "index": index,
        "repo_matches": {"fs": False, "yaml": False, "csv": False},
        "home_matches": {"fs": False, "yaml": False, "csv": False},
        "full_dom_matches": {"repo": False, "home": False, "home_to_repo": False},
        "st_match_symb": st_match_symb  # Add the st_match_symb field
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

def print_debug_info(index, repo_name, home_name, repo_name_db, home_name_db, dot_struc_value, debug_status):
    if DEBUG:
        print(
            f"Index {index} - repo_name: {repo_name}, home_name: {home_name}, "
              f"repo_name_db: {repo_name_db}, home_name_db: {home_name_db}, "
              f"dot_struc: {dot_struc_value}, st_misc: {debug_status}"
              )

def process_row(index, row):
    repo_values = [row['item_name_rp'], row['item_name_rp_db'], row['item_name_rp_cf']]
    home_values = [row['item_name_hm'], row['item_name_hm_db'], row['item_name_hm_cf']]
    repo_values = [val if pd.notna(val) else None for val in repo_values]
    home_values = [val if pd.notna(val) else None for val in home_values]

    repo_first = repo_values[0]
    home_first = home_values[0]

    repo_debug_characters, repo_name_status = assign_debug_characters(repo_values, repo_first)
    home_debug_characters, home_name_status = assign_debug_characters(home_values, home_first)

    dot_struc_value, debug_status, m_status_result, match_debug = apply_matching_logic(
        repo_name_status, home_name_status, repo_first, home_first
    )

    st_match_symb_content = f"{repo_debug_characters} | {home_debug_characters} | {match_debug.split(': ')[1]}"
    st_misc_content = debug_status if DISPLAY_MATCH_DETAIL_IN_ST_MISC else ""

    return dot_struc_value, m_status_result, st_match_symb_content, st_misc_content

def detect_full_domain_match(report_dataframe):
    report_dataframe['st_match_symb'] = ''
    report_dataframe['st_misc'] = ''
    for index, row in report_dataframe.iterrows():
        try:
            dot_struc_value, m_status_result, st_match_symb_content, st_misc_content = process_row(index, row)
            if dot_struc_value is not None:
                report_dataframe.at[index, 'dot_struc'] = dot_struc_value
            report_dataframe.at[index, 'm_status_result'] = m_status_result
            report_dataframe.at[index, 'st_match_symb'] = st_match_symb_content
            report_dataframe.at[index, 'st_misc'] = st_misc_content
            if DEBUG:
                print_debug_info(index, row['item_name_rp'], row['item_name_hm'], row['item_name_rp_db'], row['item_name_hm_db'], dot_struc_value, st_misc_content)
                if DEBUG:
                    m_status_dict = make_status_match_log_dict(
                        index, row, row['item_name_rp'], row['item_name_hm'], row['item_name_rp_cf'], row['item_name_hm_cf'],
                        row['item_name_rp_db'], row['item_name_hm_db'], dot_struc_value, m_status_result, st_match_symb_content
                    )
                    print(m_status_dict)
        except Exception as e:
            print(f"Error processing index {index}: {e}")
    return report_dataframe