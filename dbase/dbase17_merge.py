import logging
import pandas as pd
import numpy as np

from .dbase16_validate import replace_string_blanks

def merge_dataframes(main_df_dict, input_df_dict_section, merge_type='outer'):
    # Extract the DataFrames from the dictionary sections
    main_df = main_df_dict['dataframe']
    input_df = input_df_dict_section['dataframe']
    left_merge_field = main_df_dict['merge_field']
    right_merge_field = input_df_dict_section['merge_field']

    # Perform the merge operation
    try:
        merged_dataframe = pd.merge(
            main_df, input_df,
            left_on=left_merge_field,
            right_on=right_merge_field,
            how=merge_type
        ).copy()

        # Apply the blank replacement after the merge
        merged_dataframe = replace_string_blanks(merged_dataframe)

    except Exception as e:
        raise RuntimeError(f"Error during merge: {e}")
    
    return merged_dataframe



# def merge_dataframes(main_df_dict, input_df_dict):
#     # Initialize main_df from the main_df_dict
#     main_df = main_df_dict['dataframe']

#     # Merge with 'home' section
#     if 'home' in input_df_dict:
#         home_df_dict = input_df_dict['home']
#         left_merge_field_home = main_df_dict['merge_field']
#         right_merge_field_home = home_df_dict['merge_field']

#         try:
#             main_df = pd.merge(
#                 main_df, home_df_dict['dataframe'],
#                 left_on=left_merge_field_home,
#                 right_on=right_merge_field_home,
#                 how='outer'
#             ).copy()

#             # Apply the blank replacement after the merge
#             main_df = replace_string_blanks(main_df)

#         except Exception as e:
#             raise RuntimeError(f"Error during merge with section 'home': {e}")

#     # Merge with 'repo' section
#     if 'repo' in input_df_dict:
#         repo_df_dict = input_df_dict['repo']
#         left_merge_field_repo = main_df_dict['merge_field']
#         right_merge_field_repo = repo_df_dict['merge_field']

#         try:
#             main_df = pd.merge(
#                 main_df, repo_df_dict['dataframe'],
#                 left_on=left_merge_field_repo,
#                 right_on=right_merge_field_repo,
#                 how='outer'
#             ).copy()

#             # Apply the blank replacement after the merge
#             main_df = replace_string_blanks(main_df)

#         except Exception as e:
#             raise RuntimeError(f"Error during merge with section 'repo': {e}")

#     # Merge with 'dotbot' section
#     if 'dotbot' in input_df_dict:
#         dotbot_df_dict = input_df_dict['dotbot']
#         left_merge_field_dotbot = main_df_dict['merge_field']
#         right_merge_field_dotbot = dotbot_df_dict['merge_field']

#         try:
#             main_df = pd.merge(
#                 main_df, dotbot_df_dict['dataframe'],
#                 left_on=left_merge_field_dotbot,
#                 right_on=right_merge_field_dotbot,
#                 how='outer'
#             ).copy()

#             # Apply the blank replacement after the merge
#             main_df = replace_string_blanks(main_df)

#         except Exception as e:
#             raise RuntimeError(f"Error during merge with section 'dotbot': {e}")

#     # Merge with 'dot_info' section
#     if 'dot_info' in input_df_dict:
#         dot_info_df_dict = input_df_dict['dot_info']
#         left_merge_field_dot_info = main_df_dict['merge_field']
#         right_merge_field_dot_info = dot_info_df_dict['merge_field']

#         try:
#             main_df = pd.merge(
#                 main_df, dot_info_df_dict['dataframe'],
#                 left_on=left_merge_field_dot_info,
#                 right_on=right_merge_field_dot_info,
#                 how='outer'
#             ).copy()

#             # Apply the blank replacement after the merge
#             main_df = replace_string_blanks(main_df)

#         except Exception as e:
#             raise RuntimeError(f"Error during merge with section 'dot_info': {e}")

#     # Update the main_df_dict to store the final merged DataFrame
#     main_df_dict['dataframe'] = main_df
    
#     return main_df_dict