import os
import logging

import pandas as pd
from jinja2 import Environment, FileSystemLoader
import regex as re
from datetime import datetime

def generate_timestamped_output_paths(csv_directory_path, csv_base_name, markdown_directory_path, markdown_base_name):
    timestamp = datetime.now().strftime('%y%m%d-%H%M%S')
    csv_output_path = os.path.join(csv_directory_path, f"{timestamp}_{csv_base_name}.csv")
    markdown_output_path = os.path.join(markdown_directory_path, f"{timestamp}_{markdown_base_name}.md")
    return csv_output_path, markdown_output_path

def prepare_output_dataframes(main_dataframe):
    # Filter and sort for each group
    group1_df = main_dataframe[main_dataframe['sort_out'] == 1].sort_values('item_name')
    group2_df = main_dataframe[main_dataframe['sort_out'] == 2].sort_values('sort_orig')
    group3_df = main_dataframe[main_dataframe['sort_out'] == 3].sort_values('item_name')


    return group1_df, group2_df, group3_df

def export_dataframe_to_csv(df, filename, columns=None):
    try:
        # If columns are specified, export only those columns
        if columns:
            df.to_csv(filename, index=False, columns=columns)
        else:
            df.to_csv(filename, index=False)  # Export all columns by default
        print(f"DataFrame exported to '{filename}'")
    except Exception as e:
        print(f"Failed to export DataFrame to CSV: {e}")

def export_to_markdown(dot_info_path, dot_info_file, output_file, df=None):
    try:
        # if df is None:
        #     raise ValueError("DataFrame 'df' must be provided")

        # Prepare output DataFrames (most recent data)
        group1_df, group2_df, group3_df = prepare_output_dataframes(df)

        # Logging to inspect the DataFrames (you can keep this for debugging)
        # logging.debug("Group 1 DataFrame (FS items not in dot_info):\n%s", group1_df.to_string())
        # logging.debug("Group 2 DataFrame (Matched items):\n%s", group2_df.to_string())
        # logging.debug("Group 3 DataFrame (dot_info items not in FS):\n%s", group3_df.to_string())

        # Jinja2 setup
        env = Environment(
            loader=FileSystemLoader(dot_info_path),
            trim_blocks=True,
            lstrip_blocks=True
        )
        env.globals['pd'] = pd

        # Render dot_info - pass the Group DataFrames directly
        dot_info = env.get_dot_info(dot_info_file)
        rendered_markdown = dot_info.render(
            csv_data=group2_df.to_dict(orient='records'),  # Group 2: Matched items
            fs_not_in_di=group1_df.to_dict(orient='records'),  # Group 1: FS items not in dot_info
            di_not_in_fs=group3_df.to_dict(orient='records')  # Group 3: dot_info items not in FS
        )

        # Output the rendered Markdown to a file
        with open(output_file, 'w') as file:
            file.write(rendered_markdown)

        print(f"Markdown report generated at: {os.path.abspath(output_file)}")
    except Exception as e:
        print(f"Failed to export DataFrame to Markdown: {e}")