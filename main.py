import os
import logging
import pandas as pd

from dbase.db01_setup import build_full_output_dict
from report_gen import generate_timestamped_output_paths, export_dataframe_to_csv, export_to_markdown

home_dir = os.path.expanduser("~")  # Define the home directory

# Paths for output
user_main_path = os.path.join(home_dir, "Library/Mobile Documents/com~apple~CloudDocs")  # Set the main user path for iCloud
user_path_md = os.path.join(user_main_path, "Documents_SRB iCloud/Filespace control/FS Ctrl - LOGS/Info Reports + Logs IN")  # Paths for md output
user_path_csv = os.path.join(user_main_path, "Documents_SRB iCloud/Filespace control/FS Ctrl - LOGS/Info Reports + Logs IN")  # Paths for csv output

output_base_name_md = "mac-dot-report"
output_base_name_csv = "mac-dot-report"
output_base_name_csv_full = "mac-dot-report_FULL_DF"

def save_outputs(main_df_dict, csv_output_path, markdown_output_path, csv_full_output_path):
    # save_markdown(main_df_dict, markdown_output_path)  # Export Markdown report
    save_report_csv(main_df_dict, csv_output_path)  # Export the report DataFrame to CSV
    # save_full_csv(main_df_dict, csv_full_output_path)  # Save full main DataFrame to CSV

def save_markdown(main_df_dict, markdown_output_path):
    export_to_markdown(
        dot_info_path='data',
        dot_info_file='report_md.jinja2',
        output_file=markdown_output_path,
        df=main_df_dict['report_dataframe'],  # Use report_dataframe for the Markdown report
        fs_not_in_di=main_df_dict.get('fs_not_in_di', []),
        di_not_in_fs=main_df_dict.get('di_not_in_fs', [])
    )

def save_report_csv(main_df_dict, csv_output_path):
    export_dataframe_to_csv(main_df_dict['report_dataframe'], filename=csv_output_path.replace(".csv", "_report.csv"))

def save_full_csv(main_df_dict, csv_full_output_path):
    export_dataframe_to_csv(main_df_dict['full_main_dataframe'], filename=csv_full_output_path.replace(".csv", "_full_main.csv"))

def main():
    # Generate timestamped filenames with the specified base names
    csv_output_path, markdown_output_path = generate_timestamped_output_paths(
        user_path_csv, output_base_name_csv, user_path_md, output_base_name_md
    )
    _, csv_full_output_path = generate_timestamped_output_paths(
        user_path_csv, output_base_name_csv_full, user_path_md, output_base_name_md
    )

    main_df_dict = build_full_output_dict()  # Build the main_dataframe and return as a dictionary

    save_outputs(main_df_dict, csv_output_path, markdown_output_path, csv_full_output_path)  # Write to disk

# This ensures the main function is only executed if this file is run directly
if __name__ == "__main__":
    main()
    