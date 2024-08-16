import os
import logging
import pandas as pd

from dbase.dbase01_setup import build_main_dataframe
from dbase.dbase08_validate import validate_dataframes
from report_gen import generate_timestamped_output_paths, prepare_output_dataframes, export_dataframe_to_csv, export_to_markdown

# Configure logging to show DEBUG level messages 
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Change to DEBUG to capture more detailed output (INFO is the default level)
)

# Define the home directory
home_dir = os.path.expanduser("~")

# Paths for output
user_main_path = os.path.join(home_dir, "Library/Mobile Documents/com~apple~CloudDocs") # Set the main user path for iCloud
user_path_md = os.path.join(user_main_path, "Documents_SRB iCloud/Filespace control/FS Ctrl - LOGS/Info Reports + Logs IN") # Paths for md output
user_path_csv = os.path.join(user_main_path, "Documents_SRB iCloud/Filespace control/FS Ctrl - LOGS/Info Reports + Logs IN") # Paths for csv output

output_base_name_csv = "mac-dot-report"
output_base_name_md = "mac-dot-report"

def print_data_checks(df, stage):
    """Print data types and missing values for the DataFrame."""
    # print(f"\n{stage} DataFrame Info:\n")
    # print(df.info())
    # print(f"\nData types after {stage}:\n", df.dtypes)
    # print(f"\nMissing values after {stage}:\n", df.isnull().sum())

def main():
    # Generate timestamped filenames with the specified base names
    csv_output_path, markdown_output_path = generate_timestamped_output_paths(
        user_path_csv, output_base_name_csv, user_path_md, output_base_name_md
    )

    print("csv_output_path: ", csv_output_path)
    # Setup the database and get the dot items DataFrame
    main_dataframe = build_main_dataframe()
    
    # Debug: Print columns before validation
    print("Columns before validation:", main_dataframe.columns)

    # Debug: Display the DataFrame to examine its contents
    print("DataFrame contents:\n", main_dataframe.head())

    # Validate the final DataFrame (this step should occur after merging)
    # main_dataframe = validate_dataframes(main_dataframe, main_dataframe)

    # Export the DataFrame to a CSV file
    export_dataframe_to_csv(main_dataframe, filename=csv_output_path)

    # Export the DataFrame to a Markdown report using Jinja2
    export_to_markdown(
        df=main_dataframe,
        template_path='data',
        template_file='report_md.jinja2',
        output_file=markdown_output_path
    )

# This ensures the main function is only executed if this file is run directly
if __name__ == "__main__":
    main()