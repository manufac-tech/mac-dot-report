import os
import logging
import pandas as pd

from datetime import datetime
from jinja2 import Environment, FileSystemLoader

# Define user paths without including User's actual $HOME folder name
HOME_DIR = os.path.expanduser("~")
USER_MAIN_PATH = os.path.join(HOME_DIR, "Library/Mobile Documents/com~apple~CloudDocs")
USER_PATH_MD = os.path.join(USER_MAIN_PATH, "Documents_SRB iCloud/Filespace control/FS Ctrl - LOGS/Info Reports + Logs IN")
USER_PATH_CSV = os.path.join(USER_MAIN_PATH, "Documents_SRB iCloud/Filespace control/FS Ctrl - LOGS/Info Reports + Logs IN")

# Define input and output paths
DOT_INFO_CSV_PATH = 'data'
REPORT_TEMPLATE_J2 = 'report_md.jinja2'
OUTPUT_BASE_NAME = "mac-dot-report"

def generate_timestamped_output_paths(directory_path, base_name):
    timestamp = datetime.now().strftime('%y%m%d-%H%M%S')
    
    markdown_output_path = os.path.join(directory_path, f"{timestamp}_{base_name}.md")
    csv_output_path = os.path.join(directory_path, f"{timestamp}_{base_name}.csv")
    full_csv_output_path = os.path.join(directory_path, f"{timestamp}_{base_name}_FULL_DF.csv")
    
    return csv_output_path, full_csv_output_path, markdown_output_path

# CONVERT DATAFRAMES TO OUTPUT FORMATS
def export_to_markdown(output_file, df=None, fs_not_in_di=None, di_not_in_fs=None):
    try:
        if df is None:
            raise ValueError("DataFrame 'df' must be provided")

        env = Environment(
            loader=FileSystemLoader(DOT_INFO_CSV_PATH),
            trim_blocks=True,
            lstrip_blocks=True
        )
        env.globals['pd'] = pd

        template = env.get_template(REPORT_TEMPLATE_J2)
        rendered_markdown = template.render(
            csv_data=df.to_dict(orient='records'),
            fs_not_in_di=fs_not_in_di if fs_not_in_di else [],
            di_not_in_fs=di_not_in_fs if di_not_in_fs else []
        )

        with open(output_file, 'w') as file:
            file.write(rendered_markdown)

        logging.info(f"Markdown report generated at: {os.path.abspath(output_file)}")
    except Exception as e:
        logging.error(f"Failed to export DataFrame to Markdown: {e}")

def export_dataframe_to_csv(df, filename, columns=None):
    try:
        df.to_csv(filename, index=False, columns=columns)
        logging.info(f"DataFrame exported to '{filename}'")
    except Exception as e:
        logging.error(f"Failed to export DataFrame to CSV: {e}")

# SAVE OUTPUTS TO DISK
def save_outputs(main_df_dict):
    csv_output_path, full_csv_output_path, markdown_output_path = generate_timestamped_output_paths(USER_PATH_CSV, OUTPUT_BASE_NAME)

    logging.info(f"Report CSV path: {csv_output_path}")
    logging.info(f"Full CSV path: {full_csv_output_path}")
    logging.info(f"Markdown path: {markdown_output_path}")

    save_markdown(main_df_dict, markdown_output_path)
    save_report_csv(main_df_dict, csv_output_path)
    save_full_csv(main_df_dict, full_csv_output_path)

def save_markdown(main_df_dict, markdown_output_path):
    export_to_markdown(
        output_file=markdown_output_path,
        df=main_df_dict['report_dataframe'],
        fs_not_in_di=main_df_dict.get('fs_not_in_di', []),
        di_not_in_fs=main_df_dict.get('di_not_in_fs', [])
    )

def save_report_csv(main_df_dict, csv_output_path):
    logging.info(f"Saving report DataFrame to {csv_output_path}")
    export_dataframe_to_csv(main_df_dict['report_dataframe'], filename=csv_output_path)

def save_full_csv(main_df_dict, full_csv_output_path):
    if 'full_main_dataframe' in main_df_dict:
        logging.info(f"Saving full DataFrame to {full_csv_output_path}")
        export_dataframe_to_csv(main_df_dict['full_main_dataframe'], filename=full_csv_output_path)
    else:
        logging.warning("Warning: 'full_main_dataframe' key not found in main_df_dict. Full DataFrame not saved.")