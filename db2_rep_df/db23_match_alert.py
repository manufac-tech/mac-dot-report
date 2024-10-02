# db18_match_B2.py

import pandas as pd
import numpy as np

def detect_alerts(report_dataframe):
    # Normalize missing values using the shared function
    columns_to_normalize = [
        'item_name_rp', 'item_name_hm', 'item_name_rp_db', 'item_name_hm_db',
        'item_name_rp_cf', 'item_name_hm_cf', 'item_type_rp', 'item_type_hm',
        'item_type_rp_db', 'item_type_hm_db', 'item_type_rp_cf', 'item_type_hm_cf'
    ]
    report_dataframe = normalize_missing_values(report_dataframe, columns_to_normalize)

    # Initialize the 'st_alert' column
    report_dataframe['st_alert'] = pd.Series([np.nan] * len(report_dataframe), dtype='object')

    # Iterate over each row in the DataFrame
    for index, row in report_dataframe.iterrows():
        try:
            # Extract necessary fields
            item_name_rp = row['item_name_rp']
            item_name_hm = row['item_name_hm']
            item_name_rp_db = row['item_name_rp_db']
            item_name_hm_db = row['item_name_hm_db']
            item_name_rp_cf = row['item_name_rp_cf']
            item_name_hm_cf = row['item_name_hm_cf']
            item_type_rp = row['item_type_rp']
            item_type_hm = row['item_type_hm']

            # Initialize alert as None
            alert = None

            # Reuse existing matching results
            m_status_result = row.get('m_status_result', False)
            dot_struc = row.get('dot_struc', None)

            # Apply alert logic only if no full match was found
            if not m_status_result:
                # Check for FS Home-only, with no match in docs
                if pd.isna(item_name_rp) and pd.notna(item_name_hm):
                    if pd.isna(item_name_rp_db) and pd.isna(item_name_hm_db) and pd.isna(item_name_rp_cf) and pd.isna(item_name_hm_cf):
                        alert = 'Home Folder New Item'

                # Check for Doc Only (no FS)
                elif pd.isna(item_name_rp) and pd.isna(item_name_hm):
                    if pd.notna(item_name_rp_db) or pd.notna(item_name_hm_db) or pd.notna(item_name_rp_cf) or pd.notna(item_name_hm_cf):
                        alert = 'Doc Only No FS'

                # Check for Symlink Overwrite
                elif pd.notna(item_name_rp) and pd.notna(item_name_hm):
                    if item_name_rp == item_name_hm:
                        if item_type_rp in ['file', 'folder'] and item_type_hm in ['file', 'folder']:
                            alert = 'Symlink Overwrite'

                # Check for YAML Inconsistency
                if pd.notna(item_name_hm) and pd.notna(item_name_hm_db):
                    if item_name_hm != item_name_hm_db:
                        alert = 'YAML Inconsistency'

                # Assign the alert to the 'st_alert' column
                if alert:
                    report_dataframe.at[index, 'st_alert'] = alert

        except Exception as e:
            print(f"Error processing index {index}: {e}")

    return report_dataframe


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