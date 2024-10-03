
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