import os
import logging
import pandas as pd
import numpy as np

from .dbase02_id_gen import get_next_unique_id

def load_dotbot_yaml_dataframe(dotbot_yaml_path):
    """Extract file or folder names from YAML paths and determine item type by processing the file as plain text."""
    dotbot_entries = []

    with open(dotbot_yaml_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and full-line comments
                # Split the line into destination and source paths
                parts = line.split(':', 1)
                if len(parts) == 2:
                    dst, src = parts
                    dst = dst.strip()
                    src = src.strip()

                    # Ensure src is not empty before attempting to process it
                    if src:
                        # Determine if the item is a folder based on the inline comment
                        db_type = 'folder' if '# folder' in src else 'file'

                        # Extract just the file or folder name from the paths
                        db_name_dst = os.path.basename(dst)
                        db_name_src = os.path.basename(src.split()[0])

                        dotbot_entries.append({
                            'db_name_dst': db_name_dst,
                            'db_name_src': db_name_src,
                            'db_type': db_type,
                            'db_unique_id': get_next_unique_id(), # Assign a unique ID
                        })

    dotbot_yaml_df = pd.DataFrame(dotbot_entries, columns=['db_name_dst', 'db_name_src', 'db_type','db_unique_id'])

    # Log the final DataFrame for debugging purposes
    # logging.debug("DotBot YAML DataFrame:\n%s", dotbot_yaml_df.to_string())

    return dotbot_yaml_df