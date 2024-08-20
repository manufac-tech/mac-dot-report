import os
import pandas as pd
from .dbase02_id_gen import get_next_unique_id

def load_dotbot_yaml_dataframe():
    dotbot_yaml_path = os.path.join(os.path.expanduser("~"), "._dotfiles/dotfiles_srb_repo/install.conf.yaml")
    # Load the YAML DataFrame using the provided path
    # Add your existing logic here
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
                        item_type = 'folder' if '# folder' in src else 'file'

                        # Extract just the file or folder name from the paths
                        name_dst = os.path.basename(dst)
                        name_src = os.path.basename(src.split()[0])

                        dotbot_entries.append({
                            'item_name_hm_db': name_dst,  # Destination in the Home folder
                            'item_name_rp_db': name_src,  # Source from the Repo folder
                            'item_type_db': item_type,
                            'unique_id_db': get_next_unique_id(), # Assign a unique ID
                        })

    dotbot_yaml_df = pd.DataFrame(dotbot_entries, columns=['item_name_dst', 'item_name_src', 'item_type', 'unique_id'])
    # print("DataFrame contents:\n", dotbot_yaml_df.head())


    return dotbot_yaml_df
