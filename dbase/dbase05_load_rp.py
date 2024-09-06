import os
import logging
import pandas as pd
import fnmatch

from .dbase02_id_gen import get_next_unique_id
from .dbase03_item_type import determine_item_type

def load_rp_dataframe():
    repo_items = []
   
    home_dir_path = os.path.expanduser("~")  # Define the home directory path
    repo_path = os.path.join(home_dir_path, "._dotfiles/dotfiles_srb_repo")  # Define the repo path

    for item in os.listdir(repo_path):
        if item.startswith("."):
            item_path = os.path.join(repo_path, item)
            item_type = determine_item_type(item_path)
            repo_items.append({
                "item_name_rp": item,  # Use the suffix '_rp' consistently
                "item_type_rp": item_type,  # Use the suffix '_rp' consistently
                "unique_id_rp": get_next_unique_id()  # Use the suffix '_rp' consistently
            })

    df = pd.DataFrame(repo_items).copy()

    # Explicitly set data types
    df["item_name_rp"] = df["item_name_rp"].astype("string")
    df["item_type_rp"] = df["item_type_rp"].astype("string")
    df["unique_id_rp"] = df["unique_id_rp"].astype("Int64")  # Ensure unique_id_rp is Int64

    # Create the git_db column based on .gitignore
    df = create_git_db_column(df, repo_path)

    # Toggle output directly within the function
    show_output = True  # Change to False to disable output
    show_full_df = False  # Change to True to show the full DataFrame

    if show_output:
        if show_full_df:
            print("5️⃣ Repo DataFrame:\n", df)
        else:
            print("5️⃣ Repo DataFrame (First 5 rows):\n", df.head())

    return df

def create_git_db_column(df, repo_path):
    # Read and parse the .gitignore file
    gitignore_path = os.path.join(repo_path, ".gitignore")
    if not os.path.exists(gitignore_path):
        print(f"No .gitignore file found at {gitignore_path}")
        df['git_db'] = True  # If .gitignore doesn't exist, assume all are tracked
        return df

    with open(gitignore_path, 'r') as f:
        gitignore_patterns = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    # Initialize the 'git_db' column with True values (assume tracked)
    df['git_db'] = True

    # Check each item in the DataFrame against the patterns in .gitignore
    for idx, row in df.iterrows():
        item_name = row['item_name_rp']
        item_type = row['item_type_rp']
        item_path = os.path.join(repo_path, item_name)

        # Debugging outputs
        print(f"Checking item: {item_name}, type: {item_type}")

        for pattern in gitignore_patterns:
            # Debugging pattern check
            print(f"Pattern from .gitignore: {pattern}")

            # Handle folders (patterns ending with '/')
            if pattern.endswith('/') and item_type == 'folder_sym':
                folder_match = fnmatch.fnmatch(item_name + '/', pattern)
                print(f"Checking folder: {item_name}/ against pattern {pattern}: {folder_match}")
                if folder_match:
                    df.at[idx, 'git_db'] = False  # Mark as untracked if folder matches
                    print(f"Folder {item_name}/ marked as untracked.")
                    break

            # Handle files and folders without trailing '/'
            else:
                file_match_name = fnmatch.fnmatch(item_name, pattern)
                file_match_path = fnmatch.fnmatch(item_path, pattern)
                print(f"Checking file: {item_name} or {item_path} against pattern {pattern}: name match: {file_match_name}, path match: {file_match_path}")
            
                if file_match_name or file_match_path:
                    df.at[idx, 'git_db'] = False  # Mark as untracked if file matches
                    print(f"File {item_name} marked as untracked.")
                    break

    return df