import os
import logging
import pandas as pd
import fnmatch

from dbase1_main.db11_merge import get_next_unique_id
from dbase1_main.db04_get_type import determine_item_type
from dbase1_main.db03_dtype_dict import field_types

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

    # Explicitly set data types using the field_types dictionary
    df["item_name_rp"] = df["item_name_rp"].astype(field_types["item_name_rp"])
    df["item_type_rp"] = df["item_type_rp"].astype(field_types["item_type_rp"])
    df["unique_id_rp"] = df["unique_id_rp"].astype(field_types["unique_id_rp"])

    # Create the git_rp column based on .gitignore
    df = create_git_rp_column(df, repo_path)

    # Toggle output directly within the function
    show_output = False
    show_full_df = False
    
    if show_output:
        if show_full_df:
            print("5️⃣ Repo DataFrame:\n", df)
        else:
            print("5️⃣ Repo DataFrame (First 5 rows):\n", df.head())

    return df

def create_git_rp_column(df, repo_path):
    # Retrieve gitignore items and their types (file or folder)
    gitignore_items = read_gitignore_items(repo_path)

    # Iterate through every item in the DataFrame and compare against gitignore items
    for idx, row in df.iterrows():
        item_name = row['item_name_rp']
        item_type = row['item_type_rp']

        # Initialize the git_rp column with True (assuming it's tracked)
        df.at[idx, 'git_rp'] = True

        # Compare with gitignore items
        for pattern, pattern_type in gitignore_items.items():
            # Use fnmatch to compare names and types
            if fnmatch.fnmatch(item_name, pattern) and item_type == pattern_type:
                df.at[idx, 'git_rp'] = False  # Mark as untracked
                # print(f"Match found: {item_name} ({item_type}) matches {pattern}")
                break  # Stop checking once a match is found

    return df


def read_gitignore_items(repo_path):
    gitignore_path = os.path.join(repo_path, ".gitignore")
    if not os.path.exists(gitignore_path):
        return {}

    gitignore_items = {}

    with open(gitignore_path, 'r') as f:
        for line in f:
            pattern = line.strip()
            if not pattern or pattern.startswith('#'):
                continue

            # Remove leading and trailing slashes for comparison purposes
            pattern_cleaned = pattern.lstrip('/').rstrip('/')

            # Determine type based on whether it had a trailing slash originally
            item_type = 'folder' if pattern.endswith('/') else 'file'

            # Store the cleaned pattern and its type
            gitignore_items[pattern_cleaned] = item_type

    return gitignore_items