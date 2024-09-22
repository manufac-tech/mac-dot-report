import os

import subprocess
import plistlib
import stat

def determine_item_type(item_path):
    """
    Determine the type of a given file system item.

    Args:
        item_path (str): The full path to the item.

    Returns:
        str: A string representing the item type. Possible values:
             - "file": for regular files
             - "folder": for directories
             - "file_sym": for symlinks pointing to files
             - "folder_sym": for symlinks pointing to directories
             - "alias": for macOS aliases
             - "unknown": for any unrecognized types
    """
    if os.path.islink(item_path):
        # It's a symlink, determine the type it points to
        target_path = os.path.realpath(item_path)
        if os.path.isdir(target_path):
            return "folder_sym"
        elif os.path.isfile(target_path):
            return "file_sym"
        else:
            return "unknown"

    elif os.path.isdir(item_path):
        return "folder"

    elif os.path.isfile(item_path):
        # Determine if it's an alias on macOS
        if item_path.endswith('.alias'):
            return "alias"
        return "file"

    else:
        return "unknown"

def is_symlink(path):
    """Check if the given path is a symlink."""
    return os.path.islink(path)

def is_alias(path):
    """Determine if the given path is an alias on macOS."""
    # Use a subprocess call to check for alias attributes using macOS specific command
    try:
        result = subprocess.run(['xattr', '-p', 'com.apple.FinderInfo', path],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # The command returns a 32-byte value for aliases; check if the output matches expected alias length
        return len(result.stdout) == 32
    except Exception as e:
        return False

def get_file_type(path):
    """Check if the given path is a regular file."""
    return os.path.isfile(path)

def get_folder_type(path):
    """Check if the given path is a directory."""
    return os.path.isdir(path)

def resolve_item_type(path):
    """Resolve the item type based on symlink, alias, file, or folder checks."""
    if is_symlink(path):
        target_type = detect_symlink_target_type(path)
        return f'{target_type}_sym'
    elif is_alias(path):
        alias_type = detect_alias_type(path)
        return f'{alias_type}_alias'
    elif get_file_type(path):
        return 'file'
    elif get_folder_type(path):
        return 'folder'
    else:
        return 'unknown'

def detect_alias_type(path):
    """Detect the target type of an alias."""
    # Assuming the alias points to either a file or folder, we use the same logic to determine target type
    target = subprocess.run(['readlink', path], stdout=subprocess.PIPE).stdout.decode().strip()
    if os.path.isfile(target):
        return 'file'
    elif os.path.isdir(target):
        return 'folder'
    else:
        return 'unknown'

def detect_symlink_target_type(path):
    """Detect the target type of a symlink."""
    target = os.readlink(path)
    if os.path.isfile(target):
        return 'file'
    elif os.path.isdir(target):
        return 'folder'
    else:
        return 'unknown'