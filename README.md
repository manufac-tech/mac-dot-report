# mac-dot-report

#### Overview

mac-dot-report is a Python-based tool to help users manage and document the dot files and dot folders in their home directory. Upon execution, the script generates a fresh list of all dot items located at the root level of your home folder, grouping the items by user-specified categories and by providing user-specified comments. This additional metadata is stored in `data/dot-info.csv`.

#### Features

- **Tracking of Dot Item State**: Each time the script is run, it generates an updated list of all dot files and dot folders at the root level of your home directory and saves it to your specified directory.
- **Categorization**: In `dot-info.csv`, assign categories to your dot items to display them as a group in each report.
- **Commenting**: Also in `dot-info.csv`, insert comments so future-you will know the purpose of cryptic dot items:
  - `.CFUserTextEncoding | macOS - stores the user’s preferred text encoding settings`
- **Comprehensive Reports**: The tool generates categorized markdown reports and a CSV output of the entire database, which is useful when updating the dot_info.
- **Symlink support**: If you're managing your dot items by storing them elsewhere and creating symlinks in $HOME (using DotBot, Chez Moi, Stow, etc.), the symlinks will also be tracked as if they are regular items. Note: I did nothing to enable this. The Python `os` library defaults to seeing symlinks as their source items.

#### Usage

1. Generating reports: Run the script to generate both the markdown report and the full csv output of the database into the folder specified in main.py as: YYMMDD-HHMMSS_dot_inventory.md and YYMMDD-HHMMSS_dot_inventory.csv.

2. Updating dot_info: Use the collected "unmatched" items in the report to manually edit the dot_info csv to that the new items automatically sort to your chosen category.
   - Any new or missing (from the dot_info) items in $HOME can be ignored and they will continue to accumulate in their respective categories in the report.
   - Periodically, as part of regular maintenance, the user can run a report, review the accumulated items, and update the dot_info CSV file to include categories for better organization. If the user is unsure of an item's purpose, they can research it and add a comment to the dot_info for future reference.
   - When new items are included in the dot_info, they will display in their correct categories and no longer display in the Unmatched section.

#### dot_info Overview

The dot_info in this project provides structure and clarity to the otherwise cluttered and cryptic list of dot files and dot folders in a user's home directory. It serves several key purposes:

- **Categorization**: Each item is assigned to categories that help organize and group related files.
- **Comments**: Descriptive comments are added to explain the purpose or significance of specific files.
- **Sorting**: Metadata like category and order help sort items in a clear way in the final report.

The dot_info includes fields that allow the user to associate information and comments to the dot items:

- **item_name**: Name of the dot item.
- **item_type**: File or folder.
- **di_cat_1**: The primary category.
- **di_cat_1_name**: The name of the primary category.
- **di_comment**: Additional comments or notes.
- **di_cat_2**: The secondary category.
- **no_show**: A flag indicating whether to hide the item in the report.

---

### Roadmap

- **Category Indexing**:

  - Implement an index at the top of the report summarizing the count of dot files and dot folders under each Category 1 and Category 2. This will provide a quick overview of which categories contribute the most items to the home folder.
    - **Index formatting**:
      - (index line) `term, zsh: 4 files, 1 folder`

- **Run-anywhere**:

  - Explore options like Zsh alias, Keyboard Maestro macro, Hazel rule, or Automator action to make the script executable from any directory or context.

- **add/clean up debug lines**:

  - tbd

- **Replace blanks in report.csv**:
  - Replace blanks in report.csv with contextual placeholders or flags, such as `[NO ITEM TYPE]` or `[NO ITEM NAME]`.

---

|                      | **fs_item_name** | **fs*item_type*** | **di_item_name** | **di_item_type** | **item_name**    | **item_type**  |
| -------------------- | ---------------- | ----------------- | ---------------- | ---------------- | ---------------- | -------------- |
| **Scenario 1 IN**    | .zshrc           | TRUE              | NaN              | NaN              |                  |                |
| **Scenario 1 NAMES** | .zshrc           | TRUE              | [NO di ITEM]     | [NO di ITEM]     | .zshrc           | TRUE           |
|                      |                  |                   |                  |                  |                  |                |
| **Scenario 2 IN**    | NaN              | NaN               | .zshrc           | TRUE             |                  |                |
| **Scenario 2 NAMES** | [NO FS ITEM]     | [NO FS ITEM]      | .zshrc           | TRUE             | .zshrc           | TRUE           |
|                      |                  |                   |                  |                  |                  |                |
| **Scenario 3 IN**    | NaN              | NaN               | .zshrc           | NaN              |                  |                |
| **Scenario 3 NAMES** | [NO FS ITEM]     | [NO FS ITEM]      | .zshrc [NO TYPE] | [NO ITEM DATA]   | .zshrc [NO TYPE] | [NO ITEM DATA] |

- Maybe use rcm somehow...https://thoughtbot.github.io/rcm/rcm.7.html
