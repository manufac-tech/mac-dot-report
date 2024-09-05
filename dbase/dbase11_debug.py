import pandas as pd

def print_debug_info(section_name, section_dict, print_df):
    if print_df == 'none':
        return  # Do not print anything
    
    # Print the dictionary summary
    print(f"\n1️⃣ Dictionary for '{section_name}' section:")
    print(f"  dataframe_name: '{section_name}_dataframe'")
    print(f"  dataframe_shape: {section_dict['dataframe'].shape}")
    print(f"  suffix: '{section_dict['suffix']}'")
    print(f"  merge_field: '{section_dict['merge_field']}'")
    print(f"  name_field: '{section_dict['name_field']}'")
    print(f"  type_field: '{section_dict['type_field']}'")

    if print_df == 'full':
        # Print the entire DataFrame
        print(f"\n1️⃣ Result of merging main_dataframe with '{section_name}':\n{section_dict['dataframe']}")
    elif print_df == 'short':
        # Print only the first 5 rows
        print(f"\n1️⃣ First 5 rows after merging main_dataframe with '{section_name}':\n{section_dict['dataframe'].head()}")