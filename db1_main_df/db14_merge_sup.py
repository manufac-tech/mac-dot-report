import pandas as pd

# Add the unique ID generation code
current_unique_id = 1

def get_next_unique_id():
    global current_unique_id
    unique_id = current_unique_id
    current_unique_id += 1
    return unique_id

def consolidate_post_merge1(main_df): # MERGE REQUIREMENT: Copies the priority name to item_name
    def consolidate(row):
        if pd.notna(row['item_name_rp']) and row['item_name_rp'] != "":
            return row['item_name_rp']
        elif pd.notna(row['item_name_hm']) and row['item_name_hm'] != "":
            return row['item_name_hm']
        else:
            return row['item_name']  # If both are null, keep the original value

    main_df['item_name'] = main_df.apply(consolidate, axis=1)
    return main_df

def consolidate_post_merge3(main_df): # MERGE REQUIREMENT: Copies the priority name to item_name
    def consolidate(row):
        if pd.notna(row['item_name_rp_cf']) and row['item_name_rp_cf'] != "":
            return row['item_name_rp_cf']
        elif pd.notna(row['item_name_hm_cf']) and row['item_name_hm_cf'] != "":
            return row['item_name_hm_cf']
        else:
            return row['item_name']  # If both are null, keep the original value

    main_df['item_name'] = main_df.apply(consolidate, axis=1)
    return main_df

def print_main_df_build_hist(df_dict):
    if not hasattr(print_main_df_build_hist, "merge_counter"):
        print_main_df_build_hist.merge_counter = 1

    for key, df in df_dict.items():
        if key == "df1":
            value_title = f"🟩 Main DataFrame (Before Merge) - Merge {print_main_df_build_hist.merge_counter} - First 5 Rows"
            type_title = f"Main DataFrame (Before Merge) - Merge {print_main_df_build_hist.merge_counter} - Data Types (First 5 Rows)"
        elif key == "df2":
            value_title = f"Input DataFrame (Before Merge) - Merge {print_main_df_build_hist.merge_counter} - First 5 Rows"
            type_title = f"Input DataFrame (Before Merge) - Merge {print_main_df_build_hist.merge_counter} - Data Types (First 5 Rows)"
        elif key == "df3":
            value_title = f"Merged DataFrame (After Merge) - Merge {print_main_df_build_hist.merge_counter} - First 5 Rows"
            type_title = f"Merged DataFrame (After Merge) - Merge {print_main_df_build_hist.merge_counter} - Data Types (First 5 Rows)"

        print(f"{value_title}:")
        # print(df.head(5))
        print(df)
        print("\n")

        print(f"{type_title}:")
        # print(df.dtypes.head(5))
        print(df.dtypes)
        print("\n" * 2)

    # Increment the counter after processing the DataFrames
    print_main_df_build_hist.merge_counter += 1

# def apply_output_grouping(df):
#     # Sort the entire DataFrame by 'sort_orig'
#     df_sorted = df.sort_values('sort_orig', ascending=True)
#     df_sorted = df_sorted.reset_index(drop=True)
#     return df_sorted

# def reorder_dfm_cols_perm(df):
#     # Define the desired column order based on the provided fields
#     desired_order = [
#         'item_name', 'item_type', 'unique_id',
#         'item_name_rp', 'item_type_rp', 'git_rp', 'item_name_hm', 'item_type_hm',
#         'item_name_hm_db', 'item_type_hm_db', 'item_name_rp_db', 'item_type_rp_db',
#         'item_name_rp_cf', 'item_type_rp_cf', 'item_name_hm_cf', 'item_type_hm_cf',
#         'dot_struc_cf', 'cat_1_cf', 'cat_1_name_cf', 'cat_2_cf', 'comment_cf', 'no_show_cf',
#         'sort_orig',
#         'unique_id_rp', 'unique_id_db', 'unique_id_hm', 'unique_id_cf'
#     ]
    
#     # Ensure all columns in desired_order are in the DataFrame
#     for col in desired_order:
#         if col not in df.columns:
#             print(f"Warning: Column {col} not found in DataFrame")
    
#     # Reorder columns
#     df = df[desired_order]
    
#     return df

