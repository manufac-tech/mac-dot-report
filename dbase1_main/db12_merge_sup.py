import pandas as pd

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
        if pd.notna(row['item_name_rp_di']) and row['item_name_rp_di'] != "":
            return row['item_name_rp_di']
        elif pd.notna(row['item_name_hm_di']) and row['item_name_hm_di'] != "":
            return row['item_name_hm_di']
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