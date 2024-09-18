import logging
from dbase.db01_setup import build_full_output_dict
from report_gen import save_outputs

def main():
    print("Generating main_df_dict...")
    main_df_dict = build_full_output_dict()
    print(f"main_df_dict keys: {main_df_dict.keys()}")

    save_outputs(main_df_dict)

if __name__ == "__main__":
    main()