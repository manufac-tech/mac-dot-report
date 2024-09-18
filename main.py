import logging
from dbase.db01_setup import build_full_output_dict
from report_gen import save_outputs

def main():
    
    main_df_dict = build_full_output_dict()

    save_outputs(main_df_dict)

if __name__ == "__main__":
    main()