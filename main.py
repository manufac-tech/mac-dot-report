import logging
from db0_main_df.db00_make_df_dict import build_full_output_dict
from report_gen import save_outputs

def main():
    main_df_dict = build_full_output_dict()

    # Configuration to control which outputs to save
    save_config = {
        'save_markdown': False,
        'save_report_csv': True,
        'save_full_csv': False
    }

    save_outputs(main_df_dict, save_config)

if __name__ == "__main__":
    main()