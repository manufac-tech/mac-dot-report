import os
import logging
import pandas as pd
import numpy as np

from .dbase2_load_fs import load_fs_dataframe
from .dbase3_load_tp import load_tp_dataframe
from .dbase4_validate import validate_dataframes
from .dbase5_merge import merge_dataframes
from .dbase6_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns,
    sort_items_1_out_group,
    sort_items_2_indiv,
)
def build_main_dataframe(template_file_path):
    """Master function to set up the database with dot items and template data."""
    dot_items_df, id_start_tp = load_fs_dataframe()
    template_df = load_tp_dataframe(template_file_path, start_id=id_start_tp)
    dot_items_df, template_df = validate_dataframes(dot_items_df, template_df)

    # --------------------- THE MERGE
    main_dataframe = merge_dataframes(dot_items_df, template_df)
    
    # Add & populate out_group
    main_dataframe = add_and_populate_out_group(main_dataframe)

    # Reorder columns
    main_dataframe = reorder_columns(main_dataframe)

    # Sort by out_group and within each group
    main_dataframe = sort_items_1_out_group(main_dataframe)
    main_dataframe = sort_items_2_indiv(main_dataframe)

    # logging.debug("Main DataFrame at current stage:\n%s", main_dataframe.to_string())

    return main_dataframe