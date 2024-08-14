import os
import logging
import pandas as pd
import numpy as np

from .dbase03_load_fs import load_fs_dataframe, load_dotbot_yaml_dataframe
from .dbase04_load_tp import load_tp_dataframe
from .dbase07_validate import validate_dataframes
from .dbase08_merge import merge_dataframes
from .dbase09_org import (
    add_and_populate_out_group,
    apply_output_grouping,
    reorder_columns,
    sort_items_1_out_group,
    sort_items_2_indiv,
)
def build_main_dataframe(template_file_path, dotbot_yaml_path):
    dot_items_df = load_fs_dataframe()  # Only get the DataFrame

    # YAML data frame loading is muted out for now
    dotbot_yaml_df = load_dotbot_yaml_dataframe(dotbot_yaml_path)

    template_df = load_tp_dataframe(template_file_path)

    # Validate only two DataFrames since YAML is muted out
    dot_items_df, template_df = validate_dataframes(dot_items_df, template_df)

    # Merging and processing
    main_dataframe = merge_dataframes(dot_items_df, template_df)
    
    # If the YAML integration is to be done later:
    # main_dataframe = pd.merge(main_dataframe, dotbot_yaml_df, how='outer', left_on='fs_item_name', right_on='db_name_dst')

    main_dataframe = add_and_populate_out_group(main_dataframe)
    main_dataframe = reorder_columns(main_dataframe)
    main_dataframe = sort_items_1_out_group(main_dataframe)
    main_dataframe = sort_items_2_indiv(main_dataframe)

    # logging.debug("Main DataFrame at current stage:\n%s", main_dataframe.to_string())

    return main_dataframe