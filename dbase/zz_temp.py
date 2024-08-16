def merge_dataframes(df1, df2, suffix_map):
    pre_merge_processing(df1, df2, suffix_mapping)
    return merge_dataframes(df1, df2, suffix_mapping['home'], suffix_mapping['repo'])

def build_main_dataframe(dataframes_to_merge):
    home_items_df = load_hm_dataframe()
    repo_items_df = load_rp_dataframe(repo_path)
    
    suffix_mapping = {
        'home': '_hm',
        'repo': '_rp',
        'dotbot': '_db',
        'template': '_tp',
        'main': '_mf'  # For merged frames
    }

    ## SOME KIND OF ITERATOR HERE, or equivilent:  
    main_dataframe = merge_dataframes(home_items_df, repo_items_df, suffix_mapping)


    return main_dataframe




    