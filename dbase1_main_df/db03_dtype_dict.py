

field_types = {

    # Unique ID Fields
    'unique_id': 'Int64',
    'unique_id_rp': 'Int64',
    'unique_id_db': 'Int64',
    'unique_id_hm': 'Int64',
    'unique_id_di': 'Int64',

    # Item Name Fields
    'item_name': 'string',
    
    'item_name_rp': 'string',
    'item_name_hm': 'string',
    'item_name_rp_db': 'string',
    'item_name_hm_db': 'string',
    'item_name_rp_di': 'string',
    'item_name_hm_di': 'string',
    
    'item_name_repo': 'string',
    'item_name_home': 'string',

    # Item Type Fields
    'item_type': 'string',
    
    'item_type_rp': 'string',
    'item_type_hm': 'string',
    'item_type_rp_db': 'string',
    'item_type_hm_db': 'string',
    'item_type_rp_di': 'string',
    'item_type_hm_di': 'string',
    
    'item_type_repo': 'string',
    'item_type_home': 'string',

    # Other Fields
    'git_rp': 'bool',
    
    'dot_struc': 'string',

    'dot_struc_di': 'string',
    'cat_1_di': 'string',
    'cat_1_name_di': 'string',
    'cat_2_di': 'string',
    'comment_di': 'string',
    'no_show_di': 'bool',
    'sort_orig': 'Int64',
    
    'sort_out': 'Int64',
    
    # Status Fields
    'st_alert': 'string',
    'st_db_all': 'string',
    'st_docs': 'string',
    'st_misc': 'string',
}


field_types_with_defaults = {
    # Unique ID Fields
    'unique_id': ('Int64', 0),
    'unique_id_rp': ('Int64', 0),
    'unique_id_db': ('Int64', 0),
    'unique_id_hm': ('Int64', 0),
    'unique_id_di': ('Int64', 0),

    # Item Name Fields
    'item_name': ('string', ''),
    'item_name_rp': ('string', ''),
    'item_name_hm': ('string', ''),
    'item_name_rp_db': ('string', ''),
    'item_name_hm_db': ('string', ''),
    'item_name_rp_di': ('string', ''),
    'item_name_hm_di': ('string', ''),
    'item_name_repo': ('string', ''),
    'item_name_home': ('string', ''),

    # Item Type Fields
    'item_type': ('string', ''),
    'item_type_rp': ('string', ''),
    'item_type_hm': ('string', ''),
    'item_type_rp_db': ('string', ''),
    'item_type_hm_db': ('string', ''),
    'item_type_rp_di': ('string', ''),
    'item_type_hm_di': ('string', ''),
    'item_type_repo': ('string', ''),
    'item_type_home': ('string', ''),

    # Other Fields
    'git_rp': ('bool', False),
    'dot_struc': ('string', ''),
    'dot_struc_di': ('string', ''),
    'cat_1_di': ('string', ''),
    'cat_1_name_di': ('string', ''),
    'cat_2_di': ('string', ''),
    'comment_di': ('string', ''),
    'no_show_di': ('bool', False),
    'sort_orig': ('Int64', 0),
    'sort_out': ('Int64', -1),

    # Status Fields
    'st_alert': ('string', ''),
    'st_db_all': ('string', ''),
    'st_docs': ('string', ''),
    'st_misc': ('string', '')
}