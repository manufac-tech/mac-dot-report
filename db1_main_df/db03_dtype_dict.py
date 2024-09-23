import numpy as np


def get_valid_types():
    valid_types_repo = {
        'file': ['file', 'file_alias'],
        'folder': ['folder', 'folder_alias']
    }

    valid_types_home = {
        'file': 'file_sym',
        'folder': 'folder_sym'
    }

    return valid_types_repo, valid_types_home



# field_types = {

#     # Unique ID Fields
#     'unique_id': 'Int64',
#     'unique_id_rp': 'Int64',
#     'unique_id_db': 'Int64',
#     'unique_id_hm': 'Int64',
#     'unique_id_di': 'Int64',

#     # Item Name Fields
#     'item_name': 'string',
    
#     'item_name_rp': 'string',
#     'item_name_hm': 'string',
#     'item_name_rp_db': 'string',
#     'item_name_hm_db': 'string',
#     'item_name_rp_di': 'string',
#     'item_name_hm_di': 'string',
    
#     'item_name_repo': 'string',
#     'item_name_home': 'string',

#     # Item Type Fields
#     'item_type': 'string',
    
#     'item_type_rp': 'string',
#     'item_type_hm': 'string',
#     'item_type_rp_db': 'string',
#     'item_type_hm_db': 'string',
#     'item_type_rp_di': 'string',
#     'item_type_hm_di': 'string',
    
#     'item_type_repo': 'string',
#     'item_type_home': 'string',

#     # Other Fields
#     'git_rp': 'bool',
    
#     'dot_struc': 'string',

#     'dot_struc_di': 'string',
#     'cat_1_di': 'string',
#     'cat_1_name_di': 'string',
#     'cat_2_di': 'string',
#     'comment_di': 'string',
#     'no_show_di': 'bool',
#     'sort_orig': 'Int64',
    
#     'sort_out': 'Int64',
    
#     # Status Fields
#     'st_alert': 'string',
#     'st_db_all': 'string',
#     'st_docs': 'string',
#     'st_misc': 'string',
# }




f_types_vals = {
    # Unique ID Fields
    'unique_id': {'dtype': 'Int64', 'val_0': 0, 'val_1': np.nan},
    'unique_id_rp': {'dtype': 'Int64', 'val_0': 0, 'val_1': np.nan},
    'unique_id_db': {'dtype': 'Int64', 'val_0': 0, 'val_1': np.nan},
    'unique_id_hm': {'dtype': 'Int64', 'val_0': 0, 'val_1': np.nan},
    'unique_id_di': {'dtype': 'Int64', 'val_0': 0, 'val_1': np.nan},

    # Item Name Fields
    'item_name': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_name_rp': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_name_hm': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_name_rp_db': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_name_hm_db': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_name_rp_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_name_hm_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_name_repo': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_name_home': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},

    # Item Type Fields
    'item_type': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_type_rp': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_type_hm': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_type_rp_db': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_type_hm_db': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_type_rp_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_type_hm_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_type_repo': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'item_type_home': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},

    # Other Fields
    'git_rp': {'dtype': 'bool', 'val_0': False, 'val_1': np.nan},
    'dot_struc': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'dot_struc_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'cat_1_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'cat_1_name_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'cat_2_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'comment_di': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'no_show_di': {'dtype': 'bool', 'val_0': False, 'val_1': np.nan},
    'sort_orig': {'dtype': 'Int64', 'val_0': 0, 'val_1': np.nan},
    'sort_out': {'dtype': 'Int64', 'val_0': -1, 'val_1': np.nan},

    # Status Fields
    'st_alert': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'st_db_all': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'st_docs': {'dtype': 'string', 'val_0': '', 'val_1': np.nan},
    'st_misc': {'dtype': 'string', 'val_0': '', 'val_1': np.nan}
}





# field_types_with_defaults = {
#     # Unique ID Fields
#     'unique_id': ('Int64', 0),
#     'unique_id_rp': ('Int64', 0),
#     'unique_id_db': ('Int64', 0),
#     'unique_id_hm': ('Int64', 0),
#     'unique_id_di': ('Int64', 0),

#     # Item Name Fields
#     'item_name': ('string', ''),
#     'item_name_rp': ('string', ''),
#     'item_name_hm': ('string', ''),
#     'item_name_rp_db': ('string', ''),
#     'item_name_hm_db': ('string', ''),
#     'item_name_rp_di': ('string', ''),
#     'item_name_hm_di': ('string', ''),
#     'item_name_repo': ('string', ''),
#     'item_name_home': ('string', ''),

#     # Item Type Fields
#     'item_type': ('string', ''),
#     'item_type_rp': ('string', ''),
#     'item_type_hm': ('string', ''),
#     'item_type_rp_db': ('string', ''),
#     'item_type_hm_db': ('string', ''),
#     'item_type_rp_di': ('string', ''),
#     'item_type_hm_di': ('string', ''),
#     'item_type_repo': ('string', ''),
#     'item_type_home': ('string', ''),

#     # Other Fields
#     'git_rp': ('bool', False),
#     'dot_struc': ('string', ''),
#     'dot_struc_di': ('string', ''),
#     'cat_1_di': ('string', ''),
#     'cat_1_name_di': ('string', ''),
#     'cat_2_di': ('string', ''),
#     'comment_di': ('string', ''),
#     'no_show_di': ('bool', False),
#     'sort_orig': ('Int64', 0),
#     'sort_out': ('Int64', -1),

#     # Status Fields
#     'st_alert': ('string', ''),
#     'st_db_all': ('string', ''),
#     'st_docs': ('string', ''),
#     'st_misc': ('string', '')
# }