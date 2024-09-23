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


f_types_vals = {
    # Unique ID Fields
    'unique_id': {'dtype': 'Int64', 'default': 0},
    'unique_id_rp': {'dtype': 'Int64', 'default': 0},
    'unique_id_db': {'dtype': 'Int64', 'default': 0},
    'unique_id_hm': {'dtype': 'Int64', 'default': 0},
    'unique_id_di': {'dtype': 'Int64', 'default': 0},

    # Item Name Fields
    'item_name': {'dtype': 'string', 'default': np.nan},
    'item_name_rp': {'dtype': 'string', 'default': np.nan},
    'item_name_hm': {'dtype': 'string', 'default': np.nan},
    'item_name_rp_db': {'dtype': 'string', 'default': np.nan},
    'item_name_hm_db': {'dtype': 'string', 'default': np.nan},
    'item_name_rp_di': {'dtype': 'string', 'default': np.nan},
    'item_name_hm_di': {'dtype': 'string', 'default': np.nan},
    'item_name_repo': {'dtype': 'string', 'default': np.nan},
    'item_name_home': {'dtype': 'string', 'default': np.nan},

    # Item Type Fields
    'item_type': {'dtype': 'string', 'default': np.nan},
    'item_type_rp': {'dtype': 'string', 'default': np.nan},
    'item_type_hm': {'dtype': 'string', 'default': np.nan},
    'item_type_rp_db': {'dtype': 'string', 'default': np.nan},
    'item_type_hm_db': {'dtype': 'string', 'default': np.nan},
    'item_type_rp_di': {'dtype': 'string', 'default': np.nan},
    'item_type_hm_di': {'dtype': 'string', 'default': np.nan},
    'item_type_repo': {'dtype': 'string', 'default': np.nan},
    'item_type_home': {'dtype': 'string', 'default': np.nan},

    # Other Fields
    'git_rp': {'dtype': 'bool', 'default': np.nan},
    'dot_struc': {'dtype': 'string', 'default': np.nan},
    'dot_struc_di': {'dtype': 'string', 'default': np.nan},
    'cat_1_di': {'dtype': 'string', 'default': np.nan},
    'cat_1_name_di': {'dtype': 'string', 'default': np.nan},
    'cat_2_di': {'dtype': 'string', 'default': np.nan},
    'comment_di': {'dtype': 'string', 'default': np.nan},
    'no_show_di': {'dtype': 'bool', 'default': np.nan},
    'sort_orig': {'dtype': 'Int64', 'default': 0},
    'sort_out': {'dtype': 'Int64', 'default': -1},

    # Status Fields
    'st_alert': {'dtype': 'string', 'default': np.nan},
    'st_db_all': {'dtype': 'string', 'default': np.nan},
    'st_docs': {'dtype': 'string', 'default': np.nan},
    'st_misc': {'dtype': 'string', 'default': np.nan}
}