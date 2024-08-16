import unittest
import pandas as pd
from dbase09_merge import merge_dataframes

class TestMergeDataFrames(unittest.TestCase):

    def setUp(self):
        # Create sample DataFrames for testing
        self.df1 = pd.DataFrame({
            'df1_item_name': ['item1', 'item2'],
            'df1_item_type': ['type1', 'type2'],
            'df1_value': [10, 20]
        })
        self.df2 = pd.DataFrame({
            'df2_item_name': ['item1', 'item3'],
            'df2_item_type': ['type1', 'type3'],
            'df2_value': [30, 40]
        })
        self.suffix_mapping = {
            'df1': ('df1', self.df1),
            'df2': ('df2', self.df2)
        }

    def test_valid_merge(self):
        result = merge_dataframes(self.df1, self.df2, self.suffix_mapping)
        self.assertIn('unique_id', result.columns)
        self.assertIn('item_name', result.columns)
        self.assertIn('item_type', result.columns)

    def test_invalid_input(self):
        with self.assertRaises(ValueError):
            merge_dataframes("not a dataframe", self.df2, self.suffix_mapping)

    def test_missing_column(self):
        df1_invalid = self.df1.drop(columns=['df1_item_name'])
        suffix_mapping_invalid = {
            'df1': ('df1', df1_invalid),
            'df2': ('df2', self.df2)
        }
        with self.assertRaises(KeyError):
            merge_dataframes(df1_invalid, self.df2, suffix_mapping_invalid)

if __name__ == '__main__':
    unittest.main()