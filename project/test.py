import os
import unittest
import pandas as pd
from data_pipeline import download_csv, extract, transform, load

class TestETLPipeline(unittest.TestCase):
    def test_download_csv(self):
        # Test downloading CSV data from a given URL
        summer_data = download_csv('https://dati.comune.milano.it/dataset/d9b6975d-7b28-413a-ac7f-cd05432824c1/resource/62de6bfa-1d15-4498-b69a-71f90dbf1018/download/ds1561_stagione_termica_estiva.csv')
        winter_data = download_csv('https://dati.comune.milano.it/dataset/ef94c475-cb1a-4432-bd90-9cb3a739bd71/resource/b5a63c19-4a34-4ba0-8b49-04696372d8d2/download/ds1560_stagione_termica_invernale.csv')
        
        self.assertIsInstance(summer_data, pd.DataFrame)
        self.assertIsInstance(winter_data, pd.DataFrame)
        self.assertTrue(len(summer_data) > 0)
        self.assertTrue(len(winter_data) > 0)
        
    def test_extract(self):
        # Test the extraction process
        df_summer, df_winter = extract()
        self.assertIsInstance(df_summer, pd.DataFrame)
        self.assertIsInstance(df_winter, pd.DataFrame)
        self.assertTrue(len(df_summer) > 0)
        self.assertTrue(len(df_winter) > 0)
        
    def test_transform(self):
        # Test the transformation process
        df_summer = pd.DataFrame({'Metric': ['Temperature'], 'Milano Centro': [25]})
        df_winter = pd.DataFrame({'Metric': ['Temperature'], 'Milano Centro': [5]})
        df_transformed = transform(df_summer, df_winter)
        
        self.assertIsInstance(df_transformed, pd.DataFrame)
        self.assertEqual(df_transformed.iloc[0]['Summer_Value Temperature'], 25)
        self.assertEqual(df_transformed.iloc[0]['Winter_Value Temperature'], 5)
        
    def test_load(self):
        # Test the loading process
        df = pd.DataFrame({'Station': ['Milano Centro'], 'Summer_Value Temperature': [25], 'Winter_Value Temperature': [5]})
        load(df)
        
        # Check if the SQLite database file exists
        self.assertTrue(os.path.exists('../data/milan_climate.db'))
        
if __name__ == '__main__':
    unittest.main()
