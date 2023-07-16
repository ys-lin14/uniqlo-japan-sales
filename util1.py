# functions for processing sales data
import camelot
import glob
import numpy as np
import pandas as pd
import re

def simplify_store_type(store_type):
    simplified_store_type = re.match('(.*?Stores)', store_type).group(0)
    return simplified_store_type
    
def clean_sales_data(sales_data):
    cleaned_sales_data = sales_data.copy()
    
    # remove newlines and any accompanying spaces
    cleaned_sales_data = cleaned_sales_data.applymap(lambda s: ' '.join(s.split()))
    
    # replace "Directly-run Stores", "Directly run Stores", and "Own Stores" from the
    # 2015, 2017 and other files with "Directly-Run Stores"
    cleaned_sales_data.replace(
        '((?i)([a-z\s]+)(?<!Same\s))Stores', 
        'Directly-Run Stores', 
        regex=True, 
        inplace=True
    )
    
    cleaned_sales_data.replace(
        'Average Purchase', 
        'Average Purchase per Customer', 
        regex=True, 
        inplace=True
    )
    
    cleaned_sales_data.loc[1, 0] = 'Store Type' # Same or Directly-Run
    cleaned_sales_data.loc[1, 1] = 'Metric'     # Net Sales, 
    
    # forward fill Store Type column
    cleaned_sales_data.loc[2:, 0].replace('', np.nan, inplace=True)
    cleaned_sales_data = cleaned_sales_data.ffill()
    
    # extract whether Metric includes Online Sales
    cleaned_sales_data[17] = ''
    cleaned_sales_data.loc[1, 17] = 'Includes Online Sales'
    cleaned_sales_data.loc[2:, 17] = cleaned_sales_data.loc[2:, 0].apply(
        lambda s: s.__contains__('Online Sales')
    ).astype(int)
    
    # flip column if excluding Online Sales (2017 file)
    online_sales_are_flipped = cleaned_sales_data.loc[2:, 0].apply(
        lambda s: s.__contains__('excluding Online Sales')
    ).any()
    if online_sales_are_flipped:
        cleaned_sales_data[17] = cleaned_sales_data[17].replace({0: 1, 1: 0})
        
    # simplify the Store Type column to contain Same Stores and Directly-Run Stores
    cleaned_sales_data.loc[2:, 0] = cleaned_sales_data.loc[2:, 0].apply(simplify_store_type)
    
    # re-order columns
    cleaned_sales_data = cleaned_sales_data[[0, 1, 17] + list(range(2, 17))]
    return cleaned_sales_data

def clean_and_save_sales_data():
    raw_sales_data_files = glob.glob('./data/raw/sales_data/*')
    raw_sales_data_files.sort()
    
    # exclude 2013 to 2016 files
    year_pattern = re.compile('(\d{4})')
    for file in raw_sales_data_files[4:]:
        raw_sales_data = camelot.read_pdf(file, split_text=True)
        cleaned_sales_data = clean_sales_data(raw_sales_data[0].df)

        august_year = int(re.search(year_pattern, file).group(0))
        cleaned_sales_data.to_csv(
            f'./data/preprocessed/sales_data/monthly_sales_sept{august_year-1}_to_aug{august_year}.csv',
            header=False,
            index=False
        )
        
def unpivot_combine_and_save_sales_data():
    preprocessed_sales_data_files = glob.glob('./data/preprocessed/sales_data/*')
    preprocessed_sales_data_files.sort()
    
    combined_sales_data = pd.DataFrame()
    for file in preprocessed_sales_data_files:
        preprocessed_sales_data = pd.read_csv(file, skiprows=1)
        preprocessed_sales_data_long = preprocessed_sales_data.melt(
            id_vars=preprocessed_sales_data.columns[:3], 
            var_name=['Period'], 
            value_name='Percentage Change'
        )

        # convert to percentage increase / decrease
        preprocessed_sales_data_long['Percentage Change'] = (
            preprocessed_sales_data_long['Percentage Change'] - 100
        )

        term = f'September {file[-19:-15]} to August {file[-8:-4]}'
        preprocessed_sales_data_long['Term'] = term

        combined_sales_data = pd.concat((combined_sales_data, preprocessed_sales_data_long))

    combined_sales_data['Period Type'] = combined_sales_data['Period'].apply(len)
    combined_sales_data['Period Type'] = combined_sales_data['Period Type'].map({
        7: 'Monthly',
        15: 'Annual',
        21: 'Biannual',
    })
    
    combined_sales_data.to_csv('./data/preprocessed/sales_data.csv', index=False)