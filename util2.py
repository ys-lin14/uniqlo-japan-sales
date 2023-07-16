# functions for processing sales comments and store opening/closure data
import glob
import numpy as np
import pandas as pd
import re

from pypdf import PdfReader

def get_month_years(text_chunks, month_year_pattern, whitespace_pattern):
    month_years = [
        re.search(month_year_pattern, text_chunk)[0] 
        for text_chunk in text_chunks
    ]
    # replace multiple whitespace with single whitespace since pypdf sometimes adds extra spaces
    month_years = [
        re.sub(whitespace_pattern, ' ', month_year) 
        for month_year in month_years
    ]
    return month_years

def search_for_opening_closure(text_chunk, pattern):
    matches = re.search(pattern, text_chunk)
    if matches is not None:
        count = int(matches.groups()[1])
    else:
        count = np.nan
    return count

def get_openings(text_chunks, opening_pattern):
    openings = [
        search_for_opening_closure(text_chunk, opening_pattern)
        for text_chunk in text_chunks
    ]
    return openings               
         
def get_closures(text_chunks, closure_pattern):
    closures = [
        search_for_opening_closure(text_chunk, closure_pattern)
        for text_chunk in text_chunks
    ]
    return closures

def get_information(text_chunks, information_pattern):
    # consider removing newlines ?
    information = [
        re.search(information_pattern, text_chunk).groups()[1] 
        for text_chunk in text_chunks
    ]
    return information

def create_opening_closure_df(month_years, openings, closures, term):
    opening_closure_df = pd.DataFrame({
        'Period': month_years,
        'Openings': openings,
        'Closures': closures,
        'Term': term
    })
    return opening_closure_df

def create_information_df(month_years, sales_information, other_information):
    information_df = pd.DataFrame({
        'Period': month_years,
        'Sales Comments': sales_information,
        'Other Information': other_information
    })
    return information_df

def remove_newlines(information):
    split_information = information.split('\n')
    split_information = [split for split in split_information if len(split) != 0]
    information_without_newlines = ' '.join(split_information)
    return information_without_newlines

def replace_missing_information_with_nan(information):
    # footers e.g. 1/2 gets included with other information
    # only consider information to be valid if it contains at least one letter
    matches = re.search('[a-zA-Z]', information)
    if matches is None:
        return np.nan
    else:
        return information
    
def clean_information_data(information_data):
    cleaned_information_data = information_data.melt(
        id_vars='Period', 
        var_name='Information Type', 
        value_name='Information'
    )
    
    cleaned_information_data['Information'] = (
        cleaned_information_data['Information'].apply(remove_newlines)
    )
    
    cleaned_information_data['Information'] =(
        cleaned_information_data['Information'].apply(replace_missing_information_with_nan)
    )
    
    cleaned_information_data.dropna(inplace=True, ignore_index=True)
    return cleaned_information_data

def clean_and_save_comments_and_opening_closure_data():
    raw_sales_comment_files = glob.glob('./data/raw/sales_comments/*')
    raw_sales_comment_files.sort()

    month_year_pattern = re.compile('([a-zA-Z]*\s*\d{4})')
    whitespace_pattern = re.compile('\s+')         
    opening_pattern = re.compile('(?<=(Openings:))\s*(\d+)')
    closure_pattern = re.compile('(?<=(Closures:))\s*(\d+)')
    sales_information_pattern = re.compile('(?<=(Sales Information:))((.|\n)*)(?=(\nOther))')
    other_information_pattern = re.compile('(?<=(Other Information:))((.|\n)*)')

    for file in raw_sales_comment_files[4:]:
        reader = PdfReader(file)
        number_of_pages = len(reader.pages)

        august_year = int(re.search('\d{4}', file)[0])
        term = f'September {august_year-1} to August {august_year}' 
        opening_closure_dfs = []
        information_dfs = []
        for page_number in range(number_of_pages):
            page = reader.pages[page_number]
            text = page.extract_text()

            # index from 1 to ignore header
            text_chunks = text.split('■')[1:]
            month_years = get_month_years(text_chunks, month_year_pattern, whitespace_pattern)
            openings = get_openings(text_chunks, opening_pattern)
            closures = get_closures(text_chunks, closure_pattern)
            sales_information = get_information(text_chunks, sales_information_pattern)
            other_information = get_information(text_chunks, other_information_pattern)

            opening_closure_df = create_opening_closure_df(month_years, openings, closures, term)
            information_df = create_information_df(month_years, sales_information, other_information)

            opening_closure_dfs.append(opening_closure_df)
            information_dfs.append(information_df)

        # combine data from different pages
        opening_closure_data = pd.concat(opening_closure_dfs, ignore_index=True)
        information_data = pd.concat(information_dfs, ignore_index=True)
        information_data = clean_information_data(information_data)

        # save sales comments and store opening/closure data
        opening_closure_data.to_csv(
            ('./data/preprocessed/store_openings_and_closures/' +
            f'monthly_openings_closures_sept{august_year-1}_to_aug{august_year}.csv'),
            index=False
        )

        information_data.to_csv(
            f'./data/preprocessed/sales_comments/monthly_sales_comments_sept{august_year-1}_to_aug{august_year}.csv',
            index=False
        )
        
def combine_and_save_sales_comments():
    preprocessed_sales_comment_files = glob.glob('./data/preprocessed/sales_comments/*')
    preprocessed_sales_comment_files.sort()
    
    sales_comment_dfs = [pd.read_csv(file) for file in preprocessed_sales_comment_files]
    combined_sales_comments = pd.concat(sales_comment_dfs, ignore_index=True)
    
    combined_sales_comments.to_csv('./data/preprocessed/monthly_sales_comments.csv', index=False)
    
def combine_and_save_store_openings_and_closures():
    preprocessed_opening_closure_files = glob.glob('./data/preprocessed/store_openings_and_closures/*')
    preprocessed_opening_closure_files.sort()
    
    opening_closure_files_dfs = [pd.read_csv(file) for file in preprocessed_opening_closure_files]
    combined_opening_closure_data = pd.concat(opening_closure_files_dfs, ignore_index=True)
    
    combined_opening_closure_data.to_csv('./data/preprocessed/monthly_openings_and_closures.csv', index=False)