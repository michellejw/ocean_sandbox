'''
ooi_crawler.py

This script looks for the raw data files for a requested instrument in the OOI network, then saves the output of those files to a pickled pandas dataframe.

TODO:
- check if the requested pickle file already exists. If it does, open it and only add what's missing.
'''

import requests
from bs4 import BeautifulSoup
import numpy as np
from dateutil import parser
import pandas as pd

def file_crawl(network, site, instrument, outfile):
    # Create a dataframe to hold the data file path/time information 
    # (miniseed file name does not contain end time, so removed that column. 
    # maybe there's another way to get that info without actually opening the 
    # file)
    df_bb = pd.DataFrame(columns=['filepath','filename','starttime'])

    # Define the top level folder for this instrument/site (Axial volcano broadband hydrophone)
    main_url = 'https://rawdata.oceanobservatories.org/files/' + network + '/' + site + '/' + instrument + '/'
    main_url_contents = requests.get(main_url).content
    # Parse the main page using beautiful soup
    soup = BeautifulSoup(main_url_contents, 'html.parser')
    year_folders = [link.get('href') for link in soup.find_all('a')][6:]

    # Loop through each year folder
    for yf in year_folders:
        year_url = main_url + yf
        year_url_contents = requests.get(year_url).content
        soup = BeautifulSoup(year_url_contents, 'html.parser')
        month_folders = [link.get('href') for link in soup.find_all('a')][6:]

        # Loop through each month folder
        for mf in month_folders:
            month_url = year_url + mf
            month_url_contents = requests.get(month_url).content
            soup = BeautifulSoup(month_url_contents, 'html.parser')
            day_folders = [link.get('href') for link in soup.find_all('a')][6:]

            # Loop through each day folder
            for df in day_folders:
                print('Starting: ' + yf.split('/')[0] + '/' + mf.split('/')[0] + '/' + df.split('/')[0])
                day_url = month_url + df
                day_url_contents = requests.get(day_url).content
                soup = BeautifulSoup(day_url_contents, 'html.parser')
                all_links = [link.get('href') for link in soup.find_all('a')]
                mseed_files = [i for i in all_links if '.mseed' in i]

                # For each miniseed file, add row to dataframe with file path, 
                # file name, start and end date/time
                for msfile in mseed_files:
                    stime = parser.parse(msfile.split('.mseed')[0][-26:])
                    df_bb = df_bb.append({'filepath': day_url,
                                   'filename': msfile.split('./')[1],
                                   'starttime': stime}, ignore_index=True)
                    
    df_bb.to_pickle(outfile)          
    
if __name__ == "__main__":
    # If run as script, do this:
    network = 'RS03AXBS'
    site = 'LJ03A'
    instrument = '09-HYDBBA302'
    outfile = '../../data/ooi_lookup/ooi_lookup'
    
    file_crawl(network, site, instrument, outfile)