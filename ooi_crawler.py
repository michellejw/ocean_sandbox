'''
ooi_crawler.py

This script looks for the raw data files for a requested instrument in the OOI network, then saves the
output of those files to a pickled pandas dataframe.

Specifically created for broadband hydrophone data (looking for miniseed (.mseed) files).
Could be more flexible in the future but this is it for now.

TODO:
- check if the requested pickle file already exists. If it does, open it and only add what's missing.
- save to file at intermediate steps
'''

import requests
from bs4 import BeautifulSoup
import numpy as np
from dateutil import parser
import pandas as pd
import os, sys

def file_crawl(network, site, instrument, outfile):
    """
    file_crawl
    A function that crawls through the folders of a specific instrument on the ooi raw data website
    and constructs a lookup table (a pandas dataframe, pickled at a requested location).
    The pandas dataframe has three columns: path to the file, the name of the miniseed file

    :param network: (string) ooi network name, e.g. 'RS03AXBS'
    :param site: (string) ooi site code, e.g. 'LJ03A'
    :param instrument: (string) instrument code, e.g. '09-HYDBBA302'
    :param outfile: (string) file name including path
    """
    # check whether the requested folder exists, and if not return an error
    requested_dir = os.path.dirname(outfile)
    if not os.path.exists(requested_dir):
        sys.exit('The requested path does not exist --> ' + requested_dir)

    df_init = pd.DataFrame(columns=['filepath','filename','starttime'])
    df_init.to_pickle(outfile)

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
            # create empty dataframe for just this month
            df_thismonth = pd.DataFrame(columns=['filepath', 'filename', 'starttime'])

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
                    df_thismonth = df_thismonth.append({'filepath': day_url,
                                   'filename': msfile.split('./')[1],
                                   'starttime': stime}, ignore_index=True)

            # at the end of each month loop, load the pickle file, add the latest dataframe,
            # then overwrite the previous pickle file.
            df_bb = pd.read_pickle(outfile)
            df_bb = pd.concat([df_bb,df_thismonth])
            df_bb.to_pickle(outfile)


if __name__ == "__main__":
    # If run as script, do this:
    network = 'RS03AXBS'
    site = 'LJ03A'
    instrument = '09-HYDBBA302'
    # outfile = '../ooi_data/ooi_lookup.pkl'
    outfile = '../../data/ooi_lookup/ooi_lookup.pkl'
    
    file_crawl(network, site, instrument, outfile)