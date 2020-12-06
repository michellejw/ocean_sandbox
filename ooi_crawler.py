"""
ooi_crawler.py

This script looks for the raw data files for a requested instrument in the OOI network, then saves the
output of those files to a pickled pandas dataframe.

Specifically created for broadband hydrophone data (looking for miniseed (.mseed) files).
Could be more flexible in the future but this is it for now.

TODO:
- check if the requested pickle file already exists. If it does, open it and only add what's missing.
- save to file at intermediate steps
"""

import requests
from bs4 import BeautifulSoup
from dateutil import parser
import pandas as pd
import os
import sys


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

    df_init = pd.DataFrame(columns=['filepath', 'filename', 'starttime'])
    df_init.to_pickle(outfile)

    # Define the top level folder for this instrument/site (Axial volcano broadband hydrophone)
    main_url = 'https://rawdata.oceanobservatories.org/files/' + network + '/' + site + '/' + instrument + '/'
    year_folders = url_get_folders(main_url)

    # Loop through each year folder
    for yf in year_folders:
        year_url = main_url + yf
        month_folders = url_get_folders(year_url)

        # Loop through each month folder
        for mf in month_folders:
            month_url = year_url + mf
            day_folders = url_get_folders(month_url)
            # create empty dataframe for just this month
            df_this_month = pd.DataFrame(columns=['filepath', 'filename', 'starttime'])

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
                    s_time = parser.parse(msfile.split('.mseed')[0][-26:])
                    df_this_month = df_this_month.append({'filepath': day_url,
                                                          'filename': msfile.split('./')[1],
                                                          'starttime': s_time}, ignore_index=True)

            # at the end of each month loop, load the pickle file, add the latest dataframe,
            # then overwrite the previous pickle file.
            df_bb = pd.read_pickle(outfile)
            df_bb = pd.concat([df_bb, df_this_month])
            df_bb.to_pickle(outfile)


def url_get_folders(base_url):
    """
    url_scraper
    Read in the base url using beautiful soup. Read links to folders.

    :param base_url: (string) url pointing to the page containing folder.
    :return: folder_list: (list) list of folder names
    """
    url_contents = requests.get(base_url).content
    soup = BeautifulSoup(url_contents, 'html.parser')
    folder_list = [link.get('href') for link in soup.find_all('a')][6:]

    return folder_list


if __name__ == "__main__":
    # If run as script, do this:
    this_network = 'RS03AXBS'
    this_site = 'LJ03A'
    this_instrument = '09-HYDBBA302'
    # outfile = '../ooi_data/ooi_lookup.pkl'
    this_outfile = '../../data/ooi_lookup/ooi_lookup.pkl'

    file_crawl(this_network, this_site, this_instrument, this_outfile)
