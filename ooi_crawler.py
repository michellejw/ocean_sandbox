"""
ooi_crawler.py

This script looks for the raw data files for a requested instrument in the OOI network, then saves the
output of those files to a pickled pandas dataframe.

Specifically created for broadband hydrophone data (looking for miniseed (.mseed) files).
Could be more flexible in the future but this is it for now.

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

    if not os.path.isfile(outfile):
        df_init = pd.DataFrame(columns=['filepath', 'filename', 'starttime'])
        df_init.to_pickle(outfile)
        delayed_start = 0
    else:
        df_from_file = pd.read_pickle(outfile)
        delayed_start = 1
        latest_year = df_from_file.iloc[-1].starttime.year
        latest_month = df_from_file.iloc[-1].starttime.month
        latest_day = df_from_file.iloc[-1].starttime.day
        del df_from_file

    # Define the top level folder for this instrument/site (Axial volcano broadband hydrophone)
    main_url = 'https://rawdata.oceanobservatories.org/files/' + network + '/' + site + '/' + instrument + '/'
    year_folders = url_get_folders(main_url)

    if delayed_start:
        # Start from the most recent year included in the existing pickle file
        latest_year_index = [int(year[:-1]) for year in year_folders].index(latest_year)
        year_folders = year_folders[latest_year_index:]

    # Loop through each year folder
    for ydex, yf in enumerate(year_folders):
        year_url = main_url + yf
        month_folders = url_get_folders(year_url)

        # If lookup file exists, start at the month where it leaves off
        if (ydex == 0) & (delayed_start):
            latest_month_index = [int(month[:-1]) for month in month_folders].index(latest_month)
            month_folders = month_folders[latest_month_index:]

        # Loop through each month folder
        for mdex, mf in enumerate(month_folders):
            month_url = year_url + mf
            day_folders = url_get_folders(month_url)

            # If lookup file exists, start at the day where it leaves off
            if (ydex == 0) & (mdex == 0) & (delayed_start):
                latest_day_index = [int(day[:-1]) for day in day_folders].index(latest_day)
                day_folders = day_folders[latest_day_index:]

            # create empty dataframe for just this month
            df_this_month = pd.DataFrame(columns=['filepath', 'filename', 'starttime'])

            # Loop through each day folder
            for df in day_folders:
                print('Starting: ' + yf.split('/')[0] + '/' + mf.split('/')[0] + '/' + df.split('/')[0])
                day_url = month_url + df
                day_url_contents = requests.get(day_url, timeout=30).content
                # Some days have weird data where there are tons of really short files. Better to skip.
                # Probably would be best to log the days that were skipped in this way so we can check
                # later.
                if day_url_contents.__sizeof__() > 200000:
                    print('Skipping the day because url contents are too large: ' + day_url)
                else:
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
            df_bb = df_bb.drop_duplicates()
            df_bb.to_pickle(outfile)
            del df_bb
            del df_this_month


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
    this_outfile = '../ooi_data/ooi_lookup.pkl'
    # this_outfile = '../../data/ooi_lookup/ooi_lookup.pkl'

    file_crawl(this_network, this_site, this_instrument, this_outfile)
