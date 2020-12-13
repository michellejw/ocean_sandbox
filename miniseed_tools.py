"""
miniseed_tools.py

A set of functions for working with miniseed data from OOI broadband hydrophone data
Requires that there is a miniseed lookup file created using ooi_crawler.py

TODO: return indices from the data lookup table that fall within a requested date/time range
TODO: call ooi_crawler.py to update an existing ooi lookup table.
TODO: read multiple time steps to generate a long-term spectral average (start w 1 LTSA per day)
TODO: Add daily LTSAs to an xarray DataArray


"""

import obspy
import pandas as pd


def load_miniseed(ooi_lookup_row, **kwargs):
    '''
    Load a miniseed file based on information contained in a row from the dataframe contained within the
    ooi_lookup.pkl file

    :param ooi_lookup_row: (pandas series) row extracted from the ooi_lookup dataframe
    :return dat: (numpy array) amplitude time series, interval = 1/fs
    :return fs: (int) samples per second (Hz)
    :return starttime: () start date and time of the current data stream
    :return endtime: () end date and time of the current data stream
    '''
    if 'decimation_factor' in kwargs.keys():
        dec_factor = kwargs.get('decimation_factor')
        do_decimate = 1
    else:
        dec_factor = 1
        do_decimate = 0

    mseed_data_url = ooi_lookup_row['filepath'] + ooi_lookup_row['filename']
    stream = obspy.read(mseed_data_url, ssl_verify=False)  # Fetch from data server

    # If decimation is requested, do it here
    if do_decimate:
        stream.decimate(dec_factor, no_filter=True)

    # Pull out the raw data
    dat = stream[0].data

    # Extract info from the stream metadata
    fs = stream.traces[0].stats.sampling_rate
    starttime = stream.traces[0].stats.starttime
    endtime = stream.traces[0].stats.endtime

    return dat, fs, starttime, endtime

if __name__ == "__main__":
    path_to_df = '../ooi_data/ooi_lookup.pkl'
    df = pd.read_pickle(path_to_df)