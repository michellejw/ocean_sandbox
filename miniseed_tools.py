"""
miniseed_tools.py

A set of functions for working with miniseed data from OOI broadband hydrophone data
Requires that there is a miniseed lookup file created using ooi_crawler.py

TODO: call ooi_crawler.py to update an existing ooi lookup table.
TODO: read multiple time steps to generate a long-term spectral average
TODO: Add daily LTSAs to an xarray DataArray


"""

import obspy
import pandas as pd
import numpy as np
import datetime
import xarray as xr
from scipy import signal
import matplotlib.pyplot as plt

from scipy.fft import fft, fftfreq




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


def find_by_time(df_in, start_time, end_time):
    """
    Find and return a dataframe containing only those rows where 'starttime' of the miniseed file is between start_time
    and end_time (include start point, not end point)

    :param df_in: (Pandas DataFrame)
    :param start_time: (numpy datetime) requested start time
    :param end_time: (numpy datetime) requested end time
    :return: (Pandas DataFrame) dataframe with the subset of rows between start_time and end_time
    """
    return df_in[(df_in['starttime'] >= start_time) & (df_in['starttime'] < end_time)]


def make_ltsa(df_in, segment_seconds, percent_overlap, data_decimation_factor):

    # Get the length of the frequency vector so we can pre-allocate the ltsa array
    data, sample_rate, t_start, t_end = \
        load_miniseed(df_in.iloc[0], decimation_factor=data_decimation_factor)
    points_per_segment = int(segment_seconds * sample_rate)
    ltsa = [int(points_per_segment/2+1)*[0]]*len(df_in)
    row_number = 0
    for rdex, row in df_in.iterrows():
        print('Row ' + str(row_number) + ' of ' + str(len(df_in)))
        row_number += 1
        # Load the current row
        data, sample_rate, t_start, t_end = \
            load_miniseed(row, decimation_factor=data_decimation_factor)
        # Prepare inputs for spectrogram function (convert segment_seconds and percent_overlap to samples)
        overlap_points = int(points_per_segment * percent_overlap / 100)
        # Compute spectrogram
        frequency, time, signal_spectrogram = signal.spectrogram(data, sample_rate, nperseg=points_per_segment, noverlap=overlap_points)
        spectrogram_db = 20 * np.log10(signal_spectrogram + 0.001)
        vmin = np.percentile(spectrogram_db, 5)
        vmax = np.percentile(spectrogram_db, 95)
        # ### Test plotting - uncomment for debugging/exploration
        # plt.pcolormesh(time, frequency / 1000, spectrogram_db, vmin=vmin, vmax=vmax, shading='auto')
        # plt.xlabel('Time (s)')
        # plt.ylabel('Frequency (kHz)')
        # plt.colorbar()
        # ###
        # Number of samples in normalized_tone
        ltsa.append(np.median(spectrogram_db, axis=1).tolist())
    return ltsa


if __name__ == "__main__":
    path_to_df = '../ooi_data/ooi_lookup.pkl'
    df = pd.read_pickle(path_to_df)

    # Specify start and end times in [Year, month, day, hour, minute, second]
    t_start_data = datetime.datetime(2016, 1, 15, 0, 0, 0)
    t_end_data = datetime.datetime(2016, 1, 16, 0, 0, 0)
    df_sub = find_by_time(df, t_start_data, t_end_data)

    # Parameters for the LTSA
    decimation_factor = 1
    time_segment = 0.1
    pct_overlap = 20

    # Construct a long term spectral average from the current set of miniseed files
    average_array = make_ltsa(df_sub, time_segment, pct_overlap, decimation_factor)

    print('pause')
    # for rdex, row in df_sub.iterrows():
    #     dat, fs, t_start, t_end = load_miniseed(row, decimation_factor=16)
    #     make_ltsa(dat, fs, 1, 20)