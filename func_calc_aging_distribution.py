'''
calulate the distribution of the temperature data for each device.

'''

import pandas as pd
import math
import logs
import datetime as dt
import os

dir_path = os.path.dirname(os.path.realpath(__file__))

input_file = f'{dir_path}/d_hotspot_aging_calc_output.pd'
input_file_devices = f'{dir_path}/d_shared_attributes_by_device.pd'
output_file = f'{dir_path}/d_aging_distribution_by_device.pd'

MAX_BINS = 25
BINS_QTY = 15                   # the number of bins for loading distribution within the normal operating range
temp_rating = 120               # temperature value
temp_field_key = 'hotspot'     # key for the temperature to use in distribution
aging_data_key = 'trxTemp3_L'

def setup_bins(dev_rating):
    # set up bins
    bin_size = int(dev_rating / BINS_QTY)
    bin_count = MAX_BINS

    df_bins = pd.DataFrame({
        'bin': range(0, bin_count),
        'aging': 0,
        'count_aging': 0,
        'time': 0
    }, dtype='float')
    df_bins['lower'] = df_bins['bin'] * bin_size
    df_bins['upper'] = (df_bins['bin'] + 1) * bin_size
    df_bins['percent'] = round(df_bins['upper'] / dev_rating * 100)

    # print(df_bins)
    return df_bins


# evaluate the loading levels and populate the
def evaluate_loading(df_dev_telemetry, df_bins):
    last_ts = 0
    last_temp_bin_ind = 0
    last_norm_temp_bin_ind = 0
    total_time = 0
    bin_size = df_bins['upper'][1] - df_bins['upper'][0]
    # print(f'Bin size is {bin_size}')

    for ind, tel in df_dev_telemetry.iterrows():
        ts = int(ind[1]/1000)

        if last_ts is not 0:
            duration = ts - last_ts
            if duration > 6000:
                print(f'Long ts difference {duration / 60} minutes. ts {dt.datetime.fromtimestamp(last_ts)} to {dt.datetime.fromtimestamp(ts)}')
            total_time += duration
            last_ts = ts
            # print(tel)
            df_bins.at[last_temp_bin_ind, 'aging'] += tel[aging_data_key]
            # print(df_bins.at[last_temp_bin_ind, 'aging'])
            df_bins.at[last_temp_bin_ind, 'count_aging'] += 1
            df_bins.at[last_temp_bin_ind, 'time'] += duration
        else:
            last_ts = ts

        last_temp_bin_ind = min(math.trunc(tel[temp_field_key] / bin_size), MAX_BINS - 1)

        if last_norm_temp_bin_ind < 0:
            last_norm_temp_bin_ind = 0
    return total_time


def main():
    # load the arguments and read config file
    #arguments = cfg.parse_command_line_args()
    #conf = cfg.read_conf_file(arguments.config)

    # load devices list with shared attributes
    df_devices = pd.read_csv(input_file_devices)

    # get the historic transformer temperature data
    df_telemetry = pd.read_csv(input_file)
    df_telemetry.set_index(['device_id', 'ts'], inplace=True)

    # print(df_telemetry.index.levels[0])

    # get last token from file in order to connect to Thingsboard API
    with open(f'{dir_path}/d_token.pd', 'rt') as token_in:
        token = token_in.read()
        token_in.close()

    if token is not None:

        df_distributions = pd.DataFrame()

        # perform for each of the devices
        for index, dev in df_devices.iterrows():
            print(f'Device {index} - {dev["id"]}')

            # get device telemetry for each device
            if dev['id'] in df_telemetry.index:
                df_dev_telemetry = df_telemetry.loc[[dev['id']]]
                print(f'with {df_dev_telemetry.shape[0]} telemetry records ')

                if df_dev_telemetry.shape[0] > 10:
                    df_dev_telemetry.sort_index(inplace=True)
                    df_dev_telemetry = df_dev_telemetry[~df_dev_telemetry.index.duplicated(keep='first')]
                    # fill forward with last known values
                    df_dev_telemetry.fillna(method='ffill', inplace=True)
                    # replace remaining NaNs with 0
                    df_dev_telemetry.fillna(0, inplace=True)

                    # set up bins
                    df_bins = setup_bins(temp_rating)

                    # calculation each bin's value
                    total_time = evaluate_loading(df_dev_telemetry, df_bins)

                    # change to hours from seconds
                    df_bins['aging'] /= 3600
                    df_bins['time'] /= 3600
                    total_time = total_time / 3600
                    print(f'Time evaluated {total_time} hours')

                    df_bins = df_bins.append({'bin': 25,
                                              'aging': df_bins['aging'].sum(),
                                              'count_aging': df_bins['count_aging'].sum(),
                                              'time': df_bins['time'].sum(),
                                              'lower': 0,
                                              'upper': 0,
                                              'percent': 0}, ignore_index=True)

                    # add the device name
                    df_bins['device_id'] = dev["id"]

                    # print(df_bins.to_string())
                    df_distributions = pd.concat([df_distributions, df_bins], sort=False)

        if df_bins.size > 0:
            df_distributions.set_index(['device_id', 'bin'])
            df_distributions.sort_index()
            df_distributions.to_csv(output_file, header=True, index=False)
        else:
            logs.log_error('temperature distribution evaluation: no telemetry data!')
            logs.console('ERROR: TB_Connect: no telemetry data')


if __name__ == '__main__':
    main()

