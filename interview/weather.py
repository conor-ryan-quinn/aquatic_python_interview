from datetime import datetime
import pandas as pd

# Input csv constants
TIMESTAMP_COL = "Measurement Timestamp"
STATION_NAME_COL = "Station Name"
TEMP_COL = "Air Temperature"
DATE_FORMAT = "mixed"

# Output csv constants
DATE_COL_NAME = "Date"
MIN_COL_NAME = "Min Temp"
MAX_COL_NAME = "Max Temp"
FIRST_COL_NAME = "First Temp"
LAST_COL_NAME = "Last Temp"


def process_csv(reader, writer):
    df = pd.read_csv(
        reader,
        date_format=DATE_FORMAT,
        parse_dates=[TIMESTAMP_COL],
        usecols=[TIMESTAMP_COL, STATION_NAME_COL, TEMP_COL],
    )

    transform_df(df).to_csv(writer)


def transform_df(input_df):
    # ensure float output for all temp data
    input_df[TEMP_COL] = pd.to_numeric(input_df[TEMP_COL], downcast="float")

    # Split by station name and resample by day
    group_station_resample_day = input_df.groupby(
        [STATION_NAME_COL, pd.Grouper(key=TIMESTAMP_COL, freq="D")]
    )

    # Combine groups to get desired aggregations
    min_temp = group_station_resample_day.min().rename(
        {TEMP_COL: MIN_COL_NAME}, axis=1
    )
    max_temp = group_station_resample_day.max().rename(
        {TEMP_COL: MAX_COL_NAME}, axis=1
    )
    first_temp = group_station_resample_day.first().rename(
        {TEMP_COL: FIRST_COL_NAME}, axis=1
    )
    last_temp = group_station_resample_day.last().rename(
        {TEMP_COL: LAST_COL_NAME}, axis=1
    )

    temp_dataframes = [min_temp, max_temp, first_temp, last_temp]

    # Glue dataframes horizonatally
    concated_temps_df = pd.concat(temp_dataframes, axis=1).reset_index()

    # Prepare for desired CSV format
    result_df = concated_temps_df.set_index(STATION_NAME_COL).rename(
        {TIMESTAMP_COL: DATE_COL_NAME}, axis=1
    )

    return result_df
