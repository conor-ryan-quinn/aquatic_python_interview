import pandas as pd

# Input csv constants
TIMESTAMP_COL = "Measurement Timestamp"
STATION_NAME_COL = "Station Name"
TEMP_COL = "Air Temperature"
INPUT_DATE_FORMAT = "mixed"
CHUNK_SIZE = 1000

# Intermediary column to help with batching aggregation
EXTRA_TIMESTAMP_COL = "Duplicate Timestamp"

# Output csv constants
DATE_COL_NAME = "Date"
OUTPUT_DATE_FORMAT = "%m/%d/%Y"
MIN_COL_NAME = "Min Temp"
MAX_COL_NAME = "Max Temp"
FIRST_COL_NAME = "First Temp"
LAST_COL_NAME = "Last Temp"


def process_csv(reader, writer):

    df_chunks = pd.read_csv(
        reader,
        chunksize=CHUNK_SIZE,
        date_format=INPUT_DATE_FORMAT,
        parse_dates=[TIMESTAMP_COL],
        usecols=[TIMESTAMP_COL, STATION_NAME_COL, TEMP_COL],
    )

    transform_df_batch(df_chunks).to_csv(writer)


def transform_df_batch(df_chunks):

    # Dataframes used to accumulate aggregations as batches are processed
    min_acc_df = pd.DataFrame()
    max_acc_df = pd.DataFrame()
    first_acc_df = pd.DataFrame()
    last_acc_df = pd.DataFrame()

    for chunk in df_chunks:

        # A second timestamp column is added to the dataframe - this is done
        # because when the data is downsampled by day to get aggregation data,
        # hourly information is lost. If the data for
        # a day is split in between batches, hourly info must be used to
        # determine the first and last temperatures taken
        chunk[EXTRA_TIMESTAMP_COL] = chunk[TIMESTAMP_COL]

        # ensure float output for all temp data
        chunk[TEMP_COL] = pd.to_numeric(chunk[TEMP_COL], downcast="float")

        # Split by station name and resample by day
        group_station_resample_day = groupby_name_resample_day(
            chunk, STATION_NAME_COL, TIMESTAMP_COL)

        # Combine groups to get desired aggregations
        min_chunk = group_station_resample_day.min().rename(
            {TEMP_COL: MIN_COL_NAME}, axis=1
        )
        max_chunk = group_station_resample_day.max().rename(
            {TEMP_COL: MAX_COL_NAME}, axis=1
        )
        first_chunk = group_station_resample_day.first().rename(
            {TEMP_COL: FIRST_COL_NAME}, axis=1
        )
        last_chunk = group_station_resample_day.last().rename(
            {TEMP_COL: LAST_COL_NAME}, axis=1
        )

        min_acc_df = pd.concat([min_acc_df, min_chunk])
        max_acc_df = pd.concat([max_acc_df, max_chunk])
        first_acc_df = pd.concat([first_acc_df, first_chunk])
        last_acc_df = pd.concat([last_acc_df, last_chunk])

    # After all batches have been aggregated, group by extra timestamp column
    # to ensure that first and last data is correct
    result_min = groupby_name_resample_day(
        min_acc_df, STATION_NAME_COL, EXTRA_TIMESTAMP_COL).min()
    result_max = groupby_name_resample_day(
        max_acc_df, STATION_NAME_COL, EXTRA_TIMESTAMP_COL).max()
    result_first = groupby_name_resample_day(
        first_acc_df, STATION_NAME_COL, EXTRA_TIMESTAMP_COL).first()
    result_last = groupby_name_resample_day(
        last_acc_df, STATION_NAME_COL, EXTRA_TIMESTAMP_COL).last()

    temp_dataframes = [result_min, result_max, result_first, result_last]

    # Glue dataframes horizonatally
    concated_temps_df = pd.concat(temp_dataframes, axis=1).reset_index()

    result_df = prepare_for_output_format(
        concated_temps_df, EXTRA_TIMESTAMP_COL)

    return result_df


# Group data by the name of a column and resample data by day
def groupby_name_resample_day(
        df: pd.DataFrame | pd.Series, name_col: str, date_col: str):
    return df.groupby(
        [
            name_col,
            pd.Grouper(key=date_col, freq="D"),
        ]
    )


# Prepare for desired CSV format
def prepare_for_output_format(
        df: pd.DataFrame, time_col_name: str) -> pd.DataFrame:

    df = df.set_index([STATION_NAME_COL]).rename(
        {time_col_name: DATE_COL_NAME}, axis=1
    )
    df[DATE_COL_NAME] = df[DATE_COL_NAME].dt.strftime(
        OUTPUT_DATE_FORMAT)

    return df


# Transform without batch. Used for testing
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

    result_df = prepare_for_output_format(concated_temps_df, TIMESTAMP_COL)

    return result_df
