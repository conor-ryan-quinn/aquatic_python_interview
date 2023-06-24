from interview import weather
import io
import numpy as np
import pandas as pd
import random

DF_COLUMNS = [
    weather.STATION_NAME_COL,
    weather.TIMESTAMP_COL,
    weather.TEMP_COL
]

STATIONS = ["Foster Weather Station" for _ in range(0, 10000)]
DATES_RANGE_LONG = pd.date_range("1/1/2000", periods=10000, freq="H")

TEMPS_ASC = [i for i in range(0, 10000)]
DATA_ASC = zip(STATIONS, DATES_RANGE_LONG, TEMPS_ASC)

TEMPS_DESC = [i for i in reversed(range(0, 1000))]
DATA_DESC = zip(STATIONS, DATES_RANGE_LONG, TEMPS_DESC)

DATES_RANGE_SHORT = pd.date_range("1/1/2000", periods=13, freq="H")
TEMPS_SPEC = [12, 4, 7, 8, 2, 13, 7, 5, 3, 10, 9, 8, 4]
DATA_SPEC = zip(STATIONS, DATES_RANGE_SHORT, TEMPS_SPEC)

# A data frame with all ascending temperatures
TEST_INPUT_ASC = pd.DataFrame(DATA_ASC, columns=DF_COLUMNS)

# Descending temps
TEST_INPUT_DESC = pd.DataFrame(DATA_DESC, columns=DF_COLUMNS)

# Specific values for testing
TEST_INPUT_SPEC = pd.DataFrame(DATA_SPEC, columns=DF_COLUMNS)


# If all temps are ascending, corrent aggregation would
# result in the min temp for a given day being the same as the
# first temp and max temp same as last temp
def test_with_ascending_data():
    result_asc = weather.transform_df(TEST_INPUT_ASC)
    random_row_number = random.randint(0, len(result_asc.index) - 1)

    min_temp = result_asc.iloc[random_row_number]["Min Temp"]
    max_temp = result_asc.iloc[random_row_number]["Max Temp"]
    first_temp = result_asc.iloc[random_row_number]["First Temp"]
    last_temp = result_asc.iloc[random_row_number]["Last Temp"]

    assert min_temp == first_temp and max_temp == last_temp


# If all temps are descending, corrent aggregation would
# result in the max temp for a given day being the same as the
# first temp and min temp same as last temp
def test_with_descending_data():
    result_desc = weather.transform_df(TEST_INPUT_DESC)
    random_row_number = random.randint(0, len(result_desc.index) - 1)

    min_temp = result_desc.iloc[random_row_number]["Min Temp"]
    max_temp = result_desc.iloc[random_row_number]["Max Temp"]
    first_temp = result_desc.iloc[random_row_number]["First Temp"]
    last_temp = result_desc.iloc[random_row_number]["Last Temp"]

    assert max_temp == first_temp and min_temp == last_temp


# Test that aggregations as expected for specific sample input
def test_expected_values_single_day():
    result = weather.transform_df(TEST_INPUT_SPEC)

    expected_min = min(TEMPS_SPEC)
    expected_max = max(TEMPS_SPEC)
    expected_first = TEMPS_SPEC[0]
    expected_last = TEMPS_SPEC[-1]

    min_result = result.iloc[0][weather.MIN_COL_NAME]
    max_result = result.iloc[0][weather.MAX_COL_NAME]
    first_result = result.iloc[0][weather.FIRST_COL_NAME]
    last_result = result.iloc[0][weather.LAST_COL_NAME]

    assert (
        expected_min == min_result
        and expected_max == max_result
        and expected_first == first_result
        and expected_last == last_result
    )


# Make sure dataframe contains only floats
def test_expected_output_type():
    result = weather.transform_df(TEST_INPUT_SPEC)
    assert (
        isinstance(result.iloc[0][weather.MIN_COL_NAME], np.floating)
        and isinstance(result.iloc[0][weather.MAX_COL_NAME], np.floating)
        and isinstance(result.iloc[0][weather.FIRST_COL_NAME], np.floating)
        and isinstance(result.iloc[0][weather.LAST_COL_NAME], np.floating)
    )


# Test serialization and check that correct columns generated
def test_output_shape():
    EXPECTED_HEADER_LEN = 56

    TARGET_OUTPUT_COLS = [
        "Station Name",
        "Date",
        "Min Temp",
        "Max Temp",
        "First Temp",
        "Last Temp",
    ]

    reader = io.StringIO(TEST_INPUT_ASC.to_csv())
    writer = io.StringIO()
    weather.process_csv(reader, writer)
    print(writer.getvalue()[:EXPECTED_HEADER_LEN].split(","))
    print(TARGET_OUTPUT_COLS)
    assert writer.getvalue()[:EXPECTED_HEADER_LEN].split(",")\
        == TARGET_OUTPUT_COLS
