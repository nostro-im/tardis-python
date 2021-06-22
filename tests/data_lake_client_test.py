"""
Simple test module for verify data lake API functionality

Make sure you have pytest installed (otherwise 'pip install pytest')
Run it using: pytest  --log-cli-level=INFO test_data_lake.py
"""

import pytest
from datetime import datetime, timedelta
from tardis_client.data_lake_client import DataLakeClient

expected_keys = [
    'time',
    'open',
    'high',
    'low',
    'close',
    'volumeto',
    'volumefrom',
]

max_date = datetime(2021, 1, 1)


def convert_to_timedelta(time_val):
    num = int(time_val[:-1])
    if time_val.endswith('s'):
        return timedelta(seconds=num)
    elif time_val.endswith('m'):
        return timedelta(minutes=num)
    elif time_val.endswith('h'):
        return timedelta(hours=num)
    elif time_val.endswith('d'):
        return timedelta(days=num)


@pytest.fixture(params=[10, 100, 1000, 10000])
def number_of_candles(request):
    bin_size = request.param
    return bin_size


@pytest.fixture(params=["", "1m", "1h", "24h"])
def bin_size(request):
    bin_size = request.param
    return bin_size


def test_datalake_sanity(number_of_candles):
    _test_get_multi_minutes_candles_on_provider(number_of_candles, end_time=max_date)


def test_datalake_bin_sizes(bin_size):
    _test_get_multi_minutes_candles_on_provider(100, bin_size=bin_size, end_time=max_date)


def _test_get_multi_minutes_candles_on_provider(tested_data_size, end_time=None, bin_size='',
                                                limit='', exchange_name='Binance'):
    if bin_size:
        start_time = end_time - convert_to_timedelta(bin_size) * tested_data_size
    else:
        # Server's default bin size is 24H
        start_time = end_time - timedelta(days=1) * tested_data_size

    end_time = end_time or datetime.now()

    data_lake_client = DataLakeClient(rest_api_key={'x-api-key': "j8thcf2FA854h6vVAei7K5Wfjf2pjfLj862cl3IK"})

    candles = data_lake_client.get_historical_price(
        'BTC', 'USDT',
        end_time=end_time,
        start_time=start_time,
        limit=limit,
        exchange_name=exchange_name,
        bin_size=bin_size)

    # Allow query of 100 items to return 100 or 101 items
    allowed_candles_number = [tested_data_size, tested_data_size + 1]
    assert tested_data_size in allowed_candles_number, (
        f"Wrong number of candles returned!"
        f"Expected to be in : {allowed_candles_number}"
        f"Actual: {len(candles)}")
    print(len(candles))
    first_candle = candles[0]
    for key in expected_keys:
        assert key in first_candle

    assert isinstance(first_candle['time'], int), \
        f"time should be unix, found {type(first_candle['time'])} instead"
