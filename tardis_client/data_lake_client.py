import asyncio
import gzip
import logging
import json as default_json
import os
import tempfile
import shutil

import requests

import vertica_python

from time import time
from typing import List, AsyncIterable
from collections import namedtuple
from datetime import datetime, timedelta

from tardis_client.handy import get_slice_cache_path
from tardis_client.channel import Channel
from tardis_client.data_downloader import fetch_data_to_replay
from tardis_client.reconstructors import get_market_reconstructor
from tardis_client.reconstructors.market_reconstructor import MarketResponse

Response = namedtuple("Response", ["local_timestamp", "message"])

DATE_MESSAGE_SPLIT_INDEX = 28
DEFAULT_CACHE_DIR = os.path.join(tempfile.gettempdir(), ".tardis-cache")


class DataLakeClient:
    def __init__(self, endpoint="https://api.tardis.dev", cache_dir=DEFAULT_CACHE_DIR, api_key="", http_timeout=60, http_proxy=None,
                 rest_api_url = "https://iijlm9hgoj.execute-api.us-east-2.amazonaws.com/First_Deploy/getmultycandles",
                 rest_api_key = "",
                 vertica_connection_info = {}):
        self.logger = logging.getLogger(__name__)
        self.endpoint = endpoint
        self.cache_dir = cache_dir
        self.api_key = api_key
        self.http_timeout = http_timeout
        self.http_proxy = http_proxy
        self.rest_api_url = rest_api_url
        self.rest_api_key = rest_api_key
        self.vertica_connection_info = vertica_connection_info

        self.logger.debug("initialized with: %s", {"endpoint": endpoint, "cache_dir": cache_dir, "api_key": api_key})

    async def replay(
        self,
        exchange: str,
        from_date: str,
        to_date: str,
        filters: List[Channel] = [],
        decode_response=True,
        json=default_json,
    ):
        # start with validation of provided args
        self._validate_payload(exchange, from_date, to_date, filters)

        from_date = datetime.fromisoformat(from_date)
        to_date = datetime.fromisoformat(to_date)
        current_slice_date = from_date
        start_time = time()

        # sort filters to improve local disk cache ratio - same filters same has
        if filters is not None:
            filters.sort(key=lambda filter: filter.name)

        self.logger.debug(
            "replay for '%s' exchange started from: %s, to: %s, filters: %s",
            exchange,
            from_date.isoformat(),
            to_date.isoformat(),
            filters,
        )

        # start fetch_data_to_replay task
        fetch_data_task = asyncio.create_task(
            fetch_data_to_replay(exchange, from_date, to_date, filters, self.endpoint, self.cache_dir, self.api_key, self.http_timeout, self.http_proxy)
        )

        # iterate over every minute in <=from_date,to_date> date range
        # get cached 'slice' (single minute) files, decompress
        # and return each line as Response - using yield
        while current_slice_date < to_date:
            current_slice_path = None
            while current_slice_path is None:
                # this allows other tasks to run (suspends current task)
                await asyncio.sleep(0)
                path_to_check = get_slice_cache_path(self.cache_dir, exchange, current_slice_date, filters)

                self.logger.debug("getting slice: %s", path_to_check)

                # always check if data fetching task has finished prematurely
                #  with exception (network issue, auth issue etc) and if it did raise such exception
                # and stop the loop
                if fetch_data_task.done() and fetch_data_task.exception():
                    raise fetch_data_task.exception()

                # if data for requested date already exists we can proceed further
                if os.path.isfile(path_to_check):
                    current_slice_path = path_to_check
                # otherwise if data for requested date is not ready yet (cached) wait 100ms and check again
                else:
                    self.logger.debug("waiting for slice: %s", path_to_check)
                    await asyncio.sleep(0.1)

            messages_count = 0

            # open data file as binary and read line by line
            with gzip.open(current_slice_path, "rb") as file:
                for line in file:
                    # each line ends with \n byte, so in order to exclude empty lines (\n only) we must check if line length is < 1
                    if len(line) <= 1:
                        continue
                    messages_count = messages_count + 1

                    # local timestamps provided by the API always have 28 characters
                    # eg 2019-08-01T08:52:00.0324272Z
                    # let's split each line to date and message part and yield them as Response
                    if decode_response:
                        # if returning decoded response we need to convert:
                        # timestamp returned by the API to Python datetime
                        # message returned by the API to JSON object

                        # since python datetime has microsecond precision and provided timestamp has 100ns precision
                        # we ignore last two characters of timestmap provided by the API (last character is Z)
                        # so we can decode it as python datetime
                        timestamp = datetime.strptime(
                            line[0 : DATE_MESSAGE_SPLIT_INDEX - 2].decode("utf-8"), "%Y-%m-%dT%H:%M:%S.%f"
                        )

                        yield Response(timestamp, json.loads(line[DATE_MESSAGE_SPLIT_INDEX + 1 :]))
                    else:
                        yield Response(line[0:DATE_MESSAGE_SPLIT_INDEX], line[DATE_MESSAGE_SPLIT_INDEX + 1 :])

            self.logger.debug("processed slice: %s, messages count: %i", current_slice_path, messages_count)

            current_slice_date = current_slice_date + timedelta(seconds=60)

        end_time = time()

        # always await on fetch_data_task as theoreticaly it could still be pending
        await fetch_data_task

        self.logger.debug(
            "replay for '%s' exchange finished from: %s, to: %s, filters: %s, total time: %s seconds",
            exchange,
            from_date.isoformat(),
            to_date.isoformat(),
            filters,
            end_time - start_time,
        )

    async def reconstruct_market(
        self, exchange: str, from_date: str, to_date: str, symbols: List[str]
    ) -> AsyncIterable[MarketResponse]:
        market_reconstructor = get_market_reconstructor(exchange, symbols)
        filters = market_reconstructor.get_filters()

        self._validate_payload(exchange, from_date, to_date, filters)

        async for local_timestamp, message in self.replay(exchange, from_date, to_date, filters):
            market_response = market_reconstructor.reconstruct(local_timestamp, message)
            if market_response is not None:
                yield market_response

    def clear_cache(self):
        shutil.rmtree(self.cache_dir)

    def _validate_payload(self, exchange, from_date, to_date, filters):
        if from_date is None or self._try_parse_as_iso_date(from_date) is False:
            raise ValueError(
                f"""Invalid 'from_date' argument: {from_date}. Please provide valid ISO date string.
                https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat"""
            )

        if to_date is None or self._try_parse_as_iso_date(to_date) is False:
            raise ValueError(
                f"""Invalid 'to_date' argument: {to_date}. Please provide valid ISO date string.
                https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat"""
            )

        if datetime.fromisoformat(from_date) >= datetime.fromisoformat(to_date):
            raise ValueError(
                f""" 'from_date' and 'to_date' arguments combination.
                Please provide 'to_date' date string that is later than 'from_date'."""
            )

        if filters is None:
            return

        if isinstance(filters, list) is False:
            raise ValueError("Invalid 'filters' argument. Please provide valid filters Channel list")

        if len(filters) > 0:
            for filter in filters:

                if filter.symbols is None:
                    continue

                if isinstance(filter.symbols, list) is False or any(
                    isinstance(symbol, str) == False for symbol in filter.symbols
                ):
                    raise ValueError(
                        f"Invalid 'symbols[]' argument: {filter.symbols}. Please provide list of symbol strings."
                    )

    def _try_parse_as_iso_date(self, date_string):
        try:
            datetime.fromisoformat(date_string)
            return True
        except ValueError:
            return False

    def get_historical_price(self, base_currency, counter_currency, end_time='', start_time='',
                             limit='', exchange_name='Binance', bin_size='1m'):
        assert (end_time and start_time) or ((end_time or start_time) and limit)

        payload = {
            'base_currency': base_currency,
            'counter_currency': counter_currency,
            'end_time': end_time,
            'start_time': start_time,
            'period': limit,
            'provider_name': exchange_name,
            'bin_size': bin_size
        }
        print(payload)
        response = requests.get(url=self.rest_api_url, params=payload, headers=self.rest_api_key)
        response.raise_for_status()
        result = response.json()
        if result == 'No results found':
            raise Exception("404: No results found")
        return result

    def get_data_from_vertica(self, base_currency, counter_currency, end_time='', start_time='',
                             provider_name='', exchange_name='Binance', bin_size='1m'):

        assert (end_time and start_time)

        class DateTimeEncoder(default_json.JSONEncoder):
            def default(self, o):
                if isinstance(o, datetime):
                    return o.isoformat()
                return default_json.JSONEncoder.default(self, o)

        def FormatChanger(date):
            return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        # Default Params
        error = None

        table_name = 'cryptocompare_ohlcv_' + bin_size

        try:
            connection = vertica_python.connect(**self.vertica_connection_info)
            cur = connection.cursor()
            output = []
            try:
                cur.execute("""SELECT time AS date, close, high, low, open, volumefrom, volumeto FROM {}
                         WHERE fsym = :base_currency AND tsym = :counter_currency
                             AND
                         time BETWEEN DATE(:start_time) AND DATE(:end_time)
                             AND
                         exchange = :exchange_name""".format(table_name),
                            {'base_currency' : base_currency,
                             'counter_currency' : counter_currency,
                             'start_time' : start_time,
                             'end_time' : end_time,
                             'exchange_name' : exchange_name})

                columns = cur.description
                result = [{columns[index][0]: column for index, column in enumerate(value)} for value in cur.fetchall()]
                for row in result:
                    finall_result = {"time": int(row["date"].timestamp())}
                    row["date"] = default_json.dumps(row["date"], cls=DateTimeEncoder).replace('"', '').replace('T', ' ')
                    finall_result.update(row)
                    output.append(finall_result)

            except vertica_python.errors.QueryError as err:
                error = "Error: {}".format(err.message)
                # print("Error is: ")
                # print(error)

        finally:
            connection.close()

        if error is not None:
            return {
                'body': default_json.dumps(error)
            }
        else:
            return {
                'statusCode': 200,
                'body': default_json.dumps(output)
            }

