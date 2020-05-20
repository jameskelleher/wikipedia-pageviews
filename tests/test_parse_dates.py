from wiki_counts.parse_dates import (
    str_to_timestamp, date_to_url, parse_start_and_end)

from wiki_counts import parse_dates as parse_dates_module

import pandas as pd
import pytest


@pytest.fixture
def timestamp():
    time_str = '2020-05-19T00:00:00+00:00'
    return pd.Timestamp(time_str)


@pytest.fixture
def mock_str_to_timestamp(monkeypatch):
    def mock(time_str):
        return pd.Timestamp(time_str)

    monkeypatch.setattr(parse_dates_module, 'str_to_timestamp', mock)


def test_str_to_timestamp_well_formed(timestamp):
    as_str = timestamp.isoformat()
    assert str_to_timestamp(as_str) == timestamp


def test_str_to_timestamp_rounds_up(timestamp):
    offset = timestamp - pd.Timedelta(minutes=45)
    as_str = offset.isoformat()
    assert str_to_timestamp(as_str) == timestamp


def test_str_to_timestamp_tz_convert(timestamp):
    us_central = timestamp.tz_convert('US/Central')
    as_str = us_central.isoformat()
    assert str_to_timestamp(as_str) == timestamp


def test_str_to_timestamp_tz_localize(timestamp):
    no_tz = timestamp.tz_localize(None)
    as_str = no_tz.isoformat()
    assert str_to_timestamp(as_str) == timestamp


def test_str_to_timestamp_ok_input(timestamp):
    as_str = '05/19/2020'
    assert str_to_timestamp(as_str) == timestamp


def test_str_to_timestamp_raises_error_on_lousy_input():
    bad_input = "i'm some pretty bad input"
    with pytest.raises(ValueError):
        str_to_timestamp(bad_input)


def test_date_to_url_makes_correct_url(timestamp):
    expected = 'https://dumps.wikimedia.org/other/pageviews/2020/2020-05/pageviews-20200519-000000.gz'
    assert date_to_url(timestamp, set()) == expected


def test_date_to_url_returns_none_if_already_downloaded(timestamp):
    filename_for_ts = 'pageviews-20200519-000000'
    already_downloaded_set = set()
    already_downloaded_set.add(filename_for_ts)

    assert date_to_url(timestamp, already_downloaded_set) == None


# the following have to mock the function "str_to_timestamp"

def test_parse_start_and_end_no_arguments(monkeypatch, timestamp, mock_str_to_timestamp):

    def mock_utcnow():
        return timestamp

    monkeypatch.setattr(pd.Timestamp, 'utcnow', mock_utcnow)

    yesterday = timestamp - pd.Timedelta(days=1)

    start, end = parse_start_and_end(None, None)
    assert (start, end) == (yesterday, yesterday)


def test_parse_start_and_end_one_argument(timestamp, mock_str_to_timestamp):
    as_str = timestamp.isoformat()
    start, end = parse_start_and_end(as_str, None)
    assert (start, end) == (timestamp, timestamp)


def test_parse_start_and_end_two_arguments(timestamp, mock_str_to_timestamp):
    start_str = timestamp.isoformat()

    plus_day = timestamp + pd.Timedelta(days=1)
    end_str = plus_day.isoformat()

    start, end = parse_start_and_end(start_str, end_str)
    assert (start, end) == (timestamp, plus_day)


def test_parse_start_end_end_before_earliest(timestamp, mock_str_to_timestamp):
    minus_two_days = timestamp - pd.Timedelta(days=2)
    minus_one_day = timestamp - pd.Timedelta(days=1)

    start_str = minus_two_days.isoformat()
    end_str = minus_one_day.isoformat()
    earliest_str = timestamp.isoformat()

    start, end = parse_start_and_end(start_str, end_str, earliest_str)
    assert (start, end) == (timestamp, timestamp)


def test_parse_start_and_end_only_start_before_earliest(timestamp, mock_str_to_timestamp):
    minus_one_day = timestamp - pd.Timedelta(days=1)
    plus_one_day = timestamp + pd.Timedelta(days=1)

    start_str = minus_one_day.isoformat()
    end_str = plus_one_day.isoformat()
    earliest_str = timestamp.isoformat()

    start, end = parse_start_and_end(start_str, end_str, earliest_str)
    assert (start, end) == (timestamp, plus_one_day)


def test_parse_start_and_end_raises_when_start_after_end(timestamp, mock_str_to_timestamp):
    minus_one_day = timestamp - pd.Timedelta(days=1)

    start_str = timestamp.isoformat()
    end_str = minus_one_day.isoformat()

    with pytest.raises(ValueError):
        parse_start_and_end(start_str, end_str)
