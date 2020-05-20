import pandas as pd
import glob
import os

from .config import ROOT_URL, RESULTS_DIR, TMP_DIR, EARLIEST_DATE
from .utils import filename_from_path


def parse_dates(start, end):
    """From a start and end date, return a list of urls to download

    Arguments:
        start {string, None} -- start date as a string, if None it is set to utcnow minus 24 hours
        end {string, None} -- end date as a string, if None function returns only one URL for the start date

    Returns:
        List[str] -- list of urls to download
    """
    # handle the inputted start and end dates
    start, end = parse_start_and_end(start, end)

    # a timestamp for every hour in between start and end, inclusive
    to_download = pd.date_range(start=start, end=end, freq='H')

    # load the names of files that we already have
    exclusion_set = get_exclusion_set()

    # convert dates to urls, while filtering out dates we already have info for
    date_map_and_filter = filter(
        lambda x: x, map(
            lambda date: date_to_url(date, exclusion_set),
            to_download))

    to_download = list(date_map_and_filter)

    return to_download


def parse_start_and_end(start, end, earliest_date=EARLIEST_DATE):
    """parse the start and end dates, and handle any discrepencies

    Arguments:
        start {string, None} -- start date as a string, if None it is set to utcnow minus 24 hours
        end {string, None} -- end date as a string, if None function returns only one URL for the start date

    Keyword Arguments:
        earliest_date {str} -- earliest datetime with data available, as a string (default: {EARLIEST_DATE})

    Raises:
        ValueError: end date before start date, or date strings not parsable by pd.Timestamp

    Returns:
        Tuple(Timestamp, Timestamp) -- start date and end date as Timestamp objects
    """
    # if start is None, set to utcnow minus 24 hours, rounded up to nearest hour
    start = str_to_timestamp(start) if start else \
        pd.Timestamp.utcnow().ceil('H') - pd.Timedelta(hours=24)

    # if end is None, we only get records for one datetime
    end = str_to_timestamp(end) if end else start

    # make sure we don't try to download records before earliest available date
    earliest_start = pd.Timestamp(earliest_date)
    start = max(start, earliest_start)
    end = max(end, earliest_start)

    if end < start:
        raise ValueError('end date cannot be before start date')

    return start, end


def str_to_timestamp(time_str):
    """convert a datetime, represented as a string, to a pandas Timestamp

    Arguments:
        time_str {string} -- datetime represented as a string

    Raises:
        ValueError: time string not parsable by pd.Timestamp

    Returns:
        Timestamp -- pandas Timestamp representation of time_str
    """
    # check for valid time string
    try:
        ts = pd.Timestamp(time_str)
    except ValueError:
        raise ValueError(f'could not convert "{time_str}" to Timestamp')

    # make sure timestamp is in utc
    if ts.tzinfo:
        ts = ts.tz_convert('utc')
    else:
        ts = ts.tz_localize('utc')

    # if timestamp is not on the hour, raise it to the next hour
    # we ceil instead of floor because the file's timestamp refers to the end of
    # the aggregation period
    ts = ts.ceil('H')

    return ts


def date_to_url(datetime, already_downloaded):
    """convert a datetime to its corresponding wiki dump url

    Arguments:
        datetime {Timestamp} -- Timestamp whose corresponding wiki pageviews dump will be downloaded
        already_downloaded {Set(str)} -- set of filenames for datetimes whose pageviews have already been downloaded and processed

    Returns:
        string, None -- url to download, or None if data already downloaded and processed
    """
    year = datetime.strftime('%Y')
    year_month = datetime.strftime('%Y-%m')
    pageviews = datetime.strftime('pageviews-%Y%m%d-%H0000.gz')

    # check to see if the file has already been downloaded and processed
    # we do "pageviews[:-3]" because the string ends in ".gz"
    if pageviews[:-3] in already_downloaded:
        print(f'already downloaded {pageviews[:-3]}')
        return None

    # url looks like:
    # https://dumps.wikimedia.org/other/pageviews/2020/2020-05/pageviews-20200501-100000.gz
    url = os.path.join(ROOT_URL, year, year_month, pageviews)
    return url


def get_exclusion_set():
    """get a set of files we don't need to download, because they are processed or are ready to be processed

    Returns:
        Set(str) -- set of filenames for datetimes whose pageviews or archives have already been downloaded
    """

    # for every results file that we have in results, add the filename
    # to the exclusion set
    results_path = os.path.join(RESULTS_DIR, '*')
    already_processed = glob.glob(results_path)
    downloaded_filenames = set(
        [filename_from_path(p) for p in already_processed])

    # for every unprocessed gzip we have in tmp, add to filename
    # to the exclusion set
    tmp_path = os.path.join(TMP_DIR, '*.gz')
    ready_for_processing = glob.glob(tmp_path)
    downloaded_filenames = downloaded_filenames.union(set(
        [filename_from_path(p, remove_gz=True) for p in ready_for_processing]))

    return downloaded_filenames
