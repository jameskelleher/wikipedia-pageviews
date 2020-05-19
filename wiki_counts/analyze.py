
import gzip
import heapq
import os
import glob
import time

from collections import defaultdict

from .config import TMP_DIR, RESULTS_DIR, TOP_N_PAGEVIEWS, ROOT_DIR
from .utils import killswitch_on_exception, filename_from_path


@killswitch_on_exception
def analyze_from_queue(queue, downloads_done, process_killswitch):
    """driver function that calls analyze_file on filenames read from queue

    Arguments:
        queue {multiprocessing.Queue} -- queue that provides names of downloaded files
        downloads_done {multiprocessing.Value(bool)} -- flag that indicates when downloads are done
        process_killswitch {multiprocessing.Value(bool)} -- flag that indicates to kill this process
                                                            because of an error in another process
    """

    # get the list of domains and pages to not include in the analysis
    blacklist_set = make_blacklist_set()

    # as long as downloads are not done or the queue is not empty,
    # this process runs
    while not downloads_done.value or not queue.empty():
        if process_killswitch.value:
            print('process killed')
            return

        if queue.empty():
            print('no files yet!')
            # sleep so resources aren't hogged
            time.sleep(5)
        else:
            # pulls the name of a downloaded gzip archive
            file_abspath = queue.get()

            # analyzes the gzip archive
            analyze_file(file_abspath, blacklist_set)


def analyze_file(file_abspath, blacklist_set):
    """performs analysis of top n pageviews

    Arguments:
        file_abspath {string} -- path to gzip file to analyze
        blacklist_set {Set(Tuple(str, str))} -- set of blacklisted (domain_code, page_names) tuples
    """
    filename = filename_from_path(file_abspath)

    print(f'processing {filename}')
    most_viewed_map = build_most_viewed_map(file_abspath, blacklist_set)
    persist_results(file_abspath, most_viewed_map)
    os.remove(file_abspath)
    print(f'finished processing {filename}')


def build_most_viewed_map(file_abspath, blacklist_set):
    """get a dictionary of top n most viewed pages for each domain

    Arguments:
        file_abspath {string} -- path to gzip file to analyze
        blacklist_set {Set(Tuple(str, str))} -- set of blacklisted (domain_code, page_names) tuples

    Returns:
        Dict[str, List[Tuple(int, str)]] -- keys are domains, values are lists of top n most viewed pages
    """

    # initialize our dictionary
    most_viewed_map = defaultdict(list)

    # read the gzip file
    with gzip.open(file_abspath, mode='rt') as f:
        for line in f:

            # sometimes lines can be malformed
            # e.g. too many elements after the split, or
            # third value in split not an int
            # this is especially common in earlier data dumps
            # probably not worth crashing the process bc of unexpected data,
            # so we just print and move on
            try:
                domain_code, page_title, count_views = get_line_info(line)

            # problematic lines are not added to most_viewed_map
            # we print them so that there's some record of them
            except (AssertionError, ValueError):
                print(f'malformed line in {file_abspath}: {line.strip()}')
                continue

            # make sure that the domain and page are not blacklisted
            if in_blacklist_set(domain_code, page_title, blacklist_set):
                continue

            add_to_heap_map(most_viewed_map, domain_code, page_title, count_views)

    return most_viewed_map


def get_line_info(line):
    """extract the necessary info from a line of the gzip file

    Arguments:
        line {str} -- line of text from one of the gzip archives

    Returns:
        Tuple(str, str, int) -- domain_code, page_title, count_views
    """
    # split on whitespace
    # expected format: [domain_code page_title count_views total_response_size]
    # in this case, we only need the first three, but still check to see
    # if the line is well-formed
    split = line.split()

    assert len(split) == 4

    # extract the data
    domain_code = split[0]
    page_title = split[1]
    count_views = int(split[2])

    return domain_code, page_title, count_views


def in_blacklist_set(domain_code, page_title, blacklist_set):
    """check if a given domain_code and page_title is in the set of blacklisted domains/pages

    Arguments:
        domain_code {str} -- domain code
        page_title {str} -- page title
        blacklist_set {Set(Tuple(str, str))} -- set of blacklisted (domain_code, page_names) tuples

    Returns:
        bool -- True if in blacklist, False otherwise
    """
    return (domain_code, page_title) in blacklist_set


def add_to_heap_map(most_viewed_map, domain_code, page_title, count_views, top_n_pageviews=TOP_N_PAGEVIEWS):
    """add page info to the most viewed map, if one of the most viewed

    Arguments:
        most_viewed_map Dict[str, List[Tuple(int, str)]] -- keys are domains, values are lists of top n most viewed pages
        domain_code {str} -- domain code
        page_title {str} -- page title
        count_views {int} -- number of views a page has

    Keyword Arguments:
        top_n_pageviews {int} -- only add pages to the map if they are in the top n of pageviews (default: {config.TOP_N_PAGEVIEWS})
    """
    # get the list of top n pageviews for the domain
    min_heap = most_viewed_map[domain_code]

    # convert count_views and page_title to immutable data type
    page_view_tuple = (count_views, page_title)

    # we use a min heap to keep track of the n most viewed pages
    # if the heap is not "full", we can freely add items to it
    if len(min_heap) < top_n_pageviews:
        heapq.heappush(min_heap, page_view_tuple)

    # otherwise, if the new page has more views than the least viewed
    # page in the heap, we pop the smallest element and add the new one
    elif min_heap[0] < page_view_tuple:
        heapq.heappop(min_heap)
        heapq.heappush(min_heap, page_view_tuple)

    # add the possibly updated heap back to the most_viewed_map
    most_viewed_map[domain_code] = min_heap


def persist_results(abspath, most_viewed_map):
    """save the date collected in most_viewed_map to a file

    Arguments:
        abspath {str} -- name of the file to persist
        most_viewed_map {Dict[str, List[Tuple(int, str)]]} -- keys are domains, values are lists of top n most viewed pages per domain
    """

    filename = filename_from_path(abspath, remove_gz=True)

    # path to save file to
    result_path = os.path.join(RESULTS_DIR, filename)

    with open(result_path, 'w+') as f:
        # iterate through most_viewed_map
        # python dicts remember the order of insertion,
        # so as long as the archives are alphabetized by domain,
        # the output will be alphabetized by domain as well
        for domain, heap in most_viewed_map.items():
            while len(heap) > 0:
                # this saves our records in increasing order
                page_view_tuple = heapq.heappop(heap)
                # write the line
                f.write(
                    f'{domain} {page_view_tuple[1]} {page_view_tuple[0]}\n')


def make_blacklist_set():
    """get set of domains and page titles to not include in the analysis

    Returns:
       Set(Tuple(str, str)) -- set of blacklisted (domain_code, page_names) tuples
    """
    blacklist_file = os.path.join(ROOT_DIR, 'blacklist_domains_and_pages')
    blacklist_set = set()

    with open(blacklist_file, 'r') as f:
        for line in f:
            try:
                add_to_blacklist(line, blacklist_set)
            except AssertionError:
                # there's one line that I'm not sure how to handle, so I skip it
                print(f'malformed blacklist line: {line.strip()}')

    return blacklist_set


def add_to_blacklist(line, blacklist_set):
    """add data from a line of the blacklist file to the blacklist_set

    Arguments:
        line {str} -- line from the blacklist file
        blacklist_set {Set(Tuple(str, str))} -- set of blacklisted (domain_code, page_names) tuples
    """
    split = line.strip().split()
    
    # check input data is good
    assert len(split) == 2
        
    domain_code = split[0]
    page_title = split[1]

    # cache domain_code and page_title as a tuple
    blacklist_set.add((domain_code, page_title))
