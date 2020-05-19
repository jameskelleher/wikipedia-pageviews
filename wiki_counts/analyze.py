
import gzip
import heapq
import os
import time

from collections import defaultdict

from .config import TMP_DIR, RESULTS_DIR, TOP_N_PAGEVIEWS, ROOT_DIR
from .wrapper import killswitch_on_exception


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
            time.sleep(5)
        else:
            # pulls the name of a downloaded gzip archive
            filename = queue.get()

            # analyzes the gzip archive
            analyze_file(filename, blacklist_set)



def analyze_file(filename, blacklist_set):
    """performs analysis of top n pageviews

    Arguments:
        filename {string} -- name of gzip file to analyze
       Set(Tuple(str, str))-- set of (domain_code, page_title) tuples to blacklist
    """
    raise ValueError('lol')
    print(f'processing {filename}')
    most_viewed_map = build_most_viewed_map(filename, blacklist_set)
    persist_results(filename, most_viewed_map)
    delete_tmp_file(filename)


def build_most_viewed_map(filename, blacklist_set):
    """get a dictionary of top n most viewed pages for each domain

    Arguments:
        filename {string} -- name of gzip file to analyze
        blacklist_set {set} -- set of blacklisted domains and pagenames

    Returns:
        Dict[str, List[Tuple(int, str)]] -- keys are domains, values are lists of top n most viewed pages
    """

    # initialize our dictionary
    most_viewed_map = defaultdict(list)

    # read the gzip file
    path = os.path.join(TMP_DIR, filename)
    with gzip.open(path, mode='rt') as f:
        for line in f:
            # split on whitespace
            # expected format: [domain_code page_title count_views total_response_size]
            # in this case, we only need the first three
            split = line.split()

            # sometimes lines can be malformed
            # e.g. too many elements after the split, or
            # third value in split not an int
            # this is especially common in earlier data dumps
            # probably not worth crashing the process bc of unexpected data,
            # so we just print and move on
            try:
                assert len(split) == 4

                # extract the data
                domain_code = split[0]
                page_title = split[1]
                count_views = int(split[2])

            # problematic lines are not added to most_viewed_map
            # we print them so that there's some record of them
            except (AssertionError, ValueError):
                print(f'malformed line in {filename}: {line.strip()}')
                continue

            # make sure that the domain and page are not blacklisted
            if (domain_code, page_title) in blacklist_set:
                continue

            # get the list of top n pageviews for the domain
            min_heap = most_viewed_map[domain_code]

            # convert count_views and page_title to immutable data type
            page_view_tuple = (count_views, page_title)

            # we use a min heap to keep track of the n most viewed pages
            # if the heap is not "full", we can freely add items to it
            if len(min_heap) < TOP_N_PAGEVIEWS:
                heapq.heappush(min_heap, page_view_tuple)

            # otherwise, if the new page has more views than the least viewed
            # page in the heap, we pop the smallest element and add the new one
            elif min_heap[0] < page_view_tuple:
                heapq.heappop(min_heap)
                heapq.heappush(min_heap, page_view_tuple)

            # add the possibly updated heap back to the most_viewed_map
            most_viewed_map[domain_code] = min_heap

    return most_viewed_map


def persist_results(filename, most_viewed_map):
    """save the date collected in most_viewed_map to a file

    Arguments:
        filename {str} -- name of the file to persist
        most_viewed_map {Dict[str, List[Tuple(int, str)]]} -- keys are domains, values are lists of top n most viewed pages per domain
    """

    # strip the ".gz" from filenames
    if filename.endswith('.gz'):
        filename = filename[:-3]

    # path to save file to
    result_path = os.path.join(RESULTS_DIR, filename)

    with open(result_path, 'w+') as f:
        # iterate through most_viewed_map
        for domain, heap in most_viewed_map.items():
            while len(heap) > 0:
                # this saves our records in increasing order
                page_view_tuple = heapq.heappop(heap)
                # write the line
                f.write(
                    f'{domain} {page_view_tuple[1]} {page_view_tuple[0]}\n')


def delete_tmp_file(filename):
    """delete the gzip archive file

    Arguments:
        filename {str} -- name of file to delete from tmp dir
    """
    tmp_path = os.path.join(TMP_DIR, filename)
    os.remove(tmp_path)


def make_blacklist_set():
    """get set of domains and page titles to not include in the analysis

    Returns:
        Set(Tuple(str, str))-- set of (domain_code, page_title) tuples to blacklist
    """
    blacklist_file = os.path.join(ROOT_DIR, 'blacklist_domains_and_pages')
    blacklist_set = set()

    with open(blacklist_file, 'r') as f:
        for line in f:
            split = line.strip().split()
            # there's one line that I'm not sure how to handle, so I skip it
            if len(split) < 2:
                print(f'malformed blacklist line: {line.strip()}')
                continue
            domain_code = split[0]
            page_title = split[1]
            blacklist_set.add((domain_code, page_title))

    return blacklist_set
