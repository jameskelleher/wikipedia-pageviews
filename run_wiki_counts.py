from wiki_counts.config import DEFAULT_NUM_FILE_PROCESSORS, EARLIEST_DATE, DEFAULT_NUM_DOWNLOADERS
from wiki_counts.parse_dates import parse_dates
from wiki_counts.download import async_download
from wiki_counts.analyze import analyze_from_queue

from multiprocessing import Process, Manager
from multiprocessing.sharedctypes import Value

def run_multiprocess(start_date=None, end_date=None):
    """main function, orchestrate file downloader process and file analysis process

    Keyword Arguments:
        start_date {str, None} -- start date as a string, if None it is set to utcnow minus 24 hours
        end_date {str, None} -- end date as a string, if None function returns only one URL for the start date (default: {None})
    """

    # get urls to download
    urls = parse_dates(start_date, end_date)

    with Manager() as manager:

        # this queue will pass names of downloaded files from the download process
        # to the file analysis process
        queue = manager.Queue()

        # the downloader sets this value to True so that the file analyzer
        # knows that there will be no more filenames added to the Queue
        downloads_done = Value('b', False)

        # set up the file download process
        download_process = Process(
            target=async_download,
            args=(urls, queue, downloads_done, DEFAULT_NUM_DOWNLOADERS))

        # set up the file analysis process
        fileread_processes = [
            Process(target=analyze_from_queue, args=(queue, downloads_done))
            for _ in range(DEFAULT_NUM_FILE_PROCESSORS)]

        # start the processes
        download_process.start()
        for fp in fileread_processes:
            fp.start()

        # wait for the processes to finish
        download_process.join()
        for fp in fileread_processes:
            fp.join()



if __name__ == '__main__':
    import sys

    # if no args, just run for last updated file
    if len(sys.argv) <= 1:
        run_multiprocess()
    elif len(sys.argv) == 2:
        run_multiprocess(sys.argv[1])
    else:
        run_multiprocess(sys.argv[1], sys.argv[2])
