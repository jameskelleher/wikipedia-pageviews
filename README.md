# Wikipedia Pageviews

## Overview

Get the top 10 most viewed pages for every Wikipedia domain at a given datetime

## Installation and Running

1. If `conda` is not already installed, download it from [Anaconda](https://www.anaconda.com/) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) (Miniconda is a minimal install for `conda` , so it is recommended)

2. `cd` to this package's root directory

3. Set up the environment: `conda env create --file env.yaml` 

4. Activate the environment: `conda activate wiki`

5. Run the program with `run_wiki_counts.py`

    a. To run for 24 hours ago: `python run_wiki_counts.py`

    b. To run for an exact datetime: `python run_wiki_counts.py "2020-01-01 8:00"`

    c. To run for every hour between a range of dates: `python run_wiki_counts.py "2020-01-01 8:00" "2020-01-02 20:00"`

6. Result summary files will be written to a created `results` directory

7. To run unit tests, run `pytest`. The unit tests on asynchronous code generate a lot of warnings, so `pytest --disable-warnings` is recommended

## Discussion

This program records the top 25 most viewed Wikipedia pages for each domain for a given datetime. It can also be given a start and end time, and will generate a results file for every hour within that range.

On first glance, it was clear to me that this problem could be solved using entirely sequential code - for every hour in the range, download the file, process it, move on to the next. However, it was clear to me that this was an easy problem to parallelize. As there is no dependency between the data, the tasks can easily be isolated from each other, and set up to be run concurrently. To speed up processing large ranges of data, and to demonstrate my capacity to work within a distributed context, I used two types of concurrency: multiprocessing and asynchronous I/O.

The program uses two parallel processes to produce its results, a Downloader and a file Analyzer. The Downloader takes in a list of URLs to download, and utilizes `asyncio` and `aiohttp` to asynchronously download files. When a file is downloaded to the `tmp/` directory, the Downloader sends its path to the Analyzer, which reads the file and extracts the top 25 pages (for each domain). A shared-memory Queue is used to communicate the paths of downloaded files from the Downloader to the Analyzer. While the Downloader is active, the Analyzer will await files to read from the Queue, processing any that arrive and writing the results to `results/` (which is created if it doesn't already exist). When the Downloader is finished, it sends a message to the Analyzer, which processes all filepaths remaining in the queue and then itself terminates. These two processes act in parallel via Python's `multiprocessing` library. Though using parallel processes across cores, the model itself is simple and effective - the Downloader publishes to a Queue, and the Analyzer consumes from the Queue.

Downloading the files is mostly I/O-bound, so it provides a good use case for Python's `asyncio` and `aiohttp` libraries. You can get a significant speedboost within a single core by using async (about 25% faster on my computer/network). On the other hand, file analysis is mostly CPU-bound. By putting the Analyzer on a different core, we can process and download files concurrently.

The bottleneck here is downloading the data dumps. Based on my testing, the Wikimedia archive can only handle three connections at the same time, otherwise it starts throwing 503 errors. Therefore, The Downloader defaults to using three asynchronous download tasks. On my network, a single Analyzer was able to keep up with the Downloader. On a different network, this may not be the case, but you can configure the number of download tasks and analysis processes by setting `DEFAULT_NUM_DOWNLOADERS` and `DEFAULT_NUM_FILE_PROCESSORS` respectively in `config.py`.

Instead of passing archive data directly to the Analyzer, the Downloader saves the files to a temporary directory, which the Analyzer will then read from. While I considered passing archive data directly to the Analyzer, I decided to persist them temporarily instead. This is safer, as it makes memory leakage less likely should something go wrong with the Analyzer. Also, the Analyzer is able to read from archives already in the temporary folder. If the pipeline goes down with some archives already downloaded to the temporary folder, it does not have to redownload them, it will just load them back into the queue.

The Analyzer uses a min heap to collect the top 25 most viewed pages for each domain. There is one min heap per domain, I use a dictionary to map each domain to its min heap. First, I create a domain/page blacklist by reading every line of `blacklist_domains_and_pages` into a set. For every line in the gzip, the Analyzer first checks to make sure its domain code and page title are not in the blacklist. If this passes, the Analyzer then compares the line against its domain's min heap. If the min heap has less than 25 items in it, we'll add the page name and its view counts to it. Otherwise, we check the smallest element in the min heap. If this heap element has less page views than than the current line element, it's not one of the top 25 most viewed, so we pop it from the heap and add the data for the current line element instead. Otherwise, we continue iterating through the data dump. The size of the heap can be adjusted by changing the value of `TOP_N_PAGEVIEWS` in `config.py`.

One challenge to this approach is ensuring that if one process fails the others terminate as well. Therefore, I coded a kill switch (really a boolean flag stored in shared space) that halts all other processes should it be flipped. If a process raises an unexpected exception, the switch is thrown.

Let's say that a user enters dates that create invalid URLs - for example, they enter in a date in the future. I decided to take a friendly approach, and simply skip cases in which attempting a download results in a 404. In a production setting, it may be best to raise exceptions instead.

One task that does not use parallelism is the Date Parser. This takes a start date and and end date, and converts it to the range of URLs that will be downloaded (or converts a single date to a download URL). There are about 47520 files that could be downloaded, a List that contains all of their URLs would only be about 380 KB big. Even in its worst case, this is not a big enough task to warrant use of parallelism.

The Downloader is coded in `download.py`, the Analyzer in `analyze.py`, and the Date Parser in `parse_dates.py`. Global variables are kept in `config.py`, and a couple of utility functions in `utils.py`. The processes are kicked off and orchestrated by `run_wiki_counts.py`.

Since the size of the heap is bounded, the algorithm processes each file in linear time. Heap pushes and pops are O(log *n*), and *n* <= 25. In the worst case, where we pop and push an element onto the heap, the runtime is O(2 * log(25)) = O(1). If we let *p* represent the average number of pageview lines in a file, and *n* the number of files we'd like to process, the runtime is O(*n* \* *p*).

## Prompts

### What additional things would you want to operate this application in a production setting?

In a production setting, it may be possible to open up more connections to the archives. In that case, I could further parallelize this. For example, I could distribute partitions of the URL set to multiple machines, and on those machines run this program on each of those partitions. I would also add more complex exception handling. Currently, if a file download attempt results in a 404, the program simply catches the error and moves on to the next URL. In a production setting it could be more useful to throw an exception instead, and have a scheduler determine how/if the process should be retried.

### What might change about your solution if this application needed to run automatically for each hour of the day?

If this program tries to download a file from a URL that doesn't exist, it simply prints that it received a 404, and moves on. Let's say I try to download a data dump that hasn't been created yet (you can see in the Wikimedia archives that there is some delay, and it is not always regular). I would instead like this program to fail on the 404, so an orchestrator could reschedule it to try again later.

### How would you test this application?

- I've already included some unit tests. Unit testing the Downloader is tricky, as it mostly uses asynchronous I/O, so integration tests that ensure it can download files correctly would be good. Integration tests would also be useful for ensuring that the Analyzer properly reads from gzip archives and blacklists rows from the blacklist file.
- I would write tests to ensure that a failure in one process terminates all other processes (I tested this manually by raising Exceptions in my code).
- I would set up an end-to-end test that can test on one hour's, or a small range's worth of data dumps, and makes sure the output is expected.

### How you’d improve on this application design?

From the command line, you can only enter in dates. Everything else is hard-coded (mostly in `config.py`). It would be nice to have more controls over parameters from the command line. Better error reporting would be good too. Right now, issues that pop up (such as malformed lines in the input files) are simply printed to the console. It would be good to use a more powerful logger than can log these issues to a file, so they can be easily kept track of and reviewed later. 
