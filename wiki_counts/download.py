import os
import asyncio

from aiohttp import ClientSession, ClientResponseError

from .config import TMP_DIR


def async_download(urls, pageviews_queue, downloads_done, num_workers):
    """driver function for file download

    Arguments:
        urls {List[str]} -- list of urls to download files from
        pageviews_queue {multiprocessing.Queue} -- queue that pass downloaded file names to the pageview analyzing process
        downloads_done {Value(bool)} -- shared memory flag that communicates this process is done to pageview analyzing process
        num_workers {int} -- number of async threads to download the urls
    """
    print(f'number of files to download: {len(urls)}')
    asyncio.run(run_async_download(urls, pageviews_queue, num_workers))

    print('downloads completed')

    # set the shared boolean flag to True
    # when the pageviews_queue is empty, this causes the file analyzer to halt
    downloads_done.value = True


async def run_async_download(urls, pageviews_queue, num_workers):
    """use python async to download files

    Arguments:
        urls {List[str]} -- list of urls to download files from
        pageviews_queue {multiprocessing.Queue} -- queue that pass downloaded file names to the pageview analyzing process
        num_workers {int} -- number of async threads to download the urls
    """
    # create a queue that will store urls to download
    url_queue = asyncio.Queue()

    # load urls into the queue
    for url in urls:
        url_queue.put_nowait(url)

    # don't use unnecessary resources
    num_workers = min(len(urls), num_workers)

    # ClientSession provides async http
    async with ClientSession() as session:
        # create downloading tasks, that will read from url_queue
        tasks = [asyncio.create_task(
            file_download_worker(url_queue, pageviews_queue, session))
            for _ in range(num_workers)]

        # wait for queue to be emptied out
        await url_queue.join()

        # cancel the tasks after all urls downloaded
        for task in tasks:
            task.cancel()

        # await the cancellation of tasks
        await asyncio.gather(*tasks, return_exceptions=True)


async def file_download_worker(url_queue, pageviews_queue, session):
    """download urls pulled from the url queue, and pass their filename to the pageview analyzer

    Arguments:
        url_queue {asyncio.Queue} -- queue of urls to download gzips from
        pageviews_queue {multiproccesing.Queue} -- queue that passes names of downloaded gzips to the file analyzing process
        session {ClientSession} -- handles async http
    """
    # task runs until cancelled in run_async_download
    while True:
        url = await url_queue.get()
        
        filename = url.split('/')[-1]
        print(f'downloading {filename}')

        # try to download the file contents
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                contents = await response.read()

            # write the contents to disk
            dest = os.path.join(TMP_DIR, filename)
            with open(dest, 'wb+') as f:
                f.write(contents)

            print(f'finished downloading {filename}')

            # pass the name of the downloaded gzip to the file analyzing queue
            pageviews_queue.put(filename)

        # handle exceptions
        except ClientResponseError as e:
            # if we have a 503 error, we are attempting too many downloads
            # put the url back in the queue, sleep for a bit, try again later
            if e.status == 503:
                print('attempting too many downloads at once - sleeping for a while')
                await url_queue.put(url)
                await asyncio.sleep(10)
            # otherwise, print the error code for the url
            # this includes 404 errors, i.e. if the request is for data that
            # hasn't been dumped yet
            else:
                print(f'code {e.status}: skipping {e.request_info.url}')
        finally:
            # mark the task as done in the queue
            url_queue.task_done()
