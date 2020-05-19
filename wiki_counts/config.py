import os

# root directory of this process
ROOT_DIR = os.path.join(os.path.abspath(__file__), os.pardir, os.pardir)
ROOT_DIR = os.path.abspath(ROOT_DIR)

# directory that gzip archives will be downloaded to, then deleted after being processed
TMP_DIR = os.path.join(ROOT_DIR, 'tmp')

# directory that contains the final results
RESULTS_DIR = os.path.join(ROOT_DIR, 'results')

# earliest date the wikipedia has pageview data for
EARLIEST_DATE = '2015-05-01T01:00:00+00:00'

# wiki pageview dump root url
ROOT_URL = 'https://dumps.wikimedia.org/other/pageviews/'

# number of async threads that will be downloading from wikipedia
# too many workers will produce 503 errors and increase overhead
# based on testing, 3 is the max safe number
DEFAULT_NUM_DOWNLOADERS = 3

# number of file processors that will process the downloaded gzips
DEFAULT_NUM_FILE_PROCESSORS = 1

# capture the top {TOP_N_PAGEVIEWS} most viewed pages for each domain
TOP_N_PAGEVIEWS = 25

# make the dirs, if they don't exist already
if not os.path.exists(TMP_DIR):
    os.makedirs(TMP_DIR)

if not os.path.exists('results'):
    os.makedirs(RESULTS_DIR)
