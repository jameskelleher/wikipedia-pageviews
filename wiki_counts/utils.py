import traceback
import os

from functools import wraps


def killswitch_on_exception(func):
    """decorator - if the function fails unexpectedly, throw a killswitch that terminates other processes

    Arguments:
        func {function} -- function that should throw a killswitch, should it fail

    Returns:
        function -- decorated function
    """

    # functools.wraps is needed so the function will be picklable
    # for multiprocessing
    @wraps(func)
    def wrapper(*args):

        # requires that the killswitch be the last argument in the function
        process_killswitch = args[-1]

        try:
            func(*args)

        # should the function fail, print the traceback
        # and communicate to all other processes that they should quit
        except:
            traceback.print_exc()
            print('killing all processes')
            process_killswitch.value = True

    return wrapper


def filename_from_path(file_path, remove_gz=False):
    """extract the filename from its path, works for urls too

    Arguments:
        file_path {str} -- path to file

    Keyword Arguments:
        remove_gz {bool} -- if True, strip ".gz" from filename (default: {False})

    Returns:
        str -- name of file
    """
    filename = os.path.split(file_path)[-1]
    if remove_gz and filename.endswith('.gz'):
        filename = filename[:-3]

    return filename
