import traceback

from functools import wraps


def killswitch_on_exception(func):
    """if the function fails unexpectedly, throw a killswitch that terminates other processes

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
