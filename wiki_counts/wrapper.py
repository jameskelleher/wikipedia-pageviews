import traceback

from functools import wraps

def killswitch_on_exception(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        process_killswitch = args[-1]
        try:
            func(*args)
        except:
            traceback.print_exc()
            print('killing all processes')
            process_killswitch.value = True

    return wrapper


# class killswitch_on_exception(object):
#     def __init__(self, func):
#         self.func = func

#     def __call__(self, *args, **kwargs):

#     def wrapper(*args, **kwargs):
#         process_killswitch = args[-1]

#         try:
#             func(*args)

#         # if any unexpected error occurs,
#         # print the traceback and kill all other processes
#         except:
#             traceback.print_exc()
#             print('killing all processes')
#             process_killswitch.value = True

#     return wrapper