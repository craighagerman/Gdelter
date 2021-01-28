
import time

##############################################################################################################
#   Decorators
##############################################################################################################
def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if "log_time" in kw:
            name = kw.get("log_name", method.__name__.upper())
            kw["log_time"][name] = int((te - ts) * 1000)
        else:
            # print("Running Time for {} : {:.3f} s".format(method.__name__, (te - ts),  ))
            print("Running Time for {} : {:.2f} min".format(method.__name__, (te - ts) / 60,  ))
        return result
    return timed


def good(method):
    print("stable, working version of method: {} ".format(method.__name__))

