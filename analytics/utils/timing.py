import time
import logging
def timing(f):
    def timing_wrap(*args, **kwargs):
        time1 = time.time()
        ret = f(*args, **kwargs)
        time2 = time.time()
        logging.info('{:s} function took {:.3f} ms / {:.3f} secs / {:.3f} mins'.format(f.__name__,(time2-time1)*1000, (time2-time1), (time2-time1)/60))

        return ret
    return timing_wrap