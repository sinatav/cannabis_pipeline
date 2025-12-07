import time
from functools import wraps

def retry(times=3, delay_seconds=5, exceptions=(Exception,)):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for i in range(times):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if i < times - 1:
                        time.sleep(delay_seconds)
            raise last_exc
        return wrapper
    return deco
