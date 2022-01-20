from functools import lru_cache, wraps
from datetime import datetime, timedelta
from urllib.parse import urlparse
import re


def timed_lru_cache(seconds: int, maxsize: int = 128):
    def wrapper_cache(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = timedelta(seconds=seconds)
        func.expiration = datetime.utcnow() + func.lifetime

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if datetime.utcnow() >= func.expiration:
                func.cache_clear()
                func.expiration = datetime.utcnow() + func.lifetime

            return func(*args, **kwargs)

        return wrapped_func

    return wrapper_cache


def get_package_name(package_url: str) -> str:
    url_path = urlparse(package_url).path.strip("/")
    tags_cleaned = re.sub(r"(@[^@]+)$", "", url_path).replace(".git", "")
    return tags_cleaned.split("/")[-1]
