import logging
from datetime import datetime

from diskcache import Cache, Disk

from beans.EnvironmentProperties import EnvironmentProperties


class BasicCache(Cache):
    __basicCache__ = None

    def __new__(cls, *args, **kwargs):
        if cls.__basicCache__ is None:
            cls.__basicCache__ = Cache(str(datetime.date(datetime.now())), 60, Disk)
            cls.__basicCache__.clear()
        return cls.__basicCache__

    def set(self, key, value, expire=None, read=False, tag=None):
        """
        This custom overrides diskcache set method with time to live as 15 seconds. this is configurable
        """
        # htmlSource = html if (url is None) else self.extractHtmlFromUrl(url);
        expire = expire if (expire is not None) else float(EnvironmentProperties.cache_time)
        logging.info("time to live for key %s is %s", key, expire)
        Cache.set(self, key=key, value=value, expire=expire)
