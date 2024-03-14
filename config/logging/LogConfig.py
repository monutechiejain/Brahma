import logging
from datetime import datetime
#from logging.handlers import RotatingFileHandler


class LogConfig:

    def __init__(self):
        file_name = str(datetime.date(datetime.now())) + 'log'
        my_logger = logging.getLogger()
        my_logger.setLevel(logging.INFO)

        # handler = RotatingFileHandler(file_name, maxBytes=2000 * 1000, backupCount=50)
        # formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(threadName)s %(message)s')
        # handler.setFormatter(formatter)
        #
        # ch = logging.StreamHandler()
        # ch.setLevel(logging.DEBUG)
        # ch.setFormatter(formatter)
        # my_logger.addHandler(ch)
        #
        # my_logger.addHandler(handler)