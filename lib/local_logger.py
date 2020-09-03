#!/usr/bin/python3

import logging

class MyLoggerFormatter(logging.Formatter):

    debug_format = '%(filename)s(%(funcName)s), line %(lineno)d ' + \
                   '%(levelname)s: %(message)s'
    info_format = '%(message)s' # INFO looks like print.

    def __init__(self):
        default_format = '%(filename)s(%(funcName)s) %(levelname)s: %(message)s'
        super().__init__(fmt=default_format, datefmt=None, style='%')

    def format(self, record):

        # Save the original format configured by the user when the logger
        # formatter was instantiated.
        format_orig = self._style._fmt

        # Replace the original format with the one customized by logging
        # level.
        if record.levelno == logging.DEBUG:
            self._style._fmt = MyLoggerFormatter.debug_format
        elif record.levelno == logging.INFO:
            self._style._fmt = MyLoggerFormatter.info_format

        # Call the original formatter class.
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user.
        self._style._fmt = format_orig

        return result

def init(level=logging.INFO):

    console_handler = logging.StreamHandler()
    fmt = MyLoggerFormatter()
    console_handler.setFormatter(fmt)
    # log_format = '%(asctime)s {} '.format(socket.gethostname()) + \
    #              '%(filename)s[%(process)d]: %(levelname)s: %(message)s'
    # log_format = '%(filename)s(%(funcName)s): %(levelname)s: %(message)s'
    # console_formatter = logging.Formatter(log_format) # 
    # console_handler.setFormatter(console_formatter)
    # console_handler.setLevel(logging.DEBUG)
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(console_handler)

    return logger
