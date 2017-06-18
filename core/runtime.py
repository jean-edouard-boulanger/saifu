"""Saifu runtime components module"""
import logging

def create_logger(settings):
    """Creates a logger from settings"""

    def log_level_from_string(level_str):
        """Converts a string log level to logging log level"""
        levels = {}
        levels["debug"] = logging.DEBUG
        levels["info"] = logging.INFO
        levels["warning"] = logging.WARNING
        levels["error"] = logging.ERROR
        levels["fatal"] = logging.FATAL
        return levels.get(level_str.lower(), logging.NOTSET)

    level = log_level_from_string(settings.level)

    logger = logging.getLogger(settings.category)
    logger.setLevel(level)

    sth = logging.StreamHandler()
    sth.setLevel(level)
    formatter = logging.Formatter(settings.log_format)
    sth.setFormatter(formatter)
    logger.addHandler(sth)

    return logger
