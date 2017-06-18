"""General utility function module"""
import datetime
import time
import logging

def utc_time():
    """Returns the current timestamp in UTC timezone"""
    return datetime.datetime.utcnow()

def utc_time_with_offset(delta):
    """Returns the current plus a given offset"""
    return utc_time() + delta
