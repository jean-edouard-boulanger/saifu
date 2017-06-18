"""General utility function module"""
import datetime
import time
import logging
import json

def utc_time():
    """Returns the current timestamp in UTC timezone"""
    return datetime.datetime.utcnow()

def utc_time_with_offset(delta):
    """Returns the current plus a given offset"""
    return utc_time() + delta

def utc_from_timestamp(ts):
    """Returns UTC date time from timestamp"""
    if ts is None:
        return None
    return datetime.datetime.utcfromtimestamp(ts)

def to_timestamp(dt):
    """Returns timestamp from date time"""
    if dt is None:
        return None
    return time.mktime(dt.timetuple())

def serialize(obj):
    if type(obj) is list:
        lst = obj
        output = []
        return json.dumps([o.to_json() for o in lst])
    return json.dumps(obj.to_json())

def unserialize(one_or_many_obj, The_type):
    def unserialize_one(one_obj):
        instance = The_type()
        instance.from_json(one_obj)
        return instance
    one_or_many_obj = json.loads(one_or_many_obj)
    if type(one_or_many_obj) is list:
        many_obj = one_or_many_obj
        return [unserialize_one(one_obj) for one_obj in many_obj]
    return unserialize_one(one_or_many_obj)
