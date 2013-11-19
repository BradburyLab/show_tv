#!/usr/bin/env python3
# coding: utf-8

if __name__ == '__main__':
    import api
    
    date_str = "161005131708.111"
    import re
    m = re.match(api.timestamp_pattern, date_str)
    assert m
    
    ts = api.parse_bl_ts(m.group("startstamp"), m.group("milliseconds"))
    print(ts)
    
    date_str2 = api.bl_int_ts2bl_str(ts)
    assert date_str == date_str2
    
