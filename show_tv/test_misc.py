#!/usr/bin/env python3
# coding: utf-8

if __name__ == '__main__':
    import api
    
    if False:
        date_str = "161005131708.111"
        import re
        m = re.match(api.timestamp_pattern, date_str)
        assert m
        
        ts = api.parse_bl_ts(m.group("startstamp"), m.group("milliseconds"))
        print(ts)
        
        date_str2 = api.bl_int_ts2bl_str(ts)
        assert date_str == date_str2
        
    if True:
        full_lst = [chr(idx) for idx in range(ord('a'), ord('z')+1)]
        stream_range = {
            "names": "a, b, c, d, p-x",
            #"part":   "1, 4 - 6 / 6",
            #"python": [[1, [4, 6]], 6],
            "size":   10,
        }
        stream_lst = api.calc_from_stream_range(full_lst, stream_range)
        print(stream_lst)
    
