#!/usr/bin/env python3
# coding: utf-8

if __name__ == '__main__':
    import api
    
    for date_str in ["161005131708.111", "19700101000200.000"]:
        ts = api.parse_bl_ts(date_str)
        #print(ts)
        
        date_str2 = api.bl_int_ts2bl_str(ts)
        assert date_str == date_str2
        
    if False:
        full_lst = [chr(idx) for idx in range(ord('a'), ord('z')+1)]
        stream_range = {
            "names": "a, b, c, d, p-x",
            #"part":   "1, 4 - 6 / 6",
            #"python": [[1, [4, 6]], 6],
            "size":   10,
        }
        stream_lst = api.calc_from_stream_range(full_lst, stream_range)
        print(stream_lst)
    
    if False:
        import datetime
        now = datetime.datetime.utcnow()
        print(now)
        res = api.calc_flv_ts(now)
        print("%x" % res)

        import time
        time.sleep(1)
        
        print(api.restore_utc_ts(res))