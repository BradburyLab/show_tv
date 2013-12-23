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
    
    if True:
        import datetime
        now = datetime.datetime.utcnow()
        #print(now)
        
        def calc_flv_ts(py_ts):
            # константы, не менять при работающем DVR
            days = 24 # столько дней влезает в 32 signed bits для хранения в FLV
            first_date = datetime.datetime(2013, 12, 1)
            
            period = datetime.timedelta(days=days).total_seconds()
            delta = (py_ts - first_date).total_seconds()
            
            return delta % period

        res = calc_flv_ts(now)
        print(res)
        
        # to 32int
        print("%x" % int(res*1000))
        
        
        
        
