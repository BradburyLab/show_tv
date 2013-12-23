# coding: utf-8

import o_p
import os
import api

import collections
RTPDbClass = collections.namedtuple('RTPDbClass', ['r_t_p', 'db_path'])

def rtp_db2dir(rtp_db):
    return api.rtp2local_dvr(rtp_db.r_t_p, rtp_db.db_path)

is_internal_pts_sort = True

# :KLUDGE: файлового API как к базе данных нет, поэтому загружаем
# все в память и там сортируем
def load_dvr_lst(rtp_db):
    cache = load_dvr_lst.cache
    
    dvr_lst = cache.get(rtp_db)
    if dvr_lst is None:
        dvr_lst = []
        
        dvr_dir = rtp_db2dir(rtp_db)
        if os.path.exists(dvr_dir):
            for fname in os.listdir(dvr_dir):
                dvr_lst.append(fname)
            
            # порядок один и тот же вне зависимости от is_internal_pts_sort
            dvr_lst.sort()
            
        cache[rtp_db] = dvr_lst
        
    return dvr_lst
load_dvr_lst.cache = {}

def parse_dvr_fname(fname):
    utc_ts, chunker_ts, dur = os.path.splitext(fname)[0].split("=")
    utc_ts, chunker_ts, dur = api.parse_bl_ts(utc_ts), int(chunker_ts), int(dur)
    
    ts = chunker_ts if is_internal_pts_sort else utc_ts
    return ts, dur

def sec2dur(sec):
    return int(1000*sec)

def min2dur(mins):
    return sec2dur(mins*60)

def test_dvr_range(rtp_db):
    dvr_lst = load_dvr_lst(rtp_db)
    first_frg = dvr_lst[0]
    ts, f_dur = parse_dvr_fname(first_frg)

    # в минутах
    start, duration = 3, 1 # 2, 5 # 
    return ts + min2dur(start), min2dur(duration)

##########################
# Расширение функций bisect из-за пользовательской less_op

def builtin_less(x1, x2):
    return x1 < x2

def bisect_left(a, x, lo=0, hi=None, less_op=None):
    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    if less_op is None:
        less_op = builtin_less

    while lo < hi:
        mid = (lo+hi)//2
        if less_op(a[mid], x):
            lo = mid+1
        else:
            hi = mid
    return lo

def bisect_right(a, x, lo=0, hi=None, less_op=None):
    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    if less_op is None:
        less_op = builtin_less
        
    while lo < hi:
        mid = (lo+hi)//2
        if less_op(x, a[mid]):
            hi = mid
        else:
            lo = mid+1
    return lo

##########################

#import bisect
def request_names(rtp_db, startstamp, duration):
    dvr_lst = load_dvr_lst(rtp_db)
    if dvr_lst:
        if is_internal_pts_sort:
            def make_fname(int_ts):
                return "=%s=" % int_ts
            def un_fname(x):
                return int(x.split("=")[1])
            def less_op(x1, x2):
                return un_fname(x1) < un_fname(x2)
                
            beg = bisect_left(dvr_lst, make_fname(startstamp), less_op=less_op)
            end = bisect_right(dvr_lst, make_fname(startstamp+duration), less_op=less_op)
        else:
            beg = bisect_left(dvr_lst, api.bl_int_ts2bl_str(startstamp))
            end = bisect_right(dvr_lst, api.bl_int_ts2bl_str(startstamp+duration))
        
        lst = dvr_lst[beg:end]
    else:
        lst = []
    return lst

def request_range(rtp_db, startstamp, duration):
    names = request_names(rtp_db, startstamp, duration)

    lst = []
    if names:
        for name in names:
            ts, dur = parse_dvr_fname(name)

            # :REFACTOR:
            lst.append({
                'startstamp': ts,
                'duration':   dur,
            })                
    
    return lst

def request_chunk(rtp_db, startstamp):
    payload = b''

    # список чанков ровно с таким UTC (на единицу больше уже не подходит)
    names = request_names(rtp_db, startstamp, 1)

    if names:
        with open(o_p.join(rtp_db2dir(rtp_db), names[0]), "rb") as f:
                payload = f.read()
                    
        return payload

def main():
    r_t_p = ("pervyj", api.StreamType.HDS), "270p"
    db_path = os.path.expanduser('~/opt/bl/f451/tmp/out_dir')

    rtp_db = RTPDbClass(
        r_t_p = r_t_p,
        db_path = db_path
    )
     
    ts, duration = test_dvr_range(rtp_db)
    res_lst = request_range(rtp_db, ts, duration)
    print(res_lst)
    
    payload = request_chunk(rtp_db, res_lst[-1]["startstamp"])

if __name__ == "__main__":
    main()
    