# coding: utf-8

import o_p
import os
import api

import collections
RTPDbClass = collections.namedtuple('RTPDbClass', ['r_t_p', 'db_path'])

def rtp_db2dir(rtp_db):
    return api.rtp2local_dvr(rtp_db.r_t_p, rtp_db.db_path)

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
            
            dvr_lst.sort()
            
        cache[rtp_db] = dvr_lst
        
    return dvr_lst
load_dvr_lst.cache = {}

# :REFACTOR:
def bl_str2bl_int_ts(utc_ts):
    import re
    m = re.match(api.timestamp_pattern, utc_ts)
    assert m
    return api.parse_bl_ts(m.group("startstamp"), m.group("milliseconds"))

def parse_dvr_fname(fname):
    utc_ts, chunker_ts, dur = os.path.splitext(fname)[0].split("=")
    return bl_str2bl_int_ts(utc_ts), int(chunker_ts), int(dur)

def sec2dur(sec):
    return int(1000*sec)

def min2dur(mins):
    return sec2dur(mins*60)

def test_dvr_range(rtp_db):
    dvr_lst = load_dvr_lst(rtp_db)
    first_frg = dvr_lst[0]
    ts, f_chunker_ts, f_dur = parse_dvr_fname(first_frg)

    ts += min2dur(2)
    duration = min2dur(5)
    return ts, duration

import bisect
def request_names(rtp_db, startstamp, duration):
    dvr_lst = load_dvr_lst(rtp_db)
    if dvr_lst:
        beg = bisect.bisect_left(dvr_lst, api.bl_int_ts2bl_str(startstamp))
        end = bisect.bisect_right(dvr_lst, api.bl_int_ts2bl_str(startstamp+duration))
        
        lst = dvr_lst[beg:end]
    else:
        lst = []
    return lst

def request_range(rtp_db, startstamp, duration):
    names = request_names(rtp_db, startstamp, duration)

    lst = []
    if names:
        for name in names:
            utc_ts, chunker_ts, dur = parse_dvr_fname(name)

            # :REFACTOR:
            lst.append({
                'startstamp': utc_ts,
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
    db_path = '/home/muravyev/opt/bl/f451/tmp/out_dir'

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
    