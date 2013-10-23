import re
segment_sign = re.compile(br"(segment|hds):'(.+)' starts with packet stream:.+pts_time:(?P<pt>[\d,\.]+)")

class StreamType:
    HLS = 0
    HDS = 1

# :KLUDGE: пока DVR-хранилка не знает о типе вещания => храним в asset'е
DVR_SUFFEXES = {
    StreamType.HLS: "hls",
    StreamType.HDS: "hds",
}

def asset_name_rt(refname, typ):
    return "{0}_{1}".format(refname, DVR_SUFFEXES[typ])

def asset_name(r_t_b):
    return asset_name_rt(r_t_b.refname, DVR_SUFFEXES[r_t_b.typ])