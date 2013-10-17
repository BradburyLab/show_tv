import re
segment_sign = re.compile(br"(segment|hds):'(.+)' starts with packet stream:.+pts_time:(?P<pt>[\d,\.]+)")

class StreamType:
    HLS = 0
    HDS = 1

