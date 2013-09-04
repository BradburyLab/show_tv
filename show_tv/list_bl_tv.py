#!/usr/bin/python
# -*- coding: UTF-8 -*-

import contextlib
import csv, os, o_p

def make_path(name):
    return o_p.join(os.path.dirname(__file__), name)

@contextlib.contextmanager
def make_tbl_clns(req_clns):
    # stream_fe = признак вещания
    req_clns.extend(["stream_fe", "channel_name_ru"])
    
    csv_path = make_path('tv_bl.csv')
    with open(csv_path) as csvf:
        tbl = csv.reader(csvf, delimiter=',')
        
        has_hdr = False
        for row in tbl:
            clns = {}
            for i, cname in enumerate(row):
                if cname in req_clns:
                    clns[cname] = i
            if len(clns) == len(req_clns):
                has_hdr = True
                break
        assert has_hdr
        
        yield tbl, clns

def is_streaming(row, clns):
    return row[clns["stream_fe"]]

def channel_name(row, clns):
    return row[clns["channel_name_ru"]]

def make_formatter(fmt_, f_):
    class formatter:
        fmt = fmt_
        f = f_
    return formatter

def write_prefix(fmtr):
    txt = {
        "m3u8": "#EXTM3U\n",
        "html": """<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html;charset=utf-8">
</head>
<body>
<table><tbody>
""",
                "xspf": """<?xml version="1.0" encoding="UTF-8"?>
<playlist xmlns="http://xspf.org/ns/0/" version="1">
<trackList>
""",
    }[fmtr.fmt]
    fmtr.f.write(txt)
   
def write_suffix(fmtr):
    fmtr.f.write({
        "m3u8": "",
        "html": """
</tbody></table>
</body>
</html>
""",
        "xspf": """</trackList></playlist>""",
    }[fmtr.fmt])

def write_channel(cnxt, url):
    fmtr = cnxt.fmtr
    f = fmtr.f
    name = cnxt.name
    f.write({
        "m3u8": url,
        "html": """<tr><td><a href="%(url)s">%(name)s</a></td></tr>""",
        "xspf": """<track><title>%(name)s</title><location>%(url)s</location></track>""",
    }[fmtr.fmt] % locals())            
    f.write("\n")

@contextlib.contextmanager
def gen_formatter(dst_fname, fmt):
    with o_p.for_write(make_path(dst_fname)) as f:
        fmtr = make_formatter(fmt, f)
        write_prefix(fmtr)
        yield fmtr
        write_suffix(fmtr)

def all_channels(req_clns):
    with make_tbl_clns(req_clns) as (tbl, clns):
        for row in tbl:
            name = channel_name(row, clns)
            if name and is_streaming(row, clns):
                yield (row, name, clns)

def rewrite_channels(dst_fname, req_clns, fmt="xspf"):
    with gen_formatter(dst_fname, fmt) as fmtr_:
        for row_, name_, clns_ in all_channels(req_clns):
            class context:
                row = row_
                name = name_
                clns = clns_
                fmtr = fmtr_
            yield context

if __name__ == "__main__":
    fmt = "xspf" # "m3u8" # "html"

    import get_url
    res, msg = get_url.test_inet()
    if not res:
        print "No internet, test connection: ", msg
    else:
        import re
        pat = re.compile(r"^#EXT-X-STREAM-INF.+BANDWIDTH=(?P<bandwidth>\d+).*(?:\n|\r\n?)(?P<stream>.+)", re.MULTILINE)
        dst_fname = {
            "m3u8": "vlc.m3u8",
            "html": "tv_bl.html",
            "xspf": "vlc.xspf"
        }[fmt]

        req_clns = ["ts_port"]
        for cnxt in rewrite_channels(dst_fname, req_clns, fmt=fmt):
            # :TRICKY: своей колонки нет
            hls_idx = cnxt.clns["ts_port"] + 1
            url = cnxt.row[hls_idx]
            
            if url.startswith("http://"):
                print name, url
                try:
                    with contextlib.closing(get_url.get_url(url)) as pf:
                        txt = pf.read()
                except get_url.URLError:
                    pass
                else:
                    max_bw, max_url = 0, None
                    for m in pat.finditer(txt):
                        bw = int(m.group('bandwidth'))
                        if not max_bw or max_bw < bw:
                            max_bw = bw
                            max_url = m.group('stream')
                    assert max_url
                    max_url = o_p.join(os.path.dirname(url), max_url)
                    write_channel(cnxt, max_url)
