#!/usr/bin/python
# -*- coding: UTF-8 -*-

if __name__ == "__main__":
    import stream
    #rn_dct, names_dct = stream.get_channels()

    port = stream.PORT
    import list_bl_tv
    for cnxt in list_bl_tv.rewrite_channels("tp.xspf", ["refname"]):
        refname = cnxt.row[cnxt.clns["refname"]]
        list_bl_tv.write_channel(cnxt, "http://tp.bradburylab.tv:%(port)s/%(refname)s/playlist.m3u8" % locals())
        