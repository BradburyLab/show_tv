#!/usr/bin/env python3
# coding: utf-8

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("src_csv")
    parser.add_argument("ott_yaml")
    parser.add_argument("dst_yaml")

    args = parser.parse_args()
    src_csv = args.src_csv
    
    import yaml
    with open(args.ott_yaml) as f:
        ott_channels = yaml.load(f)
    
    import list_bl_tv
    
    channels = {}
    with list_bl_tv.make_tbl_clns(["refname", "mc_in_address", "mc_in_port"], src_csv) as (tbl, clns):
        for row in tbl:
            # :TRICKY: в новой версии имя есть refname, а не stream_fe
            refname = row[clns["refname"]]
            if not refname:
                continue

            refname = refname.lower()
            ch = channels[refname] = {}
            
            dct = dict((key, row[val]) for key, val in clns.items())
            ch["src"] = 'udp://{mc_in_address}:{mc_in_port}'.format(**dct)
            
            profiles = ["270p", "360p", "406p"]
            ott_ch = ott_channels.get(refname)
            if ott_ch:
                lst = list(ott_ch["res-src"].keys())
                if lst:
                    lst.sort()
                    profiles = lst
            ch["res"] = profiles
                
            ch["params"] = {"transcoding": True}
            
    from wwz_to_f451_cfg import dump2yaml
    dump2yaml(args.dst_yaml, channels)
