#!/usr/bin/env python3
# coding: utf-8

import xml.dom.minidom as xmldom

import o_p
import os
import re

       
def make_dom(s):
    return xmldom.parseString(s)
 
# достаточно нахождения первого пока
def find_node(self, name):
    res = None
    for node in self.childNodes:
        if isinstance(node, xmldom.Element) and node.tagName == name:
            res = node
            break
    if not res:
        raise RuntimeError("No node with name '%s'" % name)
    return res

def find_node_by_path(node, path_lst):
    res = node
    for name in path_lst:
        res = find_node(res, name)
    return res

def iterate_elements(par_node):
    for node in par_node.childNodes:
        if isinstance(node, xmldom.Element):
            yield node

import yaml
def dump_yaml(stream, obj):
    return yaml.dump(
        obj, 
        stream = stream,
        default_flow_style=False,
        encoding='utf-8',
        allow_unicode=True,
    )

def dump2yaml(fname, data):
    with open(fname, "w") as dst_f:
        dump_yaml(dst_f, data)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--channel_list", default=None, help="save channel' list to the file")
    parser.add_argument("src_dir", help="where is wowza cfg directory")
    parser.add_argument("dst_cfg_yaml", help="where to save Bradbury Lab' config")

    args = parser.parse_args()
    fdir = args.src_dir
    
    def make_fpath(fname):
        return o_p.join(fdir, fname)
    
    channels = {}
    
    def name_parts(fname):
        return os.path.splitext(fname)[0].split("_")

    for fname in os.listdir(fdir):
        if not re.match(r".+\.smil", fname):
            continue
        
        fpath = make_fpath(fname)
        channels[name_parts(fname)[0]] = c_dct = {}
        
        dom = xmldom.parse(fpath)
        sw_node = find_node_by_path(dom, ["smil", "body", "switch"])
        
        c_dct["res-src"] = profiles = {}
        for node in iterate_elements(sw_node):
            stream_fname = node.attributes["src"].value
            
            profile = name_parts(stream_fname)[-1]
            # профили, указанные в Wowza - не настоящие
            profile = {
                # pervyj
                "270p": "270p",
                "360p": "360p",
                "576p": "406p",
                
                # eurosporthd
                "720p": "540p",
                "1080p": "720p",
            }[profile]
            
            with open(make_fpath(stream_fname)) as f:
                addr = f.read().strip()
            
            profiles[profile] = addr

    #print(dst.decode())
    dump2yaml(args.dst_cfg_yaml, channels)
        
    ch_list_fname = args.channel_list
    if ch_list_fname:
        lst = {"stream-always-lst": sorted(channels)}
        def dump_ch_lst(dst):
            return dump_yaml(dst, lst)
        
        if ch_list_fname == '-':
            import sys
            dump_ch_lst(sys.stdout)
        else:
            with open(ch_list_fname, "w") as dst_f:
                dump_ch_lst(dst_f)
        
    