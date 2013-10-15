#!/usr/bin/env python3
# coding: utf-8

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    def add_arg(name, help):
        parser.add_argument(name, help=help)
        
    add_arg("src_file_dir", "source file directory")
    add_arg("src_basename", "source file basename (w/o extension)")
    add_arg("build_dir",    "build directory for make")
    args = parser.parse_args()
    #print args
    
    is_eq = True
    ln = min(len(args.build_dir), len(args.src_file_dir))
    for i in range(ln):
        if args.build_dir[i] != args.src_file_dir[i]:
            eq_len = i
            is_eq = False
            break
    
    if is_eq:
        eq_len = ln
    
    import o_p
    path = o_p.join(args.src_file_dir[eq_len:], args.src_basename + ".o")

    args = "/usr/bin/make", [path]
    print("Running:", *args)
    # для Qt Creator'а требуется flush, а то не покажется Running:
    import sys
    sys.stdout.flush()
    
    import call_cmd
    call_cmd.exec_process(*args, run_new_process=False)
