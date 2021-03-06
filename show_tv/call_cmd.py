#!/usr/bin/env python
# coding: utf-8

import subprocess
import os, sys

class ExitException(Exception):
    def __init__(self, retcode):
        self.retcode = retcode

def raise_exit(retcode=1, err_msg="Error"):
    print(err_msg)
    raise ExitException(retcode)

def re_if_not(res, err_msg):
    if not res:
        raise_exit(err_msg=err_msg)

def call_cmd(cmd, cwd=None, err_msg=None):
    # под win хотим обычный вызов CreateProcess(), без всяких cmd.exe /c ""
    shell = sys.platform != 'win32'
    retcode = subprocess.call(cmd, cwd=cwd, shell=shell)
    if retcode:
        raise_exit(retcode, err_msg if err_msg else 'command failed: %s' % cmd)

def make_call_in_dst(dst):
    def call_in_dst(cmd, rel_cwd=None):
        cwd = dst
        if rel_cwd:
            cwd = os.path.join(dst, rel_cwd)
        call_cmd(cmd, cwd=cwd)
    return call_in_dst

def popen_output(cmd, cwd=None, shell=True):
    return subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, shell=shell).communicate()[0]

def exec_process(program_bin, args, redirect_stdout=None, run_new_process=True):
    # :TRICKY: первый аргумент будет не параметром, а именем
    # программы; в большинстве случаев оно ничего не решает, однако
    # c помощью символических ссылок на себя проги часто реализуют разное
    # поведение, пример - unlzma -> lzma
    def to_exec_args(*args):
        return (program_bin,) + args
    
    args = to_exec_args(*args)
    
    if run_new_process:
        # :TODO: разобраться, какой код выхода сохраняет оболочка - скрипта или собственно
        # рабочего процесса, и вернуть нужное значение
        import subprocess
        proc = subprocess.Popen(args, executable=program_bin, stdout=redirect_stdout, shell=False)
        # :TRICKY: ждем, чтобы группу процессов оставили в foreground'е
        ret = proc.wait()
        assert ret == 0
    else:
        import os
        if redirect_stdout:
            import sys
            # можно прямо константу 1 подставить во второй аргумент
            os.dup2(redirect_stdout.fileno(), sys.stdout.fileno())
        
        os.execv(program_bin, args)
        assert False
