#!/usr/bin/env python
# coding: utf-8

import os
import shutil

def join(path, *fname):
    return os.path.join(path, *fname)

def without_ext(fname):
    # удалить расширение из имени файла/пути
    return os.path.splitext(fname)[0]

def extension(fname):
    """ В отличие от splitext, точка вначале убирается;
        если расширения нет вообще (а оно может быть, но при этом пустым, ''),
        то вернется не '', а None """
    ext = os.path.splitext(fname)[1]
    if ext:
        assert ext[0] == '.'
        ext = ext[1:]
    else:
        ext = None
    return ext

def exists(path):
    return os.path.exists(path)

def force_makedirs(dir_path):
    # обертка makedirs из-за ругани при сущ. директории
    if not exists(dir_path):
        os.makedirs(dir_path)

def split_all(fpath):
    # просто split(os.sep) неправилен для win32, потому что
    # c:\\foo должно превращаться в ['c:\\', 'foo']
    # (ведь os.path.join("c:", "foo") это "c:foo", и означает
    # "текущий путь + foo на диске c:", а не абсолютный путь c:\\foo)
    #return fpath.split(os.sep)
    res = []
    head, tail = os.path.split(fpath)
    if tail:
        # для "с:\\", "/" tail == ""
        if head:
            # для "ttt" head == ""
            res = split_all(head)
        res.append(tail)
    else:
        if head:
            # split() для путей типа "hhh/ggg/" дает единственный вариант с пустым tail, но
            # при этом работу продолжать нужно
            #res = [head]
            res = split_all(head) if (fpath != head) else [head]
    return res

def remove_file(fpath):
    res = False
    if os.path.isfile(fpath):
        res = True
        os.remove(fpath)
    return res

def del_any_fpath(fpath):
    if exists(fpath):
        if not remove_file(fpath):
            shutil.rmtree(fpath)

def for_write(fpath):
    """ Эта обертка над open() для записи в новый файл, чтобы
        минимизировать варианты подобных вызовов """
    return open(fpath, "wb")

def fix_rights(fpath):
    # :TRICKY: при выдаче контента через Apache (1gb.ru) последний работает
    # под отличным от скрипта пользователем, потому надо ему дать права на
    # чтение; это актуально для файлов, созданных через tempfile.NamedTemporaryFile()
    # tempfile.mkdtemp(), так как они создаются с маской 0600 и 0700 
    # (что само по себе правильно для многопользовательской системы и /tmp)
    add_read_rights = True
    if add_read_rights:
        import stat
        mode = os.stat(fpath).st_mode
        # если не дать чтение для группы, а для всех дать, то запрет для группы пересилит,
        # не нужно
        read_flags = stat.S_IRGRP|stat.S_IROTH
        os.chmod(fpath, mode | read_flags)

import tempfile
from contextlib import contextmanager

@contextmanager
def create_named_tmp_file(need_delete=True, suffix="", all_read=False):
    """ Клиент обязан закрыть файл """
    f = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    if all_read:
        # :KLUDGE: лучше сразу с нужными правами файла создавать => править tempfile.NamedTemporaryFile
        fix_rights(f.name)

    # :TRICKY: нам хочется свою инфо хранить
    assert not("need_delete" in dir(f))
    f.need_delete = need_delete
    
    try:
        yield f
    finally:
        if f.need_delete:
            os.remove(f.name)

def create_public_tmp_file():
    return create_named_tmp_file(all_read=True)
            
def for_all_files(dirname, fnr):
    for path, dirs, files in os.walk(dirname):
        for fname in files:
            full_name = path + '/' + fname
            fnr(full_name)
