#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import yaml
import os
import urlpath
import stat

from pathlib import PosixPath, _PosixFlavour, PurePath
from pathlib import Path

import collections.abc
import functools
import re
import urllib.parse
import requests
from bs4 import BeautifulSoup
import fnmatch
import numpy as np
import io
import tempfile
from argparse import Namespace

try:
    from gurlpath.cylog import Cylog
    from gurlpath.db import CacheDatabase
except ModuleNotFoundError:
    from cylog import Cylog
    from db import CacheDatabase
'''
class derived from urlpath to provide pathlib-like
interface to url data
'''

__author__    = "P. Lewis"
__email__     = "p.lewis@ucl.ac.uk"
__date__      = "28 Aug 2020"
__copyright__ = "Copyright 2020-2022 P. Lewis"
__license__   = "MIT License"

class URL(urlpath.URL):
    '''
    Object-oriented URL library

    Derived from 
    https://raw.githubusercontent.com/chrono-meter/urlpath/master/urlpath.py

    to provide more compatibility with pathlib.Path functionality

    param cachedir:         str: cache directory. Get dbdir from dbdir,
                            default '.'

    '''
    def __new__(cls,*args,**kwargs):
        '''
        new URL
        makes call to init(**kwargs)
        '''
        self = super(URL, cls).__new__(cls,*args)

        self.init(**kwargs)
        return self

    def defaults(self):
        """
        set defaults
        :return: None
        """
        self.msgs = []
        self.verbose = False
        self.cachedir = "."
        self.nocache = False
        self.refreshcache = False
        self.timeout = None

    def init(self, **kwargs):
        """
        :param kwargs: pass any kw args through to object
        :return:
        """
        self.defaults()
        self.__dict__.update(**kwargs)

    def isfile(self):
        """
        Test to see if URL corresponds to a local file

        :return: True if file
        """
        if self.scheme == '' or self.scheme == 'file':
            return True
        return False

    def local_file(self,cachedir=None):
        """
        Find the name of the local cache file for this URL

        :param cachedir: override self.cachedir

        :return: Path() object for local file or None
        """
        if self.isfile():
            self.nocache = False
            return Path(self)
        path = self.path
        if len(path) and path[0] == '/':
            path = path[1:]
        return Path(cachedir or self.cachedir,path)

    def msg(self,msg):
        """
        Print message
        :return: None
        """
        # dont repeat
        if msg not in self.msgs:
            if self.verbose:
                print(msg)
            self.msgs.append(msg)

    def readable(self,f):
        """
        Return True if file is readable

        :param f: str filename
        :return:
        """
        # play around with lstat to get octal permission
        return bin(int(oct(Path(f).lstat().st_mode)[-3]))[-3:][0] == '1'

    def writeable(self, f):
        """
        Return True if file is readable

        :param f: str filename
        :return:
        """
        # play around with lstat to get octal permission
        return bin(int(oct(Path(f).lstat().st_mode)[-3]))[-3:][1] == '1'

    def get_login(self,head=True):
        self.msg('getting login and password')
        with requests.Session() as session:
            if self.username and self.password:
                session.auth = self.username,self.password
            else:
                self.msg(f'getting login and password for {self.anchor} from cylog()')
                uinfo = Cylog(self.anchor).login()
                if uinfo == (None,None):
                    return None
                session.auth = uinfo[0].decode('utf-8'),uinfo[1].decode('utf-8')
                self.msg(f'logging in to {self.anchor}')
            try:
                self.msg(f'requesting get for {self.path}')
                r1 = session.request('get',self)
                if r1.status_code == 200:
                    self.msg(f'status good for {self.path}')
                    return r1
                # try encoded login
                if head:
                    self.msg(f'trying to access head for {self.path}')
                    r2 = session.head(r1.url)
                else:
                    self.msg(f'trying to access data for {self.path}')
                    r2 = session.get(r1.url)
                if r2.status_code == 200:
                    self.msg(f'data read for {self.path}')
                if type(r2) == requests.models.Response:
                    self.msg(f'problem with login/read for {self.path}')
                    return r2
            except:
                self.msg(f'failure reading data from {self.anchor}')
                return None
        self.msg(f'failure reading data from {self.anchor}')
        return None

    def pull_file(self,local_file,ftype='binary',skipper=False):
        """
        try a simple get()

        :param local_file: Path local file name for storage
        :param ftype: str: file type ('text' or 'binary')
        :return: data
        """
        if not skipper:
            self.msg('trying get() ...')
            r = self.get()
            self.r = r
            if type(r) == requests.models.Response:
                if r.status_code == 200:
                    # returned ok
                    return (ftype == 'binary' and r.content) or r.text
                else:
                    self.msg(f'status code for {self.path} {r.status_code}')
        # unauthorised: try with a login
        r = self.get_login(head=False)
        self.r = r
        if type(r) != requests.models.Response:
            return None
        if r.status_code == 200:
            self.msg(f'status code good for {self.path}')
            return (ftype == 'binary' and r.content) or r.text
        self.msg(f'status code poor for {self.path}: problem logging in or other access')
        return None

    def read(self,cachedir=None,ftype='binary',skipper=False):
        """
        Open the URL data in bytes mode, read it and return the data

        This first tries self.get() but if the authorisation is more complex
        (e.g. when using NASA server) then a fuller 2-pass session
        is used.

        You should specify any required login/password with
        with_components(username=str,password=str)

        :param cachedir: override self.cachedir

        :return: data from url
                 OR None                     : on failure
                 OR requests.models.Response : on connection problem
        """
        local_file = self.local_file()
        if (not self.nocache) and (not self.refreshcache):
            if local_file.exists() and local_file.readable():
                return (ftype == 'binary' and self.local_file.read_bytes()) or \
                    self.local_file.read_text()
        # else pull the file and try again
        if not self.nocache:
            local_file.parent.mkdir(parents=True, exist_ok=True)
        data = self.pull_file(local_file,ftype='binary',skipper=skipper)
        if (data != None) and (not self.nocache):
            # write to local file
            (ftype == 'binary' and local_file.write_bytes(data)) or \
            local_file.write_text()
        return data

    def read_bytes(self,cachedir=None,skipper=False):
        """
        Open the URL data in text mode, read it and return the data

        This first tries self.get() but if the authorisation is more complex
        (e.g. when using NASA server) then a fuller 2-pass session
        is used.

        You should specify any required login/password with
        with_components(username=str,password=str)

        :param cachedir: override self.cachedir

        :return: data from url
                 OR None                     : on failure
                 OR requests.models.Response : on connection problem
        """
        return self.read(cachedir=cachedir,ftype='binary',skipper=skipper)

    def read_text(self,cachedir=None,skipper=False):
        """
        Open the URL data in text mode, read it and return the data

        This first tries self.get() but if the authorisation is more complex
        (e.g. when using NASA server) then a fuller 2-pass session
        is used.

        You should specify any required login/password with
        with_components(username=str,password=str)

        :param cachedir: override self.cachedir

        :return: data from url
                 OR None                     : on failure
                 OR requests.models.Response : on connection problem
        """
        return self.read(cachedir=cachedir,ftype='text',skipper=skipper)

def main():
    u='https://e4ftl01.cr.usgs.gov/MOTA/MCD15A3H.006/2003.12.11/MCD15A3H.A2003345.h09v06.006.2015084002115.hdf'
    url = URL(u,verbose=True)
    data = url.read_bytes(skipper=True)


def xx():
  if False:
    u='https://e4ftl01.cr.usgs.gov/MOTA/MCD15A3H.006/2003.12.11'
    url = URL(u)
    files = url.glob('*0.hdf',pre_filter=True) 
    print(files) 

  if True:
    u='https://e4ftl01.cr.usgs.gov'
    import os
    os.environ['CACHE_FILE'] = 'data/database.db'

    url = URL(u,verbose=True,db_file='data/new_db.txt',local_dir='work')
    rlist = url.glob('MOT*/MCD15A3H.006/2003.12.11/*0.hdf',pre_filter=True)
    for i,r in enumerate(rlist):
      print(i)
      # we can save in decalring a new URL by passing old one
      u = URL(r,**(fdict(url.__dict__.copy())))
      data=u.read_bytes()
      # updata database
      u.flush()
      #u.write_bytes(data)

if __name__ == "__main__":
    main()

