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
except ModuleNotFoundError:
    from cylog import Cylog

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

    '''
    def __new__(cls,*args,**kwargs):
        '''
        new URL
        makes call to init(**kwargs)
        '''
        self = super(URL, cls).__new__(cls,*args)
        self.init(**kwargs)
        return self

    def init(self, **kwargs):
        """

        :param kwargs: pass any kw args through to object
        :return:
        """
        self.__dict__.update(**kwargs)

    def msg(self,msg):
        """
        Print message
        :return: None
        """
        # dont repeat
        if msg not in self.msgs:
            print(msg)
            self.msgs.append(msg)

    def get_login(self,head=True):
        with requests.Session() as session:
            if self.username and self.password:
                session.auth = self.username,self.password
            else:
                uinfo = Cylog(self.anchor).login()
                if uinfo == (None,None):
                    return None
                session.auth = uinfo[0].decode('utf-8'),uinfo[1].decode('utf-8')
                self.msg(f'logging in to {self.anchor}')
            try:
                r1 = session.request('get',self)
                if r1.status_code == 200:
                    self.msg(f'data read from {self.anchor}')
                    return r1
                # try encoded login
                if head:
                    r2 = session.head(r1.url)
                else:
                    r2 = session.get(r1.url)
                if r2.status_code == 200:
                    self.msg(f'data read from {self.anchor}')
                if type(r2) == requests.models.Response:
                    return r2
            except:
                self.msg(f'failure reading data from {self.anchor}')
                return None
        self.msg(f'failure reading data from {self.anchor}')
        return None

def main():
  if False:
    u='https://e4ftl01.cr.usgs.gov/MOTA/MCD15A3H.006/2003.12.11/MCD15A3H.A2003345.h09v06.006.2015084002115.hdf'
    url = URL(u)
    data = url.read_bytes()
    ofile = Path('data',url.name)
    osize = ofile.write_bytes(data)
    assert osize == 3365255
    print('passed')

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

