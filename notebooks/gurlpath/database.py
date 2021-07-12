#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import yaml
import os
import urlpath
import stat

import urllib
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

from cylog import Cylog
from listfile import ListPath

'''
database for URL lookup and other things ...
'''

__author__    = "P. Lewis"
__email__     = "p.lewis@ucl.ac.uk"
__date__      = "28 Aug 2020"
__copyright__ = "Copyright 2020 P. Lewis"
__license__   = "GPLv3"
    
class Database():
    '''
    URL look up database
    '''
    
    def __del__(self):
        '''
        cleanup: foce write if deleted
        '''
        try:
            if (self.database != {}) and \
                (self.init_database != self.database):
                self.set_db(self.database,write=True)
        except:
            pass

    def __exit__(self, *exc):
        '''
        cleanup
        '''
        self.__del__()
    
    def ginit(self,dbfiles,**kwargs):
        '''

        Class initialisation routine.

        1. prepare defaults
        Goes through various mechanisms to 
        find a suitable setup and default values
        and sets all of these into self.__dict__

        2. set logic for working directories etc.
        This affects:
        - self.local_dir : the local base directory for
                           storing file. This is defined 
                           as a list. When trying to write
                           we look for the first one we can
                           write to. When reading, we look 
                           in all, in order.

        3. set logging 

        Returns:
            self.__dict__

        Information is passed by (in precedence):
            - any items in ~/.url_db/init.yml
            - kwargs['defaults']
              (or internal default values). This is
              *not* loaded into self.__dict__. Instead,
              the values it contains are.
            - kwargs


        It sets up:
        - 'defaults' items

        and checks for file ~/.url_db/init.yml

        '''
        dbfiles = ListPath(dbfiles)
        
        # 1. prepare defaults
        #--------------------
        # base defaults
        defaults = {\
         'verbose'    : False,\
         'noclobber'  : True,\
         'size_check' : False,\
         'store_msg'  : [],\
         'log'        : None,\
         'database'   : None,\
         'db_dir'     : ["work"],\
         'db_file'    : None,\
         'stderr'     : sys.stderr,\
         'local_dir'  : ["work"],\
        }
        
        if 'CACHE_FILE' in os.environ and os.environ['CACHE_FILE'] is not None:
            defaults['db_file'] = ListPath(os.environ['CACHE_FILE'])

        # try to read values from ~/.url_db/.init
        initfile = ListPath['~/.url_db/init.yml'][0]
        
        if initfile.exists():
            #self.msg(f'reading init file {initfile.as_posix()}')
            with initfile.open('r') as f:
                info = yaml.safe_load(f)
        else:
            info = {}

        # update any of the default
        for k in defaults.keys():
            if k in info.keys():
                defaults[k] = info[k]
            if ('defaults' in kwargs) and \
               (k in kwargs['defaults'].keys()):
                defaults[k] = kwargs['defaults'][k]
            if k in kwargs.keys():
                defaults[k] = kwargs[k]

        # delete the defaults item
        if 'defaults' in kwargs:
            del kwargs['defaults']

        # update defaults with info and kwargs
        defaults.update(info)
        defaults.update(kwargs)
        if len(dbfiles):
            defaults['db_file'].insert(0,dbfiles)
        
        # update self.__dict__
        self.__dict__.update(defaults)
        # end of 
        # 1. prepare defaults
        #--------------------

        # 2. set logic for 
        # working directories etc.

        self.local_dir = ListPath(self.local_dir,name=self.name)
        self.db_dir    = ListPath(self.db_dir)
        self.db_file   = ListPath(self.db_dir,name='.db.yml')

        # 3. set logging 
        #--------------------
        # dont repeat yourself
        self.store_msg = ListPath(self.store_msg)
        self.store_msg = self.store_msg.remove_duplicates(self.store_msg)

        # open a channel for self.log as self.stderr
        # or use sys.stderr
        if self.log is not None:
            try:
                self.stderr = Path(self.log).open("a")
                if self.verbose:
                try:
                    msg = f"{str(self)}: log file {self.log}"
                    self.store_msg.append(msg)
                except:
                    pass
            except:
                self.stderr = sys.stderr
                self.msg(f"WARNING: failure to open log file {self.log}")
        else:
            self.stderr = sys.stderr


        return self.__dict__


    def __init__(self,args,**kwargs):
        '''
        kwargs setup and organisation of local_dir
        and db_dir

        args are database files
        '''
        # initialise database and defaults
        self.ginit(kwargs)
       
        if self.database and (len(self.database.keys())):
            self.msg('getting database from command line')
        else:
            self.database = self.set_db(dict(self.get_db()))
        self.init_database = self.database.copy()


    def filter_db(self,old_db):
        '''clean the database'''
        if not 'data' in old_db.keys():
            return old_db
        old_db = dict(old_db)
        data = dict(old_db['data'])

        # cleaning ...
        try:
            for k,v in data.items():
                if v is None:
                    # error in dba
                    print(f"WARNING: database None error {k}:{v}")
                    del data[k]
                elif type(v) is list:
                    v = v[0]
                if Path(v).is_dir():
                    del data[k]
                else:
                    data[k] = str(v)
            old_db['data'] = data
    except:
        print(f"WARNING: database error {k}:{v}")  
    return old_db


  def set_db(self,new_db,write=False,clean=False):
    '''save dictionary db in cache database'''
    if write:
      vold_db = self.database or dict(self.get_db())
      if not clean:
        old_db = vold_db.copy()
      else:
        old_db = {}

    new_db = dict(new_db)

    if write:
      for k in new_db.keys():
        if k in old_db:
          try:
            old_db[k].update(new_db[k])
          except:
            # format error
            self.msg(f"WARNING fixing database format error for {self.db_file}")
            old_db[k] = new_db[k]
        else:
          old_db[k] = new_db[k]
      old_db = self.filter_db(old_db)

    new_db = self.filter_db(new_db)

    db_files = self.db_file
    readlist,writelist = list_info(db_files)

    if write and ((readlist is None) or (old_db is {})):
      return old_db.copy()

    if not write:
      return new_db

    for dbf in np.array(db_files,dtype=np.object)[writelist]:
      # make a copy first
      try:
        with Path(str(dbf)+'.bak').open('w') as f:
          self.msg(f"updated cache database in {dbf}")
          yaml.safe_dump(vold_db,f)
      except:
        self.msg(f"unable to update cache database in {str(dbf)+'.bak'}")
      try:
        with dbf.open('w') as f:
          self.msg(f"updated cache database in {str(dbf)}")
          yaml.safe_dump(old_db,f)
      except:
        self.msg(f"unable to update cache database in {dbf}")

    return new_db

  def get_db(self):
    '''get the cache database dictionary'''
    db_files = self.db_file
    old_db = {}
    readlist,writelist = list_info(db_files)
    for dbf in np.array(db_files,dtype=np.object)[readlist]:
      with dbf.open('r') as f:
        #self.msg(f'reading db file {dbf}')
        try:
          fin = dict(yaml.safe_load(f))
        except:
          self.msg(f'WARNING: error reading data from {dbf}')
        try:
          old_db.update(fin)
        except:
          try:
            self.msg(f'WARNING: error updating with data {fin} from {dbf}')
          except:
            pass
    return old_db

  def rm_from_db(self,store_flag,store_url,**kwargs):
    self.database = self.database or self.get_db()
    del self.database[store_flag][str(store_url)]
    self.set_db(self.database,clean=True)
    return self.database

  def get_from_db(self,flag,url):
    '''see if url is in database'''
    url = str(url)
    try:
      self.database = self.database or self.get_db()
    except:
      self.msg(f'db file {self.call_db()}')
      self.database = self.database or self.get_db()
    try:
      keys = self.database.keys()
    except:
      self.database = self.get_db()
      keys = self.database.keys()
    if flag in self.database.keys():
      try:
        if url in self.database[flag].keys():
          self.msg(f'retrieving {flag} {url} from database')
          return list(np.unique(np.array(self.database[flag][url],dtype=np.object)))
      except:
        pass
    return None

  def msg(self,*args):
    '''msg to self.stderr'''
    this = str(*args)
    try:
      # DONT REPEAT MESSAGES ... doesnt work as yet
      if this in self.store_msg:
        return
      self.store_msg.append(this)
    except:
      self.store_msg = [this]
    try:
        if self.verbose or (self.log is not None):
            print('-->',*args,file=self.stderr)
    except:
        pass


    

def main():
    # database fromn file
    kwargs = {
        'verbose'   :    True
    }
    dbs = ['data/database.db','data/new_db.txt','data/lai_filelist_2016.dat.txt','data/lai_filelist_2017.dat.txt']
    db = Database(dbs,**kwargs)
    # this is how to pass on a database 
    database = db.database.copy()
    del db

    kwargs = {
        'database'  :    database,   
        'verbose'   :    True
    }
    db = Database(dbs,**kwargs)
    del db

    # try no arg: default
    kwargs = {
        'verbose'   :    True
    }
    db = Database(None,**kwargs)

if __name__ == "__main__":
    main()


