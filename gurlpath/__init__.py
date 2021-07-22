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

#from cylog import Cylog
#from database import Database,ginit

'''
class derived from urlpath to provide pathlib-like
interface to url data, hence gurlpath
'''

__author__    = "P. Lewis"
__email__     = "p.lewis@ucl.ac.uk"
__date__      = "12 July 2021"
__copyright__ = "Copyright 2020-2022 P. Lewis"
__license__   = "MIT License"

