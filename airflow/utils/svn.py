#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pysvn
import locale

def setlocale():
    language_code, encoding = locale.getdefaultlocale()
    if language_code is None:
        language_code = 'en_GB'
    if encoding is None:
        encoding = 'UTF-8'
    if encoding.lower() == 'utf':
        encoding = 'UTF-8'
    locale.setlocale( locale.LC_ALL, '%s.%s' % (language_code, encoding))


def get_login(realm, username, may_save):
    retcode = True
    username = 'workyun'
    password = 'QgPj#c8R'
    save = False
    return retcode, username, password, save

setlocale()
svnclient = pysvn.Client()
svnclient.callback_get_login = get_login

