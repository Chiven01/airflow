# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""sogou sso authentication backend"""
from __future__ import unicode_literals

from functools import wraps
from sys import version_info


from flask import flash, Response
from flask import url_for, redirect, make_response
from flask import request

import flask_login
# noinspection PyUnresolvedReferences
# pylint: disable=unused-import
from flask_login import login_required, current_user, logout_user  # noqa: F401
# pylint: enable=unused-import

from sqlalchemy import Column, String
from sqlalchemy.ext.hybrid import hybrid_property

from airflow import models
from airflow.utils.db import provide_session, create_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils import m2crypto
from airflow.configuration import  SSO_AUTH

LOGIN_MANAGER = flask_login.LoginManager()
#LOGIN_MANAGER.login_view = 'airflow.login'  # Calls login() below
LOGIN_MANAGER.login_view = SSO_AUTH 
LOGIN_MANAGER.login_message = None

LOG = LoggingMixin().log
PY3 = version_info[0] == 3


CLIENT_AUTH = None


class AuthenticationError(Exception):
    """Error returned on authentication problems"""


# pylint: disable=no-member
# noinspection PyUnresolvedReferences
class SogouUser(models.User):

    def __init__(self, user):
        self.user = user

    def authenticate(self, plaintext):
        """Authenticates user"""
        return True

    @property
    def is_active(self):
        """Required by flask_login"""
        return True

    @property
    def is_authenticated(self):
        """Required by flask_login"""
        return True

    @property
    def is_anonymous(self):
        """Required by flask_login"""
        return False

    def get_id(self):
        """Returns the current user id as required by flask_login"""
        return self.user.get_id()

    # pylint: disable=no-self-use
    # noinspection PyMethodMayBeStatic
    def data_profiling(self):
        """Provides access to data profiling tools"""
        return True
    # pylint: enable=no-self-use

    def is_superuser(self):
        """Returns True if user is superuser"""
        return hasattr(self, 'user') and self.user.is_superuser()


# noinspection PyUnresolvedReferences
@LOGIN_MANAGER.user_loader
@provide_session
def load_user(userid, session=None):
    """Loads user from the database"""
    LOG.debug("Loading user %s", userid)
    if not userid or userid == 'None':
        return None

    user = session.query(models.User).filter(models.User.id == userid).first()
    return SogouUser(user)



@provide_session
def login(self, request, session=None):
    """Logs the user in"""
    LOG.error("Loading user %s", current_user.is_authenticated)
    if current_user.is_authenticated:
        LOG.error("You are already logged in")
        return redirect(request.args.get("next") or url_for("admin.index"))

    
    ptoken = request.args.get('ptoken')
    
    if ptoken is None:
        return redirect(LOGIN_MANAGER.login_view)
    username, userid, ts = get_user_info(ptoken)
    
    if not ts_check(ts):
        return redirect(LOGIN_MANAGER.login_view)
    
    EmailSuffix = "@sogou-inc.com"
    email = username + EmailSuffix
    user = session.query(models.User).filter(
        models.User.username == username).first()
    if not user:
        user = models.User(
            id=userid,
            username=username,
            email=email,
            is_superuser=False)

    session.merge(user)
    session.commit()
    flask_login.login_user(SogouUser(user))
    session.commit()
    return redirect(request.args.get("next") or url_for("admin.index"))
    

def get_user_info(ptoken):
    """
    decrypt user info from ptoken
    :param ptoken: request
    :raise AuthenticationError: if an error occurred
    :return:username,ts
    """
    #todo 判断返回码?
    user_info = m2crypto.rsa_decrypt(ptoken)
        

    return user_info['uid'], user_info['uno'], user_info['ts'] 

def ts_check(ts):
    import time
    now = int(round(time.time() * 1000))
    if now - ts > 1000 * 60 or now - ts < -1000 * 60 :
        return False 
    else :
        return True
    
    


def _unauthorized():
    """
    Indicate that authorization is required
    :return:
    """
    return redirect(LOGIN_MANAGER.login_view)


def _forbidden():
    return Response("Forbidden", 403)


def init_app(_):
    """Initializes backend"""

class AuthenticationError(Exception):
    pass
