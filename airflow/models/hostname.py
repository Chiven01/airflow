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

from sqlalchemy import Column, Integer, String, Text, Index

from airflow.models.base import Base, ID_LEN
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.net import get_hostname

class Hostname(Base):
    """
    Used to record log and hostname to the database
    """

    __tablename__ = "hostname"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    try_number = Column(Integer, default=0, primary_key=True)
    hostname = Column(String(1000))


    def __init__(self, task_instance):
        self.hostname = get_hostname()

        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.execution_date = task_instance.execution_date
            self.try_number = task_instance.try_number

