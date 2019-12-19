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

"""add simpele dagbag pickle

Revision ID: af9bf34665d4
Revises: e3a246e0dc1
Create Date: 2019-12-09 20:36:52.877277

"""

# revision identifiers, used by Alembic.
revision = 'af9bf34665d4'
down_revision = 'e3a246e0dc1'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


def upgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()

    if 'simple_dagbag_pickle' not in tables:
        op.create_table(
            'simple_dagbag_pickle',
            sa.Column('file_name', sa.String(length=100), nullable=False),
            sa.Column('upgrade_dttm', sa.DateTime, nullable=True),
            sa.Column('pickle', sa.LargeBinary(length=1048576), nullable=True),
            sa.PrimaryKeyConstraint('file_name')
        )


def downgrade():
    op.drop_table('simple_dagbag_pickle')
