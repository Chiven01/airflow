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

"""added product_owner table

Revision ID: 1aca21c75479
Revises: c87c6486e86d
Create Date: 2019-10-17 17:41:03.485669

"""

# revision identifiers, used by Alembic.
revision = '1aca21c75479'
down_revision = 'c87c6486e86d'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('product_owner',
    sa.Column('owner', sa.Integer(), nullable=False),
    sa.Column('product', sa.String(length=500), nullable=False),
    sa.ForeignKeyConstraint(['owner'], ['users.id'], ),
    sa.ForeignKeyConstraint(['product'], ['products.productname'], ),
    sa.PrimaryKeyConstraint('owner', 'product')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('product_owner')
    # ### end Alembic commands ###
