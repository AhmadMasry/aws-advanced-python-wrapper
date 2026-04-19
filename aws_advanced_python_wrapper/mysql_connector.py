#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""PEP 249 DBAPI module bound to mysql-connector-python.

Enables SQLAlchemy's creator-pattern:

    from sqlalchemy import create_engine
    from aws_advanced_python_wrapper.mysql_connector import connect

    engine = create_engine(
        "mysql+mysqlconnector://",
        creator=lambda: connect(
            "host=... user=... database=...",
            wrapper_dialect="aurora-mysql",
        ),
    )
"""

from __future__ import annotations

import sys
from typing import Any

from mysql.connector import connect as _mysql_connect

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def connect(conninfo: str = "", **kwargs: Any) -> AwsWrapperConnection:
    """PEP 249 `connect`, target-driver-bound to mysql-connector-python.

    Equivalent to::

        AwsWrapperConnection.connect(mysql.connector.connect, conninfo, **kwargs)
    """
    return AwsWrapperConnection.connect(_mysql_connect, conninfo, **kwargs)


_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
