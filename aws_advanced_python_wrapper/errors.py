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

from typing import Optional

from .pep249 import Error, InterfaceError, NotSupportedError, OperationalError

# Driver-native OperationalError classes — used as additional bases on
# FailoverSuccessError so SQLAlchemy's exception classifier reclassifies
# it to ``sqlalchemy.exc.OperationalError`` (SA checks
# ``isinstance(exc, dialect.dbapi.OperationalError)`` where the right-hand
# side is the driver's *own* class, not the wrapper's PEP-249 class).
#
# Each is conditionally imported so the wrapper still works when only one
# driver is installed. The stand-in classes (``class _NoXOpError(Exception)``)
# never get matched at runtime because the corresponding driver isn't
# installed -- they exist only to keep the multiple-inheritance declaration
# valid.
#
# All three drivers' OperationalError classes share ``Exception`` as their
# common ancestor (psycopg.errors.Error / mysql.connector.errors.Error /
# aiomysql.OperationalError-via-pymysql.err.MySQLError each inherit directly
# from Exception), so Python's C3 linearization produces a well-defined MRO.
try:
    from psycopg import OperationalError as _PsycopgOpError
except ImportError:
    class _PsycopgOpError(Exception):  # type: ignore[no-redef]
        pass

try:
    from mysql.connector.errors import OperationalError as _MCOpError
except ImportError:
    class _MCOpError(Exception):  # type: ignore[no-redef]
        pass

try:
    from aiomysql import OperationalError as _AiomysqlOpError
except ImportError:
    class _AiomysqlOpError(Exception):  # type: ignore[no-redef]
        pass


class AwsWrapperError(Error):
    __module__ = "aws_advanced_python_wrapper"
    driver_error: Optional[Exception]

    def __init__(self, message: str = "", original_error: Optional[Exception] = None):
        super().__init__(message)
        # If wrapping another AwsWrapperError, preserve the original driver exception
        if isinstance(original_error, AwsWrapperError) and original_error.driver_error is not None:
            self.driver_error = original_error.driver_error
        else:
            self.driver_error = original_error


class UnsupportedOperationError(AwsWrapperError, NotSupportedError):
    __module__ = "aws_advanced_python_wrapper"


class QueryTimeoutError(AwsWrapperError, OperationalError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverError(OperationalError):
    __module__ = "aws_advanced_python_wrapper"


class TransactionResolutionUnknownError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverFailedError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverSuccessError(FailoverError, _PsycopgOpError, _MCOpError, _AiomysqlOpError):
    # Inheriting from the driver-native OperationalError classes makes
    # ``isinstance(exc, dialect.dbapi.OperationalError)`` return True for
    # SA's classifier, so a successful failover surfaces to SA users as
    # ``sqlalchemy.exc.OperationalError`` (retryable by SA's standard
    # idioms). For non-SA users, ``except FailoverSuccessError:`` and
    # ``except psycopg.OperationalError:`` (or mysql/aiomysql equivalents)
    # both work -- and that's the correct semantic: a failover *is* a
    # connection-level operational error from the DBAPI's perspective.
    __module__ = "aws_advanced_python_wrapper"


class ReadWriteSplittingError(AwsWrapperError, InterfaceError):
    __module__ = "aws_advanced_python_wrapper"


class AwsConnectError(AwsWrapperError, OperationalError):
    __module__ = "aws_advanced_python_wrapper"
