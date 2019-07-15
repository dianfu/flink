################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import collections
import functools
import inspect
import os
import sys
from abc import ABCMeta, abstractmethod

import pyflink
from pyflink.java_gateway import get_gateway
from pyflink.table.types import DataType, _to_java_type
from pyflink.util import utils

__all__ = ['FunctionContext', 'UserDefinedFunction', 'ScalarFunction']


class FunctionContext(object):
    pass


class UserDefinedFunction(object):
    """
    Base interface for user-defined function.
    """
    __metaclass__ = ABCMeta

    def open(self, function_context):
        pass

    def close(self):
        pass

    def is_deterministic(self):
        return True


class ScalarFunction(UserDefinedFunction):
    """
    Base interface for user-defined scalar function.
    """

    @abstractmethod
    def eval(self, *args):
        """
        Please define your implementation
        """
        pass


class LambdaScalarFunction(ScalarFunction):
    """
    Scalar function implementation for lambda function.
    """

    def __init__(self, func):
        self.func = func

    def eval(self, *args):
        return self.func(*args)


class UserDefinedFunctionWrapper(object):
    """
    Wrapper for Python user-defined function. It handles things like converting lambda
    functions to user-defined functions, creating the Java user-defined function representation,
    etc.
    """

    def __init__(self, func, input_types, result_type, name=None, deterministic=True):
        if inspect.isclass(func) or (
                not isinstance(func, UserDefinedFunction) and not callable(func)):
            raise TypeError(
                "Invalid function: not a function or callable (__call__ is not defined): "
                "{0}".format(type(func)))

        if not isinstance(input_types, collections.Iterable):
            input_types = [input_types]

        for input_type in input_types:
            if not isinstance(input_type, DataType):
                raise TypeError(
                    "Invalid input_type: input_type should be DataType "
                    "but contains {}".format(input_type))

        if not isinstance(result_type, DataType):
            raise TypeError(
                "Invalid returnType: returnType should be DataType "
                "but is {}".format(result_type))

        self._func = func
        self._input_types = input_types
        self._result_type = result_type
        self._judf_placeholder = None
        self._name = name or (
            func.__name__ if hasattr(func, '__name__')
            else func.__class__.__name__)
        self._deterministic = func.is_deterministic() if isinstance(
            func, UserDefinedFunction) else deterministic

    @property
    def _judf(self):
        if self._judf_placeholder is None:
            self._judf_placeholder = self._create_judf()
        return self._judf_placeholder

    def _create_judf(self):
        func = self._func
        if not isinstance(self._func, UserDefinedFunction):
            func = LambdaScalarFunction(self._func)

        import cloudpickle
        serialized_func = cloudpickle.dumps(func)

        gateway = get_gateway()
        j_input_types = utils.to_jarray(gateway.jvm.TypeInformation,
                                        [_to_java_type(i) for i in self._input_types])
        j_result_type = _to_java_type(self._result_type)
        return gateway.jvm.org.apache.flink.table.functions.python.PythonFunctionUtils \
            .createPythonScalarFunction(self._name,
                                        bytearray(serialized_func),
                                        _get_python_env(),
                                        j_input_types,
                                        j_result_type,
                                        self._deterministic)


# TODO: support to configure the python execution environment
def _get_python_env():
    python_path = sys.executable
    pip_path = os.path.join(os.path.dirname(python_path), "pip")
    command = os.path.join(os.path.dirname(
        os.path.abspath(pyflink.__file__)), "fn_execution", "run.sh")
    return {"python": python_path, "pip": pip_path, "command": command}


def _create_udf(f, input_types, result_type, name=None, deterministic=True):
    return UserDefinedFunctionWrapper(f, input_types, result_type, name, deterministic)


def udf(f=None, input_types=None, result_type=None, name=None, deterministic=True):
    """
    Creates a user-defined function.

    Example:
        ::

            >>> add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())
            >>> table_env.register_function("add_one", add_one)

    :param f: lambda function or user-defined function.
    :type f: lambda function or UserDefinedFunction
    :param input_types: DataType or the input data type.
    :type input_types: list
    :param result_type: the result data type.
    :type result_type: DataType
    :param name: the function name.
    :type name: str
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters.
    :type deterministic: bool
    :return: UserDefinedFunctionWrapper.
    """
    # decorator
    if f is None:
        return functools.partial(_create_udf, input_types=input_types, result_type=result_type,
                                 name=name, deterministic=deterministic)
    else:
        return _create_udf(f, input_types, result_type, name, deterministic)
