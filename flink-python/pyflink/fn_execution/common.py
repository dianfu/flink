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

from pyflink.table.udf import ScalarFunction


class MethodWrapper(object):
    """
    Represents a method that can be invoked by `ScalarFunctionInvoker`.

    :param obj_to_invoke: the object that contains the method.
    :param method_name: name of the method as a string.
    """

    def __init__(self, obj_to_invoke, method_name):
        if not isinstance(obj_to_invoke, ScalarFunction):
            raise ValueError('\'obj_to_invoke\' has to be a \'ScalarFunction\'.'
                             'Received %r instead.' % obj_to_invoke)

        from apache_beam.transforms import core
        full_arg_spec = core.get_function_arguments(obj_to_invoke, method_name)

        args = full_arg_spec[0]
        defaults = full_arg_spec[3]

        defaults = defaults if defaults else []
        method_value = getattr(obj_to_invoke, method_name)
        self.method_value = method_value
        self.args = args
        self.defaults = defaults


class ScalarFunctionSignature(object):
    """
    Represents the signature of a given `ScalarFunction` object. Signature of a `ScalarFunction`
    provides a view of the properties of a given `ScalarFunction`.
    """

    def __init__(self, scalar_function, input_offsets):
        assert isinstance(scalar_function, ScalarFunction)
        self.open_method = MethodWrapper(scalar_function, 'open')
        self.close_method = MethodWrapper(scalar_function, 'close')
        self.eval_method = MethodWrapper(scalar_function, 'eval')
        self.input_offsets = input_offsets


class ScalarFunctionInvoker(object):
    """
    An abstraction that can be used to execute ScalarFunction methods.

    A ScalarFunctionInvoker describes a particular way for invoking methods of a ScalarFunction
    represented by a given ScalarFunctionSignature.
    """

    def __init__(self, signature):
        self.open_method = signature.open_method.method_value
        self.close_method = signature.close_method.method_value
        self.eval_method = signature.eval_method.method_value
        self.input_offsets = signature.input_offsets

    def invoke_open(self):
        self.open_method(None)

    def invoke_close(self):
        self.close_method()

    def invoke_eval(self, windowed_value):
        """
        Invokes the ScalarFunction.eval() function.

        :param windowed_value: a WindowedValue object that gives the element for which
                               eval() method should be invoked.
        """
        args = [windowed_value.value[i] for i in self.input_offsets]
        return self.eval_method(*args)
