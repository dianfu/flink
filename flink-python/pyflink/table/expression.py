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
from pyflink.java_gateway import get_gateway
from pyflink.util.utils import to_jarray

__all__ = ['Expression']


def create_expression_from_column_name(name):
    gw = get_gateway()
    return getattr(gw.jvm.Expressions, '$')(name)


def get_java_expression(expr):
    if isinstance(expr, Expression):
        return expr._j_expr
    else:
        return expr


def get_or_create_java_expression(expr):
    if isinstance(expr, Expression):
        return expr._j_expr
    elif isinstance(expr, str):
        return create_expression_from_column_name(expr)
    else:
        raise TypeError(
            "Invalid argument: expected Expression or string, got {0}.".format(type(expr)))


def _unary_op(name, doc=''):
    def _(self):
        return Expression(getattr(self._j_expr, name)())
    _.__doc__ = doc
    return _


def _binary_op(name, doc="binary operator"):
    def _(self, other):
        j_expr = other._j_expr if isinstance(other, Expression) else other
        return Expression(getattr(self._j_expr, name)(j_expr))
    _.__doc__ = doc
    return _


class Expression(object):

    def __init__(self, j_expr):
        self._j_expr = j_expr

    __abs__ = _unary_op("abs")

    __eq__ = _binary_op("isEqual")

    __add__ = _binary_op("add")

    def is_not_null(self):
        return _unary_op("isNotNull")(self)

    def alias(self, alias, *extra_names):
        gateway = get_gateway()
        return Expression(getattr(self._j_expr, "as")(alias, to_jarray(
            gateway.jvm.String, extra_names)))

    def then(self, if_true, if_false):
        return Expression(getattr(self._j_expr, "then")(if_true._j_expr, if_false._j_expr))
