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
from pyflink.table.expression import Expression, get_or_create_java_expression, get_java_expression
from pyflink.util.utils import to_jarray

_expressions_over_column = {
    'sqrt': 'Computes the square root of the specified float value.',
    'abs': 'Computes the absolute value.',

    'max': 'Aggregate function: returns the maximum value of the expression in a group.',
    'min': 'Aggregate function: returns the minimum value of the expression in a group.',
    'count': 'Aggregate function: returns the number of items in a group.',
    'sum': 'Aggregate function: returns the sum of all values in the expression.',
    'avg': 'Aggregate function: returns the average of the values in a group.',
    'mean': 'Aggregate function: returns the average of the values in a group.',
}


def _create_expression_over_column(name, doc=""):
    def _(expr):
        return Expression(getattr(get_or_create_java_expression(expr), name)())
    _.__name__ = name
    _.__doc__ = doc
    return _


for _name, _doc in _expressions_over_column.items():
    globals()[_name] = _create_expression_over_column(_name, _doc)


def if_then_else(condition, if_true, if_false):
    if not isinstance(condition, Expression):
        raise TypeError("condition should be an Expression")
    gateway = get_gateway()
    return Expression(gateway.Expressions.ifThenElse(
        condition._j_expr, get_java_expression(if_true), get_java_expression(if_false)))


def concat(first, *others):
    gateway = get_gateway()
    return Expression(gateway.Expressions.concat(
        get_java_expression(first),
        to_jarray(gateway.jvm.Object, [get_java_expression(other) for other in others])))


def lit(v):
    gateway = get_gateway()
    return Expression(gateway.jvm.Expressions.lit(v))
