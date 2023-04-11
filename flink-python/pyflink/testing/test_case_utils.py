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
#################################################################################
import glob
import logging
import os
import shutil
import sys
import tempfile
import unittest
from abc import abstractmethod

from py4j.java_gateway import JavaObject

from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.find_flink_home import _find_flink_home, _find_flink_source_root
from pyflink.java_gateway import get_gateway
from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.util.java_utils import add_jars_to_context_class_loader

if os.getenv("VERBOSE"):
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
logging.basicConfig(stream=sys.stdout, level=log_level,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def _load_specific_flink_module_jars(jars_relative_path):
    flink_source_root = _find_flink_source_root()
    jars_abs_path = flink_source_root + jars_relative_path
    specific_jars = glob.glob(jars_abs_path + '/target/flink*.jar')
    specific_jars = ['file://' + specific_jar for specific_jar in specific_jars]
    add_jars_to_context_class_loader(specific_jars)


class PyFlinkTestCase(unittest.TestCase):
    """
    Base class for unit tests.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

        os.environ["FLINK_TESTING"] = "1"
        os.environ['_python_worker_execution_mode'] = "process"
        _find_flink_home()

        logging.info("Using %s as FLINK_HOME...", os.environ["FLINK_HOME"])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)
        del os.environ['_python_worker_execution_mode']

    @classmethod
    def assert_equals(cls, actual, expected):
        if isinstance(actual, JavaObject):
            actual_py_list = cls.to_py_list(actual)
        else:
            actual_py_list = actual
        actual_py_list.sort()
        expected.sort()
        assert len(actual_py_list) == len(expected)
        assert all(x == y for x, y in zip(actual_py_list, expected))

    @classmethod
    def to_py_list(cls, actual):
        py_list = []
        for i in range(0, actual.size()):
            py_list.append(actual.get(i))
        return py_list


class PyFlinkITTestCase(PyFlinkTestCase):

    @classmethod
    def setUpClass(cls):
        super(PyFlinkITTestCase, cls).setUpClass()
        gateway = get_gateway()
        MiniClusterResourceConfiguration = (
            gateway.jvm.org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
            .Builder()
            .setNumberTaskManagers(8)
            .setNumberSlotsPerTaskManager(1)
            .setRpcServiceSharing(
                get_gateway().jvm.org.apache.flink.runtime.minicluster.RpcServiceSharing.DEDICATED)
            .withHaLeadershipControl()
            .build())
        cls.resource = (
            get_gateway().jvm.org.apache.flink.test.util.
            MiniClusterWithClientResource(MiniClusterResourceConfiguration))
        cls.resource.before()

        cls.env = StreamExecutionEnvironment(
            get_gateway().jvm.org.apache.flink.streaming.util.TestStreamEnvironment(
                cls.resource.getMiniCluster(), 2))

    @classmethod
    def tearDownClass(cls):
        super(PyFlinkITTestCase, cls).tearDownClass()
        cls.resource.after()


class PyFlinkUTTestCase(PyFlinkTestCase):
    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.set_parallelism(2)
        self.t_env = StreamTableEnvironment.create(self.env)
        self.t_env.get_config().set("python.fn-execution.bundle.size", "1")


class PyFlinkStreamingTestCase(PyFlinkITTestCase):
    """
    Base class for streaming tests.
    """

    @classmethod
    def setUpClass(cls):
        super(PyFlinkStreamingTestCase, cls).setUpClass()
        cls.env.set_parallelism(2)
        cls.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)


class PyFlinkBatchTestCase(PyFlinkITTestCase):
    """
    Base class for batch tests.
    """

    @classmethod
    def setUpClass(cls):
        super(PyFlinkBatchTestCase, cls).setUpClass()
        cls.env.set_parallelism(2)
        cls.env.set_runtime_mode(RuntimeExecutionMode.BATCH)


class PyFlinkStreamTableTestCase(PyFlinkITTestCase):
    """
    Base class for table stream tests.
    """

    @classmethod
    def setUpClass(cls):
        super(PyFlinkStreamTableTestCase, cls).setUpClass()
        cls.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        cls.env.set_parallelism(2)
        cls.t_env = StreamTableEnvironment.create(cls.env)
        cls.t_env.get_config().set("python.fn-execution.bundle.size", "1")


class PyFlinkBatchTableTestCase(PyFlinkITTestCase):
    """
    Base class for table batch tests.
    """

    @classmethod
    def setUpClass(cls):
        super(PyFlinkBatchTableTestCase, cls).setUpClass()
        cls.env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        cls.env.set_parallelism(2)
        cls.t_env = StreamTableEnvironment.create(cls.env)
        cls.t_env.get_config().set("python.fn-execution.bundle.size", "1")


class PythonAPICompletenessTestCase(object):
    """
    Base class for Python API completeness tests, i.e.,
    Python API should be aligned with the Java API as much as possible.
    """

    @classmethod
    def get_python_class_methods(cls, python_class):
        return {cls.snake_to_camel(cls.java_method_name(method_name))
                for method_name in dir(python_class) if not method_name.startswith('_')}

    @staticmethod
    def snake_to_camel(method_name):
        output = ''.join(x.capitalize() or '_' for x in method_name.split('_'))
        return output[0].lower() + output[1:]

    @staticmethod
    def get_java_class_methods(java_class):
        gateway = get_gateway()
        s = set()
        method_arr = gateway.jvm.Class.forName(java_class).getMethods()
        for i in range(0, len(method_arr)):
            s.add(method_arr[i].getName())
        return s

    @classmethod
    def check_methods(cls):
        java_primary_methods = {'getClass', 'notifyAll', 'equals', 'hashCode', 'toString',
                                'notify', 'wait'}
        java_methods = PythonAPICompletenessTestCase.get_java_class_methods(cls.java_class())
        python_methods = cls.get_python_class_methods(cls.python_class())
        missing_methods = java_methods - python_methods - cls.excluded_methods() \
            - java_primary_methods
        if len(missing_methods) > 0:
            raise Exception('Methods: %s in Java class %s have not been added in Python class %s.'
                            % (missing_methods, cls.java_class(), cls.python_class()))

    @classmethod
    def java_method_name(cls, python_method_name):
        """
        This method should be overwritten when the method name of the Python API cannot be
        consistent with the Java API method name. e.g.: 'as' is python
        keyword, so we use 'alias' in Python API corresponding 'as' in Java API.

        :param python_method_name: Method name of Python API.
        :return: The corresponding method name of Java API.
        """
        return python_method_name

    @classmethod
    @abstractmethod
    def python_class(cls):
        """
        Return the Python class that needs to be compared. such as :class:`Table`.
        """
        pass

    @classmethod
    @abstractmethod
    def java_class(cls):
        """
        Return the Java class that needs to be compared. such as `org.apache.flink.table.api.Table`.
        """
        pass

    @classmethod
    def excluded_methods(cls):
        """
        Exclude method names that do not need to be checked. When adding excluded methods
        to the lists you should give a good reason in a comment.
        :return:
        """
        return {"equals", "hashCode", "toString"}

    def test_completeness(self):
        self.check_methods()

