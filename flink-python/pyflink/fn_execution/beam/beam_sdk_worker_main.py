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
import logging
import os
import sys

# force to register the operations to SDK Harness
try:
    import pyflink.fn_execution.beam.beam_operations_fast
except ImportError:
    import pyflink.fn_execution.beam.beam_operations_slow

# resource is only available in Unix
try:
    import resource
    has_resource_module = True
except ImportError:
    has_resource_module = False

# force to register the coders to SDK Harness
import pyflink.fn_execution.beam.beam_coders # noqa # pylint: disable=unused-import

import apache_beam.runners.worker.sdk_worker_main


def set_memory_limit():
    memory_limit = int(os.environ.get('_PYTHON_WORKER_MEMORY_LIMIT', "-1"))
    if memory_limit > 0 and has_resource_module:
        try:
            (soft_limit, hard_limit) = resource.getrlimit(resource.RLIMIT_RSS)

            # hard limit can only be lowered, but not raised
            if soft_limit == resource.RLIM_INFINITY or memory_limit < hard_limit:
                logging.info("Setting memory limit to (%s, %s)" % (memory_limit, memory_limit))
                resource.setrlimit(resource.RLIMIT_RSS, (memory_limit, memory_limit))
        except Exception as e:
            logging.warning("Failed to set memory limit: %s", e)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    set_memory_limit()
    apache_beam.runners.worker.sdk_worker_main.main(sys.argv)
