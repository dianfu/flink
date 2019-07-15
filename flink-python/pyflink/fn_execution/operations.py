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
from apache_beam.runners.common import Receiver, _OutputProcessor
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker.operations import Operation

from pyflink.fn_execution.common import ScalarFunctionSignature, ScalarFunctionInvoker
from pyflink.fn_execution import flink_fn_execution_pb2

SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1"


class ScalarFunctionRunner(Receiver):
    """
    ScalarFunctionRunner. It executes a list of scalar functions.

    :param serialized_scalar_functions: serialized scalar functions to execute
    :param main_receivers: a list of Receiver objects
    :param step_name: the name of this step
    """

    def __init__(self, serialized_scalar_functions, main_receivers=None, step_name=None):
        self.step_name = step_name

        self.output_processor = _OutputProcessor(
            window_fn=None,
            main_receivers=main_receivers,
            tagged_receivers=None,
            per_element_output_counter=None)

        self.scalar_function_invokers = []
        for serialized_scalar_function in serialized_scalar_functions:
            import cloudpickle
            scalar_function = cloudpickle.loads(serialized_scalar_function.payload)
            input_offsets = [i.inputOffset for i in serialized_scalar_function.inputs]
            self.scalar_function_invokers.append(
                ScalarFunctionInvoker(ScalarFunctionSignature(scalar_function, input_offsets)))

    def open(self):
        for invoker in self.scalar_function_invokers:
            invoker.invoke_open()

    def close(self):
        for invoker in self.scalar_function_invokers:
            invoker.invoke_close()

    def start(self):
        pass

    def finish(self):
        pass

    def receive(self, windowed_value):
        self.process(windowed_value)

    def process(self, windowed_value):
        try:
            results = [invoker.invoke_eval(windowed_value) for invoker in
                       self.scalar_function_invokers]
            from pyflink.table import Row
            result = Row(*results)
            self.output_processor.process_outputs(windowed_value, [result])
        except BaseException as exn:
            self._reraise_augmented(exn)

    def _invoke_bundle_method(self, bundle_method):
        try:
            self.context.set_element(None)
            bundle_method()
        except BaseException as exn:
            self._reraise_augmented(exn)

    def finalize(self):
        self.bundle_finalizer_param.finalize_bundle()

    def _reraise_augmented(self, exn):
        if getattr(exn, '_tagged_with_step', False) or not self.step_name:
            raise exn
        step_annotation = " [while running '%s']" % self.step_name
        # To emulate exception chaining (not available in Python 2).
        try:
            # Attempt to construct the same kind of exception
            # with an augmented message.
            new_exn = type(exn)(exn.args[0] + step_annotation, *exn.args[1:])
            new_exn._tagged_with_step = True  # Could raise attribute error.
        except:  # pylint: disable=bare-except
            # If anything goes wrong, construct a RuntimeError whose message
            # records the original exception's type and message.
            import traceback
            new_exn = RuntimeError(
                traceback.format_exception_only(type(exn), exn)[-1].strip()
                + step_annotation)
            new_exn._tagged_with_step = True
        from apache_beam.runners.common import raise_with_traceback
        raise_with_traceback(new_exn)


class ScalarFunctionOperation(Operation):
    """
    An operation that will execute ScalarFunctions for each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(ScalarFunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        self.tagged_receivers = None
        self.step_name = name
        for tag, op_consumers in consumers.items():
            for consumer in op_consumers:
                self.add_receiver(consumer, 0)

    def setup(self):
        with self.scoped_start_state:
            super(ScalarFunctionOperation, self).setup()

            self.scalar_function_runner = ScalarFunctionRunner(
                self.spec.serialized_fn,
                main_receivers=self.receivers[0],
                step_name=self.name_context.logging_name())

            self.scalar_function_runner.open()

    def start(self):
        with self.scoped_start_state:
            super(ScalarFunctionOperation, self).start()
            self.scalar_function_runner.start()

    def process(self, o):
        with self.scoped_process_state:
            self.scalar_function_runner.receive(o)

    def finish(self):
        with self.scoped_finish_state:
            super(ScalarFunctionOperation, self).finish()
            self.scalar_function_runner.finish()

    def needs_finalization(self):
        return False

    def reset(self):
        super(ScalarFunctionOperation, self).reset()

    def progress_metrics(self):
        metrics = super(ScalarFunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
        return metrics

    def monitoring_infos(self, transform_id):
        return super(ScalarFunctionOperation, self).monitoring_infos(transform_id)


@bundle_processor.BeamTransformFactory.register_urn(
    SCALAR_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter.udfs)


def _create_user_defined_function_operation(factory, transform_proto, consumers, udfs,
                                            operation_cls=ScalarFunctionOperation):
    output_tags = list(transform_proto.outputs.keys())
    output_coders = factory.get_output_coders(transform_proto)
    spec = operation_specs.WorkerDoFn(
        serialized_fn=udfs,
        output_tags=output_tags,
        input=None,
        side_inputs=None,
        output_coders=[output_coders[tag] for tag in output_tags])

    return operation_cls(
        transform_proto.unique_name,
        spec,
        factory.counter_factory,
        factory.state_sampler,
        consumers)
