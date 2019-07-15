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
from apache_beam.coders.coder_impl import StreamCoderImpl


class RowCoderImpl(StreamCoderImpl):

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def encode_to_stream(self, value, out_stream, nested):
        for i in range(len(self._field_coders)):
            self._field_coders[i].encode_to_stream(value[i], out_stream, nested)

    def decode_from_stream(self, in_stream, nested):
        from pyflink.table import Row
        return Row(*[f.decode_from_stream(in_stream, nested) for f in self._field_coders])

    def __repr__(self):
        return 'RowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)


class BigIntCoderImpl(StreamCoderImpl):

    def encode_to_stream(self, value, out_stream, nested):
        out_stream.write_var_int64(value)

    def decode_from_stream(self, in_stream, nested):
        v = in_stream.read_var_int64()
        return v

    def __repr__(self):
        return 'BigIntCoder'
