# Copyright 2019 The DataFrame Show Reader Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType


def assert_equal(expected: DataFrame,
                 actual: DataFrame,
                 verbose: bool = True,
                 ignore_schema_metadata: bool = True):
    """Assert that two DataFrames contain the same data.
    :param expected: The expected DataFrame
    :param actual: The actual DataFrame
    :param verbose: If the DataFrames are not equal, show
    the DataFrame schema or data (depending on where the mismatch is) in order
    to help debugging.
    :param ignore_schema_metadata: When comparing the schemas, ignore the
    metadata, which can include comments.
    # comments.
    :return: None
    """
    if expected is None:
        assert actual is None, \
            'The expected DataFrame is None, but the actual DataFrame is not.'
        return  # Both DataFrames are None.
    else:
        assert actual is not None, \
            'The actual DataFrame is None, but the expected DataFrame is not.'

    expected.persist()
    actual.persist()

    expected_schema = _copy_without_metadata(expected.schema) if \
        ignore_schema_metadata else expected.schema
    actual_schema = _copy_without_metadata(actual.schema) if \
        ignore_schema_metadata else actual.schema

    is_schema_match = expected_schema == actual_schema
    if not is_schema_match:
        if verbose:
            # Print the schema to help identify subtle cases such as one DF
            # containing an INT and the other containing a BIGINT, which can be
            # an issue if we try to write a DF containing a BIGINT into a
            # previously existing Hive table defined to contain an INT.
            print('Expected DataFrame schema:')
            expected.printSchema()
            print('Actual DataFrame schema:')
            actual.printSchema()
        assert is_schema_match is True, 'The DataFrame schemas differ.'

    is_match = (0 == (expected.count() - actual.count()) ==
                expected.subtract(actual).count() ==
                actual.subtract(expected).count())

    if not is_match:
        if verbose:
            print('Expected DataFrame:')
            expected.show(20, False)
            print('Actual DataFrame:')
            actual.show(20, False)
        # Possible future enhancement: Make the assertion failure message more
        # helpful.
        assert is_match is True, 'The DataFrames differ.'


def _copy_without_metadata(schema: StructType):
    return StructType(
        [StructField(f.name, f.dataType, f.nullable) for f in schema]
    )
