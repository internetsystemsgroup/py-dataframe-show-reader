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

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType
from pytest import raises

from dataframe_show_reader.dataframe_show_reader import show_output_to_df


class TestDFUtil:

    def test_show_output_to_df_when_data_types_not_specified(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
        +-------------+----------+------------+-------------------+
        |string_column|int_column|float_column|timestamp_column   |
        +-------------+----------+------------+-------------------+
        # This is a comment that gets ignored.
        |one          |1         |1.1         |2018-01-01 00:00:00| # Another comment to ignore
        |two          |2         |2.2         |2018-01-02 12:34:56|
        +-------------+----------+------------+-------------------+
        """, spark_session).collect()
        assert 2 == len(rows)
        assert 4 == len(rows[0])  # Number of columns

        row = rows[0]
        assert 'one'                 == row['string_column']
        assert '1'                   == row['int_column']
        assert '1.1'                 == row['float_column']
        assert '2018-01-01 00:00:00' == row['timestamp_column']

        row = rows[1]
        assert 'two'                 == row['string_column']
        assert '2'                   == row['int_column']
        assert '2.2'                 == row['float_column']
        assert '2018-01-02 12:34:56' == row['timestamp_column']

    def test_show_output_to_df_when_row_delimiters_not_present(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
        |string_column|int_column|float_column|timestamp_column   |
        |one          |1         |1.1         |2018-01-01 00:00:00|
        |two          |2         |2.2         |2018-01-02 12:34:56|
        """, spark_session).collect()
        assert 2 == len(rows)
        assert 4 == len(rows[0])  # Number of columns

        row = rows[0]
        assert 'one'                 == row['string_column']
        assert '1'                   == row['int_column']
        assert '1.1'                 == row['float_column']
        assert '2018-01-01 00:00:00' == row['timestamp_column']

        row = rows[1]
        assert 'two'                 == row['string_column']
        assert '2'                   == row['int_column']
        assert '2.2'                 == row['float_column']
        assert '2018-01-02 12:34:56' == row['timestamp_column']

    def test_show_output_to_df_when_data_types_specified(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
        +-------------+----------+-------------+------------+-------------------+-------------------+-----------+
        |string_column|int_column|bigint_column|float_column|timestamp_column   |default_type_column|bool_column|
        [string       |int       |bigint       |float       |timestamp          |                   |boolean    ]
        +-------------+----------+-------------+------------+-------------------+-------------------+-----------+
        |one          |1         |1            |1.1         |2018-01-01 00:00:00|11                 |true       |
        |two          |2         |2            |2.2         |2018-01-02 12:34:56|22                 |false      |
        +-------------+----------+-------------+------------+-------------------+-------------------+-----------+
        """, spark_session).collect()
        assert 2 == len(rows)
        assert 7 == len(rows[0])  # Number of columns

        row = rows[0]
        assert 'one'                            == row['string_column']
        assert 1                                == row['int_column']
        assert 1                                == row['bigint_column']
        assert 1.1                              == row['float_column']
        assert datetime(2018, 1, 1)             == row['timestamp_column']
        assert '11'                             == row['default_type_column']
        assert True                             is row['bool_column']

        row = rows[1]
        assert 'two'                            == row['string_column']
        assert 2                                == row['int_column']
        assert 2                                == row['bigint_column']
        assert 2.2                              == row['float_column']
        assert datetime(2018, 1, 2, 12, 34, 56) == row['timestamp_column']
        assert '22'                             == row['default_type_column']
        assert False                            is row['bool_column']

    def test_show_output_to_df_with_multiple_data_type_declarations(
            self,
            spark_session: SparkSession
    ):
        with raises(ValueError) as exception_info:
            show_output_to_df("""
            +-------------+
            |string_column|
            [string       ]
            [string       ]
            +-------------+
            |one          |
            +-------------+
            """, spark_session)
        assert ('Cannot have more than one data type declaration line.' ==
                str(exception_info.value))

    def test_show_output_to_df_when_default_data_type_specified(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
        +----------+
        |int_column|
        +----------+
        |1         |
        +----------+
        """, spark_session, default_data_type='int').collect()
        assert 1 == len(rows)
        assert 1 == len(rows[0])  # Number of columns
        assert 1 == rows[0]['int_column']

    def test_show_output_to_df_when_data_does_not_match_declared_data_type(
            self,
            spark_session: SparkSession
    ):
        with raises(ValueError) as exception_info:
            show_output_to_df("""
            +----------+
            |int_column|
            [int       ]
            +----------+
            |          | # Empty string is not a valid INT
            +----------+
            """, spark_session).collect()
        assert ("invalid literal for int() with base 10: ''" ==
                str(exception_info.value))

    def test_show_output_to_df_when_data_does_not_match_default_data_type(
            self,
            spark_session: SparkSession
    ):
        with raises(ValueError) as exception_info:
            show_output_to_df("""
            +----------+
            |int_column|
            +----------+
            |          | # Empty string is not a valid INT
            +----------+
            """, spark_session, default_data_type='int').collect()
        assert ("invalid literal for int() with base 10: ''" ==
                str(exception_info.value))

    def test_show_output_to_df_when_default_data_type_overridden(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
        +-------------+----------+
        |string_column|int_column|
        [string       |          ]
        +-------------+----------+
        |1            |1         |
        +-------------+----------+
        """, spark_session, default_data_type='int').collect()
        assert 1 == len(rows)
        assert 2 == len(rows[0])  # Number of columns
        assert '1' == rows[0]['string_column']
        assert 1 == rows[0]['int_column']

    def test_show_output_to_df_when_values_are_null_and_data_types_not_specified(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
        +-------------+
        |string_column|
        +-------------+
        |null         |
        +-------------+
        """, spark_session).collect()
        assert 1 == len(rows)
        assert 1 == len(rows[0])  # Number of columns

        row = rows[0]
        assert None is row['string_column']

    def test_show_output_to_df_when_values_are_null_and_data_types_are_specified(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
        +-------------+----------+-------------+------------+----------------+-------------------+-----------+
        |string_column|int_column|bigint_column|float_column|timestamp_column|default_type_column|bool_column|
        [string       |int       |bigint       |float       |timestamp       |                   |boolean    ]
        +-------------+----------+-------------+------------+----------------+-------------------+-----------+
        |null         |null      |null         |null        |null            |null               |null       |
        +-------------+----------+-------------+------------+----------------+-------------------+-----------+
        """, spark_session).collect()
        assert 1 == len(rows)
        assert 7 == len(rows[0])  # Number of columns

        row = rows[0]
        assert None is row['string_column']
        assert None is row['int_column']
        assert None is row['bigint_column']
        assert None is row['float_column']
        assert None is row['timestamp_column']
        assert None is row['default_type_column']
        assert None is row['bool_column']

    def test_show_output_to_df_when_values_are_empty_and_data_types_are_specified(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
        +-------------+-------------------+
        |string_column|default_type_column|
        [string       |                   |
        +-------------+-------------------+
        |             |                   |
        +-------------+-------------------+
        """, spark_session).collect()
        assert 1 == len(rows)
        assert 2 == len(rows[0])  # Number of columns

        row = rows[0]
        assert '' == row['string_column']
        assert '' == row['default_type_column']

    def test_show_output_to_df_with_1d_arrays(self,
                                              spark_session: SparkSession):
        df = show_output_to_df("""
        +----+-------------+-------------+---------------+----------------+
        |id  |array_str_col|array_int_col|array_float_col|array_double_col|
        [int |array<string>|array<int>   |array<float>   |array<double>   |
        +----+-------------+-------------+---------------+----------------+
        |1   |[ a ,b, c ]  |[0,  1 ,2 ]  |[0, 1, 2.0]    |[0, 1, 2.0]     |
        |2   |[  ]         |[     ]      |[]             |[]              |
        +----+-------------+-------------+---------------+----------------+
        """, spark_session)
        rows = df.collect()
        assert 2 == len(rows)
        assert 5 == len(rows[0])  # Number of columns

        row = rows[0]
        assert 1               == row['id']
        assert ['a', 'b', 'c'] == row['array_str_col']
        assert [0, 1, 2]       == row['array_int_col']
        assert [0.0, 1.0, 2.0] == row['array_float_col']
        assert [0.0, 1.0, 2.0] == row['array_double_col']

        row = rows[1]
        assert 2               == row['id']
        assert []              == row['array_str_col']
        assert []              == row['array_int_col']
        assert []              == row['array_float_col']
        assert []              == row['array_double_col']

    def test_show_output_to_df_with_2d_arrays(self,
                                              spark_session: SparkSession):
        df = show_output_to_df("""
        +----+----------------------+-------------------+---------------------+-----------------------+
        |id  |array_array_str_col   |array_array_int_col|array_array_float_col|array_array_double_col |
        [int |array<array<string>>  |array<array<int>>  |array<array<float>>  |array<array<double>>   |
        +----+----------------------+-------------------+---------------------+-----------------------+
        |1   |[[ a , b, c ],[d],[]] |[[ 1 , 2 ],[-1],[]]|[[ 1 , 2 ],[-1.0],[]]|[[ -1 , 2 ],[ 1.0 ],[]]|
        |2   |[]                    |[]                 |[]                   |[]                     |
        |3   |[[]]                  |[[],[]]            |[[],[],[]]           |[ [] ]                 |
        +----+----------------------+-------------------+---------------------+-----------------------+
        """, spark_session)
        rows = df.collect()
        assert 3 == len(rows)
        assert 5 == len(rows[0])  # Number of columns

        row = rows[0]
        assert 1                            == row['id']
        assert [['a', 'b', 'c'], ['d'], []] == row['array_array_str_col']
        assert [[1, 2], [-1], []]           == row['array_array_int_col']
        assert [[1.0, 2.0], [-1.0], []]     == row['array_array_float_col']
        assert [[-1.0, 2.0], [1.0], []]     == row['array_array_double_col']

        row = rows[1]
        assert 2                            == row['id']
        assert [[]]                         == row['array_array_str_col']
        assert [[]]                         == row['array_array_int_col']
        assert [[]]                         == row['array_array_float_col']
        assert [[]]                         == row['array_array_double_col']

        row = rows[2]
        assert 3                            == row['id']
        assert [[]]                         == row['array_array_str_col']
        assert [[], []]                     == row['array_array_int_col']
        assert [[], [], []]                 == row['array_array_float_col']
        assert [[]]                         == row['array_array_double_col']

    def test_show_output_to_df_with_wrapped_arrays(self,
                                                   spark_session: SparkSession):
        df = show_output_to_df("""
        +---+--------------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------------+-----------------------------------------------------------+
        |id |array_array_str_col                                     |array_array_int_col                                  |array_array_float_col                                      |array_array_double_col                                     |
        [int|array<array<string>>                                    |array<array<int>>                                    |array<array<float>>                                        |array<array<double>>                                       |
        +---+--------------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------------+-----------------------------------------------------------+
        |1  |[WrappedArray(a, b, c), WrappedArray(d), WrappedArray()]|[WrappedArray(1, 2), WrappedArray(1), WrappedArray()]|[WrappedArray(1.0, 2.0), WrappedArray(1.0), WrappedArray()]|[WrappedArray(1.0, 2.0), WrappedArray(1.0), WrappedArray()]|
        |2  |[WrappedArray()]                                        |[WrappedArray()]                                     |[WrappedArray()]                                           |[WrappedArray()]                                           |
        |3  |[WrappedArray()]                                        |[WrappedArray(), WrappedArray()]                     |[WrappedArray(), WrappedArray(), WrappedArray()]           |[WrappedArray()]                                           |
        +---+--------------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------------+-----------------------------------------------------------+
        """, spark_session)
        rows = df.collect()
        assert 3 == len(rows)
        assert 5 == len(rows[0])  # Number of columns

        row = rows[0]
        assert 1                            == row['id']
        assert [['a', 'b', 'c'], ['d'], []] == row['array_array_str_col']
        assert [[1, 2], [1], []]            == row['array_array_int_col']
        assert [[1.0, 2.0], [1.0], []]      == row['array_array_float_col']
        assert [[1.0, 2.0], [1.0], []]      == row['array_array_double_col']

        row = rows[1]
        assert 2                            == row['id']
        assert [[]]                         == row['array_array_str_col']
        assert [[]]                         == row['array_array_int_col']
        assert [[]]                         == row['array_array_float_col']
        assert [[]]                         == row['array_array_double_col']

        row = rows[2]
        assert 3                            == row['id']
        assert [[]]                         == row['array_array_str_col']
        assert [[], []]                     == row['array_array_int_col']
        assert [[], [], []]                 == row['array_array_float_col']
        assert [[]]                         == row['array_array_double_col']

    def test_show_output_to_df_with_no_data_rows_and_data_types_not_specified(
            self,
            spark_session: SparkSession
    ):
        df = show_output_to_df("""
        +-------------+
        |string_column|
        +-------------+
        +-------------+
        """, spark_session)
        assert DataFrame == type(df)
        assert 1 == len(df.columns)
        assert 0 == df.count()
        assert isinstance(df.schema.fields[0].dataType, StringType)

    def test_show_output_to_df_with_no_data_rows_and_data_types_specified(
            self,
            spark_session: SparkSession
    ):
        df = show_output_to_df("""
        +----------+
        |int_column|
        [int       ]
        +----------+
        +----------+
        """, spark_session)
        assert DataFrame == type(df)
        assert 1 == len(df.columns)
        assert 0 == df.count()
        assert isinstance(df.schema.fields[0].dataType, IntegerType)
