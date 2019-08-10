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
from pyspark.sql import SparkSession

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
            |one          |1         |1.1         |2018-01-01 00:00:00|
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
        assert True                             == row['bool_column']

        row = rows[1]
        assert 'two'                            == row['string_column']
        assert 2                                == row['int_column']
        assert 2                                == row['bigint_column']
        assert 2.2                              == row['float_column']
        assert datetime(2018, 1, 2, 12, 34, 56) == row['timestamp_column']
        assert '22'                             == row['default_type_column']
        assert False                            == row['bool_column']

    def test_show_output_to_df_when_values_are_null(
            self,
            spark_session: SparkSession
    ):
        rows = show_output_to_df("""
            +-------------+----------+-------------+------------+-------------------+-------------------+-----------+
            |string_column|int_column|bigint_column|float_column|timestamp_column   |default_type_column|bool_column|
            [string       |int       |bigint       |float       |timestamp          |                   |boolean    ]
            +-------------+----------+-------------+------------+-------------------+-------------------+-----------+
            |null         |null      |null         |null        |null               |null               |null       |
            +-------------+----------+-------------+------------+-------------------+-------------------+-----------+
        """, spark_session).collect()
        assert 1 == len(rows)
        assert 7 == len(rows[0])  # Number of columns

        row = rows[0]
        assert None == row['string_column']
        assert None == row['int_column']
        assert None == row['bigint_column']
        assert None == row['float_column']
        assert None == row['timestamp_column']
        assert None == row['default_type_column']
        assert None == row['bool_column']

    def test_show_output_to_df_with_arrays(self,
                                           spark_session: SparkSession):
        df = show_output_to_df("""
                    +----+--------------------+-------------+----------------+----------------+----------------------+-------------------------------+
                    |id  |array_str_col       |array_int_col|array_float_col |array_double_col|array_array_float_col |array_array_str_col            |
                    [int |array_str           |array_int    |array_float     |array_double    |array_array_float     |array_array_str                ]
                    +----+--------------------+-------------+----------------+----------------+----------------------+-------------------------------+
                    |1   |[ hi ,bye, so long ]|[0,  1 ,2 ]  |[0, 1, 2.0]     |[0, 1, 2.0]     |[[1, 2],[1.0],[]]     |[[ hi , bye, so long ],[yo],[]]|
                    |2   |[  ]                |[     ]      |[]              |[]              |[]                    |[]                             |
                    +----+--------------------+-------------+----------------+----------------+----------------------+-------------------------------+
                """, spark_session)
        rows = df.collect()
        assert 2 == len(rows)
        assert 7 == len(rows[0])  # Number of columns

        assert 1                                  == rows[0]['id']
        assert ['hi','bye','so long']             == rows[0]['array_str_col']
        assert [0,1,2]                            == rows[0]['array_int_col']
        assert [0.0, 1.0, 2.0]                    == rows[0]['array_float_col']
        assert [0.0, 1.0, 2.0]                    == rows[0]['array_double_col']
        assert [[1.0, 2.0],[1.0],[]]              == rows[0]['array_array_float_col']
        assert [['hi','bye','so long'],['yo'],[]] == rows[0]['array_array_str_col']

        assert 2                     == rows[1]['id']
        assert []                    == rows[1]['array_str_col']
        assert []                    == rows[1]['array_int_col']
        assert []                    == rows[1]['array_float_col']
        assert []                    == rows[1]['array_double_col']
        assert [[]]                  == rows[1]['array_array_float_col']
        assert [[]]                  == rows[1]['array_array_str_col']

    def test_show_output_to_df_with_wrapped_arrays(self,
                                                   spark_session: SparkSession):
        df = show_output_to_df("""
                +---+-----------------------------------------------------------+------------------------------------------------------------------+
                |id |array_array_float_col                                      |array_array_str_col                                               |
                [int|array_array_float                                          |array_array_str                                                   |
                +---+-----------------------------------------------------------+------------------------------------------------------------------+
                |1  |[WrappedArray(1.0, 2.0), WrappedArray(1.0), WrappedArray()]|[WrappedArray(hi, bye, so long), WrappedArray(yo), WrappedArray()]|
                |2  |[WrappedArray()]                                           |[WrappedArray()]                                                  |
                +---+-----------------------------------------------------------+------------------------------------------------------------------+
                """, spark_session)
        rows = df.collect()
        assert 2 == len(rows)
        assert 3 == len(rows[0])  # Number of columns

        assert 1                                  == rows[0]['id']
        assert [[1.0, 2.0],[1.0],[]]              == rows[0]['array_array_float_col']
        assert [['hi','bye','so long'],['yo'],[]] == rows[0]['array_array_str_col']

        assert 2                     == rows[1]['id']
        assert [[]]                  == rows[1]['array_array_float_col']
        assert [[]]                  == rows[1]['array_array_str_col']