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

import re
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Row
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType, LongType,
                               StringType, StructType, StructField, TimestampType)

DATA_TYPE_START_INDICATOR = '['
DATA_TYPE_END_INDICATOR = ']'

ARRAY_ELEMENT_START_INDICATOR = '['
ARRAY_ELEMENT_END_INDICATOR = ']'


def show_output_to_df(
        show_output: str,
        spark_session: SparkSession,
        default_data_type: str = 'string'
):
    """
    Takes a string containing the output of a Spark DataFrame.show() call and
    "rehydrates" it into a new Spark DataFrame instance. Example input:
    +--------+--------+
    |column_a|column_b|
    +--------+--------+
    |value 1a|value 1b|
    |value 2a|value 2b|
    +--------+--------+

    Optionally, row delimiters can be omitted, and comment lines can be present
    (whether or not row delimiters are provided):
    |column_a|column_b|
    |value 1a|value 1b|
    # This is a comment that gets ignored.
    |value 2a|value 2b|

    Optionally, data types can be specified in a second header line, prefixed
    with the DATA_TYPE_START_INDICATOR ("["):
    +-------------+----------+------------+-------------------+-----------+
    |string_column|int_column|float_column|timestamp_column   |bool_column|
    [string       |int       |float       |timestamp          |boolean    ]
    +-------------+----------+------------+-------------------+-----------+
    |one          |1         |1.1         |2018-01-01 00:00:00|true       |
    |two          |2         |2.2         |2018-01-02 12:34:56|false      |
    +-------------+----------+------------+-------------------+-----------+

    :param show_output: A string that resembles the output of a call to
           DataFrame.show()
    :param spark_session: A SparkSession used to create the new DataFrame instance
    :param default_data_type: The default data type that will be used for all
           columns for which the data type is not specified in the data type
           declaration line
    :return: A DataFrame containing the values represented in the input string
    """
    if not show_output:
        raise ValueError('show_output is required.')

    rows = []
    column_names = None
    types = None
    # Added a schema because createDataFrame() does introspection otherwise and
    # sometimes gets it wrong with int/bigint and nulls.
    schema = None
    for line in show_output.strip().splitlines():
        line = line.strip()
        if not line.startswith(tuple(f'|{DATA_TYPE_START_INDICATOR}')):
            continue
        line_parts = line.split('|')[1:-1]
        values = [part.strip() for part in line_parts]
        if column_names is None:
            column_names = values
            continue
        if line.startswith(DATA_TYPE_START_INDICATOR):
            if types is None:
                line = line.replace(DATA_TYPE_START_INDICATOR, '|', 1)\
                           .rstrip(f'{DATA_TYPE_END_INDICATOR}|') + '|'
                types = [part.strip() for part in line.split('|')[1:-1]]
                types = [data_type if len(data_type) > 0 else default_data_type
                         for data_type in types]
                continue
            else:
                raise ValueError('Cannot have more than one data type declaration line.')

        if types is None:
            types = [default_data_type] * len(column_names)

        _cast_types(values, types)
        row_dict = dict(zip(column_names, values))
        rows.append(Row(**row_dict))

    if types is None:
        # This can happen if data types are not specified and no data rows are
        # provided.
        types = [default_data_type] * len(column_names)
    schema = _get_schema(column_names, types)

    # Return a DataFrame with the columns in the original order:
    return spark_session.createDataFrame(rows, schema=schema).select(column_names)


def save_as_table(table_name: str,
                  show_output: str,
                  spark_session: SparkSession,
                  partition_cols: list = []):
    """
    Takes a string containing the output of a Spark DataFrame.show() call and
    "rehydrates" it into a new Spark DataFrame instance, which it then persists
    to a Hive table.
    :param table_name: The name of the Hive table to which the DataFrame will be
           persisted. Should be in "<database_name>.<table_name>" format
           (e.g., "my_db.my_table").
    :param show_output: A string that resembles the output of a call to
           DataFrame.show()
    :param spark_session: A SparkSession used to create the new DataFrame instance
    :param partition_cols: Optional list of columns on which to partition the table
    :return: A DataFrame containing the values represented in the input string
    """
    df = show_output_to_df(show_output, spark_session)
    save_df_as_table(table_name, df, spark_session, partition_cols)
    return df


def save_df_as_table(table_name: str,
                     df: DataFrame,
                     spark_session: SparkSession,
                     partition_cols: list = []):
    """
    Persists a DataFrame to a Hive table.
    :param table_name: The name of the Hive table to which the DataFrame will be
           persisted. Should be in "<database_name>.<table_name>" format
           (e.g., "my_db.my_table").
    :param df: The DataFrame to be persisted
    :param spark_session: A SparkSession used to create the new DataFrame instance
    :param partition_cols: Optional list of columns on which to partition the table
    :return: None
    """
    df.repartition(1).write.partitionBy(partition_cols) \
        .saveAsTable(table_name, mode='Overwrite')
    spark_session.sql(f'REFRESH TABLE {table_name}')


def ts(timestamp_string: str):
    """
    Convert a DataFrame show output-style timestamp string into a datetime value
    which will marshall to a Hive/Spark TimestampType
    :param timestamp_string: A timestamp string in "YYYY-MM-DD HH:MM:SS" format
    :return: A datetime object
    """
    return datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S')


def array_string_to_list(the_string: str, data_type: type = type, splitter: str = ','):
    """
    Convert a DataFrame show output-style array string into a list value
    which will marshall to a Hive/Spark ArrayType
    :param the_string: A string in the format: '[val1, val2, val3]'
    :param data_type: The data_type of each element in the array.
    :param splitter: The char/string on which we split the str_array
    :return: A list containing elements of the type indicated by 'data_type'.
    """
    array_bounds = f'{ARRAY_ELEMENT_START_INDICATOR}{ARRAY_ELEMENT_END_INDICATOR}'
    list_of_strings = [x.strip() for x in the_string.strip().strip(array_bounds).split(splitter)]

    # Handle the empty list case cuz we can't cast an empty string to a number type.
    return ([]
            if len(list_of_strings) == 0 or (len(list_of_strings) == 1 and list_of_strings[0] == '')
            else [data_type(x) for x in list_of_strings])


def array_of_array_string_to_list(the_string: str, data_type: type = str):
    """
    Convert a DataFrame show output-style array of arrays string into a list of lists value
    which will marshall to a Hive/Spark ArrayType(ArrayType(data_type))
    :param the_string: A string in the format: '[[val1,val2], [val3,val4], [val5]]'
    or : '[WrappedArray(val1, val2), WrappedArray(val3), WrappedArray()]'
    :param data_type: The data_type of each element in the inner arrays.
    :return: A list object with elements of type: list of type 'data_type'.
    """
    # We need to remove the start & end characters.
    # Then replace the outer array's '],' with something other than a comma
    # so we don't split on the inner arrays' commas.
    #
    # So we change this: '[[val1,val2], [val3,val4], [val5]]'
    #                to:  '[val1,val2], [val3,val4], [val5]'
    #              then:  '[val1,val2]| [val3,val4]| [val5]'
    # Then split on '|' and convert each '[val1,val2]' to a list.
    #
    # Also, since df.show() prints out arrays-of-arrays as:
    #       [WrappedArray(val1, val2), WrappedArray(val3), WrappedArray()]
    # we'll convert this to: '[[val1,val2], [val3], []]' then handle normally.

    # first convert 'WrappedArray()' syntax to '[]'
    the_string = re.sub(r'WrappedArray\((.*?)\)', r'[\1]', the_string)

    new_separator = '|'
    inner_end_bound = f'{ARRAY_ELEMENT_END_INDICATOR},'
    new_inner_end_bound = f'{ARRAY_ELEMENT_END_INDICATOR}{new_separator}'

    return ([array_string_to_list(x, data_type)
             for x in the_string.strip()[1:-1]
                 .replace(inner_end_bound, new_inner_end_bound)
                 .split(new_separator)])


def int_array(the_string: str):
    return array_string_to_list(the_string, int)


def float_array(the_string: str):
    return array_string_to_list(the_string, float)


def str_array(the_string: str):
    return array_string_to_list(the_string, str)


def int_array_of_array(the_string: str):
    return array_of_array_string_to_list(the_string, int)


def float_array_of_array(the_string: str):
    return array_of_array_string_to_list(the_string, float)


def str_array_of_array(the_string: str):
    return array_of_array_string_to_list(the_string, str)


def _cast_types(values: list, types: list):
    for i, value in enumerate(values):
        values[i] = (None if value == 'null' else _get_cast_function(types[i])(value))


def _parse_boolean(boolean: str):
    if boolean.lower() == 'true':
        return True
    elif boolean.lower() == 'false':
        return False
    else:
        raise ValueError(f'"{boolean}" is not a recognized boolean value')


# By convention, the type map keys should be lowercase versions of Spark SQL
# keywords that would be valid in a CREATE TABLE statement.
_TYPE_MAP = {
    'bigint': (int, LongType()),
    'boolean': (_parse_boolean, BooleanType()),
    'double': (float, DoubleType()),
    'float': (float, DoubleType()),
    'int': (int, IntegerType()),
    'string': (str, StringType()),
    'timestamp': (ts, TimestampType()),
    'array<string>': (str_array, ArrayType(StringType())),
    'array<int>': (int_array, ArrayType(IntegerType())),
    'array<double>': (float_array, ArrayType(DoubleType())),
    'array<float>': (float_array, ArrayType(DoubleType())),
    'array<array<string>>': (str_array_of_array, ArrayType(ArrayType(StringType()))),
    'array<array<int>>': (int_array_of_array, ArrayType(ArrayType(IntegerType()))),
    'array<array<double>>': (float_array_of_array, ArrayType(ArrayType(DoubleType()))),
    'array<array<float>>': (float_array_of_array, ArrayType(ArrayType(DoubleType()))),
}


def _get_cast_function(data_type: str):
    data_type_lower = data_type.lower()
    if data_type_lower not in _TYPE_MAP:
        raise ValueError(f'Unrecognized data type "{data_type_lower}"')
    return _TYPE_MAP[data_type_lower][0]


def _get_schema(columns: list, types: list):
    """
    Get a schema that can be passed to createDataFrame()
    :param columns: a list of column names
    :param types: a list of types for those column names
    :return: a schema that can be passed to createDataFrame()
    """
    schema_columns = []

    # We need to create a dictionary, then sort it by column name
    # because createDataFrame() orders the columns and the
    # schema must match that order.

    # Create a dictionary
    col_types = {}
    for i, column in enumerate(columns):
        col_types[column] = types[i].lower()

    # Sort the dictionary by column and add each
    # column (in order) to our schema.
    for col_type in sorted(col_types.items(), key=lambda kv: kv[0]):
        data_type = col_type[1]
        if data_type not in _TYPE_MAP:
            raise ValueError(f'Unrecognized data type "{data_type}"')
        schema_columns.append(StructField(col_type[0], _TYPE_MAP[data_type][1]))
    return StructType(schema_columns)
