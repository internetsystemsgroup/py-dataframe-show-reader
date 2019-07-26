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
from pyspark.sql import Row
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType,
                               StringType, StructType, StructField, TimestampType)


def show_output_to_df(show_output: str, spark_session: SparkSession):
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

    Optionally, data types can be specified in a second header line (prefixed
    with "["):
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
    :return: A DataFrame containing the values represented in the input string
    """
    if not show_output:
        raise ValueError("show_output is required.")

    rows = []
    column_names = None
    types = None
    # Added a schema because createDataFrame() does introspection otherwise and
    # sometimes gets it wrong with int/bigint and nulls.
    schema = None
    for line in show_output.strip().splitlines():
        line = line.strip()
        if not line.startswith(tuple('|[')):
            continue
        line_parts = line.split('|')[1:-1]
        values = [part.strip() for part in line_parts]
        if types is not None:
            _cast_types(values, types)
        if column_names == None:
            column_names = values
            continue
        elif types == None and line.startswith('['):
            line = line.replace('[', '|').replace(']', '|')
            types = [part.strip() for part in line.split('|')[1:-1]]
            schema = _get_schema(column_names, types)
            continue
        row_dict = dict(zip(column_names, values))
        rows.append(Row(**row_dict))
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
           (e.g., "my_db.mytable").
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
           (e.g., "my_db.mytable").
    :param The DataFrame to be persisted
    :param spark_session: A SparkSession used to create the new DataFrame instance
    :param partition_cols: Optional list of columns on which to partition the table
    :return: None
    """
    df.write.partitionBy(partition_cols).saveAsTable(table_name, mode='Overwrite')
    spark_session.sql(f'REFRESH TABLE {table_name}')


def ts(timestamp_string: str):
    """
    Convert a DataFrame show output-style timestamp string into a datetime value
    which will marshall to a Hive/Spark TimestampType
    :param date_string: A date string in "YYYY-MM-DD HH:MM:SS" format
    :return: A datetime object
    """
    return datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S')


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


_TYPE_MAP = {
    '': (str, StringType()),  # Default
    'bigint': (int, LongType()),
    'boolean': (_parse_boolean, BooleanType()),
    'float': (float, DoubleType()),
    'int': (int, IntegerType()),
    'string': (str, StringType()),
    'timestamp': (ts, TimestampType()),
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
        if col_type[1] not in _TYPE_MAP:
            raise ValueError(f'Unrecognized data type "{col_type[1]}"')
        schema_columns.append(StructField(col_type[0], _TYPE_MAP[col_type[1]][1]))
    return StructType(schema_columns)
