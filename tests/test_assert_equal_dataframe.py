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

import pytest
from pyspark.sql import DataFrame, SparkSession
from pytest import raises

from dataframe_show_reader.assert_equal_dataframe import assert_equal
from dataframe_show_reader.dataframe_show_reader import show_output_to_df


@pytest.fixture(scope="session")
def expected_df(spark_session: SparkSession) -> DataFrame:
    return show_output_to_df("""
    +-----+-----+
    |col_a|col_b|
    +-----+-----+
    |1a   |1b   |
    |2a   |2b   |
    +-----+-----+
    """, spark_session)


def test_assert_equal_when_dfs_are_equal(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+-----+
    |col_a|col_b|
    +-----+-----+
    |1a   |1b   |
    |2a   |2b   |
    +-----+-----+
    """, spark_session)

    # No error or assertion failure should be thrown:
    assert_equal(expected_df, actual_df)


def test_assert_equal_when_actual_df_has_different_value(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+-----+
    |col_a|col_b|
    +-----+-----+
    |1a   |1b   |
    |2a   |99999|
    +-----+-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df, verbose=True)
    assert 'The DataFrames differ.' == str(exception_info.value)


def test_assert_equal_when_column_order_is_different(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+-----+
    |col_b|col_a|
    +-----+-----+
    |1b   |1a   |
    |2b   |2a   |
    +-----+-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df)
    assert 'The DataFrame schemas differ.' == str(exception_info.value)


def test_assert_equal_when_dfs_are_equal_and_column_is_null(
        spark_session: SparkSession
):
    actual_df = show_output_to_df("""
    +------+
    |col_a |
    [string]
    +------+
    |null  |
    +------+
    """, spark_session)

    expected_df = show_output_to_df("""
    +------+
    |col_a |
    [string]
    +------+
    |null  |
    +------+
    """, spark_session)

    # No error or assertion failure should be thrown:
    assert_equal(expected_df, actual_df)


def test_assert_equal_when_actual_df_has_too_few_rows(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+-----+
    |col_a|col_b|
    +-----+-----+
    |1a   |1b   |
    +-----+-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df, verbose=False)
    assert 'The DataFrames differ.' == str(exception_info.value)


def test_assert_equal_when_actual_df_has_too_many_rows(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+-----+
    |col_a|col_b|
    +-----+-----+
    |1a   |1b   |
    |2a   |2b   |
    |3a   |3b   |
    +-----+-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df)
    assert 'The DataFrames differ.' == str(exception_info.value)


def test_assert_equal_when_actual_df_has_duplicate_last_row(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+-----+
    |col_a|col_b|
    +-----+-----+
    |1a   |1b   |
    |2a   |2b   |
    |2a   |2b   |
    +-----+-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df)
    assert 'The DataFrames differ.' == str(exception_info.value)


def test_assert_equal_when_actual_df_has_too_few_columns(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+
    |col_a|
    +-----+
    |1a   |
    |2a   |
    +-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df)
    assert 'The DataFrame schemas differ.' == str(exception_info.value)


def test_assert_equal_when_actual_df_has_too_many_columns(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+-----+-----+
    |col_a|col_b|col_c|
    +-----+-----+-----+
    |1a   |1b   |1c   |
    |2a   |2b   |2c   |
    +-----+-----+-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df)
    assert 'The DataFrame schemas differ.' == str(exception_info.value)


def test_assert_equal_when_column_names_do_not_match(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+-----+
    |col_a|col_x|
    +-----+-----+
    |1a   |1b   |
    |2a   |2b   |
    +-----+-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df)
    assert 'The DataFrame schemas differ.' == str(exception_info.value)


def test_assert_equal_when_data_types_do_not_match(
        spark_session: SparkSession):
    """
    Test the fairly subtle case where one DF contains an INT and the other
    contains a BIGINT, which can be an issue if we try to write a DF containing
    a BIGINT into a previously existing Hive table defined to contain an INT.
    """
    actual_df = show_output_to_df("""
    +------+
    |col_a |
    [bigint]
    +------+
    |1     |
    +------+
    """, spark_session)

    expected_df = show_output_to_df("""
    +------+
    |col_a |
    [int   ]
    +------+
    |1     |
    +------+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, actual_df)
    assert 'The DataFrame schemas differ.' == str(exception_info.value)


def test_assert_equal_when_actual_df_is_none(
        expected_df, spark_session: SparkSession):
    with raises(AssertionError) as exception_info:
        assert_equal(expected_df, None)
    assert 'The actual DataFrame is None, but the expected DataFrame is not.' \
           == str(exception_info.value)


def test_assert_equal_when_expected_df_is_none(
        expected_df, spark_session: SparkSession):
    actual_df = show_output_to_df("""
    +-----+
    |col_a|
    +-----+
    |1a   |
    +-----+
    """, spark_session)

    with raises(AssertionError) as exception_info:
        assert_equal(None, actual_df)
    assert 'The expected DataFrame is None, but the actual DataFrame is not.' \
           == str(exception_info.value)


def test_assert_equal_when_both_dfs_are_none(
        expected_df, spark_session: SparkSession):
    # No error or assertion failure should be thrown:
    assert_equal(None, None)
