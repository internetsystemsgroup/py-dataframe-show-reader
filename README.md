# py-dataframe-show-reader

``py-dataframe-show-reader`` is a library that reads the output of an Apache
Spark DataFrame.show() statement into a PySpark DataFrame.

The primary intended use of the functions in this library is to be used to
enable writing more concise and easy-to-read tests of data transformations than
would otherwise be possible.

Imagine we are working on a Python method that uses PySpark to perform a data
transformation that is not terribly complex, but complex enough that we would
like to verify that it performs as intended. For example, consider this
function that accepts a PySpark Dataframe containing daily sales figures and
returns a Dataframe containing containing weekly sales summaries:

```python
def summarize_weekly_sales(df_to_average: DataFrame):
    return (df_to_average
            .groupby(f.weekofyear('date').alias('week_of_year'))
            .agg(f.avg('units_sold').alias('avg_units_sold'),
                 f.sum('gross_sales').alias('gross_sales')))
```

This is not a complex method, but perhaps we would like to verify that:

1. Dates are in fact grouped into different weeks.
1. The units sold are actually averaged and not summed.
1. Gross sales are actually summed and not averaged.

A unit testing purist might argue that each of these assertions should be
covered by a separate test method, but there are at least two reasons why one
might choose not to go that route.

1. Practical experience tells us that there is detectable overhead incurred for
each separate PySpark transformation test, so we may want to limit the number
of separate tests in order to keep our test suite as a whole running in a
reasonable amount of time.

1. When working with sets of data as we do in Spark or SQL, particularly when
using aggregate, grouping and window functions, sometimes there are
interactions between different rows that are easy to overlook, and tweaking a
query to fix an aggregate function like a summation might inadvertently break
the intended behavior of a windowing function in the query, and a change to the
query might allow a summation-only unit test to pass while allowing broken
window function behavior to go undetected because we don't think to also update
the window-function-only unit test.  

If we accept that we'd like to use a single test to verify the three
requirements of our query listed above, we're going to need three rows in our
input DataFrame.

Using unadorned pytest, our test might look like this:

```python
def test_without_dataframe_show_reader(spark_session: SparkSession):
    input_rows = [
        Row(
            date=datetime(2019, 1, 1),
            units_sold=10,
            gross_sales=100,
        ),
        Row(
            date=datetime(2019, 1, 2),
            units_sold=20,
            gross_sales=200,
        ),
        Row(
            date=datetime(2019, 1, 8),
            units_sold=80,
            gross_sales=800,
        ),
    ]
    input_df = spark_session.createDataFrame(input_rows)

    result = summarize_weekly_sales(input_df).collect()
    assert 2 == len(result)
    assert 1 == result[0]['week_of_year']
    assert 15 == result[0]['avg_units_sold']
    assert 300 == result[0]['gross_sales']
```

Using the DataFrame show reader, our test could look like this instead:

```python
def test_using_dataframe_show_reader(spark_session: SparkSession):
    input_df = show_output_to_df("""
    +-------------------+----------+-----------+
    |date               |units_sold|gross_sales|
    +-------------------+----------+-----------+
    |2019-01-01 00:00:00|10        |100        |
    |2019-01-02 00:00:00|20        |200        |
    |2019-01-08 00:00:00|80        |800        |
    +-------------------+----------+-----------+
    """, spark_session)

    result = summarize_weekly_sales(input_df).collect()
    assert 2 == len(result)
    assert 1 == result[0]['week_of_year']
    assert 15 == result[0]['avg_units_sold']
    assert 300 == result[0]['gross_sales']
```

In the second test example, the ``show_output_to_df`` function accepts as input
a string containing the output of a Spark DataFrame.show() call and
"rehydrates" it into a new Spark DataFrame instance that can be used for
testing.

In the first version, the setup portion of the test contains eighteen lines,
and it may take a few moments to digest the contents of the input rows.
In the second version, the setup portion contains nine lines and displays the
input data in a more concise tabular form that may be easier for other
programmers (and our future selves) to digest when we need to maintain this
code down the road.

If the method under test were more complicated and required more rows and/or
columns in order to adequately test, the length of the first test format would
grow much more quickly than that of the test using the DataFrame Show Reader.

## Running the Tests

1. Clone the git repo.
1. ``cd`` into the root level of the repo.
1. At a terminal command line, run ``pytest``

## Who Maintains DataFrame Show Reader?

DataFrame Show Reader is the work of the community. The core committers and
maintainers are responsible for reviewing and merging PRs, as well as steering
conversation around new feature requests. If you would like to become a
maintainer, please contact us.
