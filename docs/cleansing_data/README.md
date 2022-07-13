# A Pyspark Usecase: Cleaning a Text File

## Pyspark vs Pandas

“There's a time and a place for everything”

Pandas is one of the most widely used modules in the Python data science
ecosystem. Unlike Spark, it keeps everything in memory in a single Python
process. Operations on Pandas Dataframes are “eagerly” evaluated, meaning they
are executed the moment the interpreter parses these operations.

This is in sharp contrast to Spark. A Spark DataFrame is a _distributed_
collection of data. It is also not kept in memory, unless explicitly asked for.
Operations on DataFrames are evaluated lazily: the actual chain of
transformations is being stored as modifications to a **query plan**, but the
actual query plan isn't executed until the user calls for an _action_ on the
DataFrame.

Spark is designed for processing large volumes of data. Pandas can only process
as much as can fit into the memory of the single Python process in which it
runs. And beware: Python objects quickly gather a lot of bytes for
housekeeping.

If you can do the job with Pandas, because the data is small or medium-sized,
by all means, do it in Pandas. People tend to forget that in Spark the task
needs to be serialized, sent over the wire to the different nodes, that various
worker processes need to be started and that all these things take time. With
Pandas, you only have to incur the processing time and the startup time of the
single Python process.

![When all you have is a hammer...](when-all-you-have-is-a-hammer2.jpg)

In short: **use a tool suited for the job**! There are times when even standard
POSIX tools like `sed` outperform Spark.

Exercise:

1. Name 3 _actions_ and 3 _transformations_ on Spark DataFrames.
2. Ask for the query plan of some DataFrame. You can use the following simple
   transformation pipeline

```python

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date
frame2 = spark.createDataFrame(
    [(1, "ONE", date(2018, 10, 22)), (2, "TWO", date(2019, 3, 12)), (3, "THREE", date(2018, 1, 1))],
    schema=StructType([
        StructField("id", IntegerType(), False),
        StructField("label", StringType(), True),
        StructField("mydate", DateType(), True)]))

frame = (
    spark.range(5)
        .withColumn("foo", col("id") % 2)
        .withColumn("bar", lit(2).cast("tinyint"))
        .withColumn("baz", when(col("id") * 5 + 10 > 22, col("bar") * col("foo")))
        .withColumn("date", date_add(to_date(lit("20181025"), "yyyyMMdd"), 1))
        .join(frame2, ["id"], how="left")
        .filter(col("id") >= 1)
        .filter(col("mydate") <= "2018-10-17")
)

frame.show()
frame.explain(True)

```

* What does the query plan tell you about the order of the filter operations?
  What does it tell you about the datefilter? Do you see an easy performance
  boost there?
* How does changing the nullable flag of the column "mydate" impact the
  queryplan? 
 
## Cleaning a CSV file

One of the ubiquitous data engineering jobs is tidying up some dataset. The
degree to which data engineers “tidy up” or “clean” is somewhat subjective, but
the common actions are these:

1. standardize column names

   Have you ever thought how annoying it is when you get a spreadsheet with the
   following columns: "NODA", "arg_x", "start_date  ", "end Date",
   "CUSTOMR_MAIL_ID"?

   Hint: unlike older databases, in Spark, the length of a column name can be
   _much_ longer than 8 characters. Use that wisely to improve the clarity of
   your datasets, by making them more self-descriptive. "date" isn't a very
   good choice for a column name when you mean "birth_date"
   
2. assign proper datatypes

   I can't stress this one enough: **the choice of datatype has a big impact on
   your job's performance**. The standard types that are available in PySpark
   cover a wide range of options. Sometimes it might even make sense to use the
   standard Spark datatypes over a custom, more appropriate object, because the
   standard Spark types are kept in an efficient binary serialization format.
   As an example: you _could_ store a numpy array as a value in an RDD.
   However, the overhead of deserializing it to allow transformations to be
   done on that array and then reserializing heavily degrades the performance
   over an implementation that simply uses `pyspark.sql.types.ArrayType`.
   A more common “mistake” is having dates and timestamps as strings
   (`pyspark.sql.types.StringType`). There are rich transformations available
   on dates and timestamps, and they also come with space saving. This same
   reasoning (using the right datatypes) also applies to non-Spark code: pure
   Python has incredibly useful data structures, like `set` (or its immutable
   variant, `frozenset`) and you do well to learn about them.
   
3. Unknown or missing data is flagged appropriately

   This comes back to the previous point: in IT, the problem of missing or
   unknown data has been solved ages ago. The solution is what we now call
   optional types. Imagine a RDBMS e.g. where customer personalia are being
   stored. Perhaps the age of a person is a field, but it is not required. In
   the RDBMS, this field could be an INT, that is sometimes NULL. In Spark SQL,
   this is a column of the IntegerType (or even smaller: a ByteType, since the
   oldest reported age is 122 years), that is *nullable*. 

Once you have a cleaned form of the data, it is typically written away for
later usage. That way, downstream processes can benefit from the improved types
and don't all have to re-execute the same steps. Good data lake practices often
have 3 zones of data: landing, clean and business (in some businesses this is
also known as bronze, silver and gold data).

1. Landing: data from various sources gets stored here typically, waiting for
   it to be picked up. It is a heterogeneous mix of structured and unstructured
   data in various formats (csv, json, parquet, jpg, wav, xml, …). This zone
   typically meets the needs of data scientists exploring new datasets from
   which to gather actionable insights. In data warehousing this
   heterogenous-ness and unsortedness was missing. 
2. Clean: the data from landing in an improved format (see above for common
   improvements). Often at this point the data will be stored in a single file
   format. For big data, the options are typically Parquet, ORC or Avro.
   Parquet and ORC offer advantages for analytics workloads, as these are
   column-oriented file formats.
3. Business, or “master”, or “projects”: this is a layer where different sets
   of cleaned data are joined, and transformed in ways that fulfill a certain
   business usecase. A machine learning model e.g. might need to combine
   several datasets to get to a feature table. The generation (and storage) of
   that feature table is being done in the business zone.

Exercise:

We are going to _clean_ a CSV file using PySpark.

* Grab the US flight data of the year 2000 from [the website of the American
  Statistical
  Association](http://stat-computing.org/dataexpo/2009/the-data.html).
* Inspect the columns: what type of data do they hold?
* Create an ETL job with PySpark where you read in the csv file, and perform
  the cleansing steps mentioned earlier.
* Write the data away as a Parquet file. How big is the Parquet file compared
  to the compressed csv file? And compared to the uncompressed csv file? How
  long does your processing take?
* While your job is running, open up the Spark UI and get a feel for what's
  there (together with the instructor)

**ANECDOTES** 

* end_of_contract: I have seen at a client a time when a dataset was ingested,
  where the data related to customers and their contracts. This table had a
  column indicating when a person's contract had ended. For the running
  contracts, this field was set to the year 9999. Using 9999 as a placeholder
  is a (poor) substitute for resolving this properly: use NULL. Statistics with
  placeholders lead to wrong insights. Most (all?) aggregation metrics on
  frames take null into account.
* age (of a person) → Could be stored in a BYTE, instead of an INT. Oldest 
  living documented person is 122 years.
 
## Data Ingestion Options

When you are processing data, be mindful of the format of the source. For
nearly all analytics usecases, you will want to start from either Parquet or
ORC files. The reason for that is that these are column-oriented file formats.
If you have a dataset of e.g. 30 columns and an ETL job is only requesting 3,
then only these three columns will be parsed.

For big data workloads, CSV is rubbish: you are required to process line by
line _AND_ everything is textual! There is no type information. Strings are
notoriously memory inefficient.

Question:
- how many bytes are required to store an empty string in JAVA?
<!-- about 42 bytes per string -->

Additionally, if you know the type of query that will be run most commonly on a
dataset, it is possible to use a partitioning scheme when writing away the
dataset. It is being done for the same reason that database administrators
create partitions of tables: performance.

## Datatypes: why and how?

Just like in Python you should use data types that reflect the nature of the
data well (e.g. rather than storing two lists, one with people’s names, the
other with their ages, a dictionary is better suited to reflect this mapping
from users to ages), you do the same in Spark.
Each of the data types requires a certain amount of bytes to store. If you can
store data with a smaller type (e.g. a short instead of an int), then Spark
will also process this data faster, because it can take more data in the CPU
cache.

Question:
- when you need to store geographical coordinates in terms of angles (e.g.
  52.2345° North), what data type would you choose? What are the implications of
  that choice in terms of number of bytes and in terms of accuracy?

<!-- About one meter accuracy can be obtained with floats, ±70cm at the latitude of Belgium -->
<!-- Note that civilian GPS are accurate to about 4 meter -->

Like we did with the cleansing, you should use the most optimal data types as 
early as possible in an ETL pipeline to benefit as much as you can from it.

Common scenarios:

* columns with Yes/No values → These should be encoded as a BooleanType
* human ages, car ages → a Byte will suffice. If you're concerned that the
  human age is stretching it thin, take a Short, which is still only half the
  size of an int. 
* number of kids → a byte suffices
* postal codes: in Belgium, a Short suffices. In many other countries (NL, UK,
  ES) this should be a String.


## User-Defined functions vs `spark.sql.functions`

User-defined functions lead to less performant code, because they incur two
drawbacks:

1. high serialization overhead

   What this means is that objects need to be transformed from very efficient
   binary representations (given by project Tungsten in Spark) to objects in a
   specific language, such as Python. After some operation on the Python
   object, the objects need to get reserialized into the efficient Spark form.

2. large number of invocation calls

   UDFs have a one-row-at-a-time characteristic: for each row of the DataFrame,
   a certain function gets called. Function calls are not for free either.
   While the cost of a function call is typically small, in terms of big data,
   a function that gets called many times over could become a bottleneck.

   Note: this has been largely alleviated by PandasUDFs in recent versions of
   Spark. You still suffer the serialization overhead and there aren't that
   many big data operations that cannot also be done with standard Spark
   functionality.

Whenever possible: use functions defined in `spark.sql.functions` or methods on
`pyspark.sql.Column` objects for better performance. If you must resort to
udfs, in PySpark there are nowadays (since Spark 2.3) also "pandas udfs", which
work on `pandas.Series` and thus have the promise of being vectorized, thereby
increasing performance over an ordinary UDF.

Exercise:

- given the [`holidays`](https://pypi.org/project/holidays/) module, create a
  function that takes as an input a Spark DataFrame with a date column called
  “date”. The function should label for each date whether or not that date is a
  holiday. Use the holidays of the country you last visited.
  Create the function using a udf at first.
- Can you create it using `isin`?
- Rewrite the function using a SQL like join operation.
- Compare the performance of these 3 solutions. 

One alternative to udfs and pandas udfs (in terms of performance it's also
somewhere between these two) is called `mapPartitions`.

`mapPartitions` gives you a lot of freedom, as you can work fully with Python
objects. It gets rid of the one-row-at-a-time characteristic of udfs, replacing
it by a one-partition-at-a-time aspect. You _iterate_ over the rows of a
partition, which does not require a lot of memory. The return type should be a
new iterator. This is easily implemented using Python generators, as shown
below.

Note that you are not limited to Python generators though: if you wanted, you
could convert the entire partition to a Pandas Dataframe and apply some
functions on it, then convert it back to an iterator with e.g.
`pandas.Dataframe.iterrows`.

```python
from datetime import date

import holidays
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("local").getOrCreate()
dummy = ("/home/oliver/work/clients/kbc/teaching/data_engineering_with_pyspark/03_transformation_pipeline/"
         "usecase1/sample_data/part-00000-9c679f89-9598-423d-a361-937859d9f737-c000.snappy.parquet")
# frame = spark.read.parquet(dummy)


def label_holiday(partition):
    belgian_holidays = holidays.BE()
    for row in partition:
        yield Row(row.date, row.date in belgian_holidays)


def main(frame):
    output = frame.rdd.mapPartitions(label_holiday).toDF(("date", "is_holiday"))
    output.printSchema()
    output.show()


if __name__ == "__main__":
    schema = StructType([StructField("date", DateType(), True),
                         StructField("foo", StringType(), True)])
    frame = spark.createDataFrame([(date(2019, 1, 1), "foo"),
                                   (date(2019, 1, 2), "bar")],
                                  schema=schema)
    main(frame)
```


Exercise:

- inspect some readily available code you've been handed by co-workers. Go over
  it in teams of two and for each block of code, discuss the pros and cons. Can
  you think of improvements?
 
 (teacher has code available) 
