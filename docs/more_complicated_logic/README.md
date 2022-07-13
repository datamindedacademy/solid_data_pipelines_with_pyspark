# A Pyspark use-case: creating features in a distributed environment

## Data Catalog

In data engineering, you create data pipelines frequently. This can range from
the very small scope of a pyspark ETL job, to a series of transformation steps
(like landing → clean → business) with possibly complex dependencies. E.g. a
typical business use case will require you to combine multiple data sources.
Because of this, you will typically want to have a _data catalog_ available: an
object that you can query to access certain datasets in a structured way. It
solves the problem of referring to specific paths all across user code by
promoting one single truth.

One major advantage is that when a datalake (or even the older data warehouses)
gets redesigned, all updates happen in one place. All ETL-code can stay the
same, as each time the path to (non-homogeneous) data is requested, the catalog
returns the correct definition. For batch jobs, tagging your datasets with an
execution date and ensuring your pipelines take into account the foreseen
execution date, will ensure you have easily rerunnable pipelines.

Exercise:

- implement a data catalog using a dictionary. Put the data catalog in its own
  module, as it is important enough by itself. In the catalog, refer to the
  following files:

  1. the flights data of the year 2000, which we've cleaned in the previous
     session.
  2. The [lookup table that maps IANA airport codes to their full names][lookup_airport]
  3. The [lookup table that maps IATA carrier codes][lookup_carrier]
     to airline carriers over a specific timerange.  Ensure that your catalog
     defines the “raw” forms and the clean forms (which should have the
     correct datatypes and be stored in one of the 3 big-data formats, being
     either avro, parquet or orc.)
- Add to the data catalog an entry for a 360°-view of the flights data.
- Create a 360°-view of the flights data in which you combine the airline
  carriers (a dimension table), the airport names (another dimension table) and
  the flights tables (a facts table). Apply your `is_holiday` function to the
  date-column.  You now have a dataset which can be used for further
  investigation, e.g. by a data scientist to see if there's a correlation
  between holidays and delays of flights.

At Data Minded, we've created
[Lighthouse](https://github.com/datamindedbe/lighthouse), an open-source
library using Spark at its core to create a catalog useful in big data
contexts. Note that the library is designed for Scala. Due to popular demand,
we've also worked on [PyHouse](https://pypi.org/project/pyhouse/), a Python
port of this library, which contains the features of LightHouse we thought were
most useful.  

### Pitfalls

**String joins**

In the previous (lengthy) exercise, you had to join two dataframes based on the
equality of a string column.  One common source of problems with string joins
is not using a **canonical form**.  As an example, consider the Citroën car
brand. Its official spelling is with the double-dots on the letter e. In many
cases however, people will blindly type a join condition like this:
`dimensions.carbrand == 'Citroën'`. For this to work as intended, there's a
_silent assumption_ that the `carbrand` column has the same case and spelling
as the word `Citroën`. In many situations, this is not the case: a developer
often has no guarantees that data provided to him has been transformed into
such a form. It is in these cases that a good developer will favor _correctness
over efficiency_ and gladly call `get_canonical_form(dimensions.carbrand) ==
get_canonical_form('Citroën')`. And with this, we revisit the importance of
_idempotence_: if the data had already been put into a canonical form, it is
important that the function does not change the form when being reapplied. 

Exercise:

- write a string normalizer: remove leading and trailing whitespace, ensure
  there is a consistent case. You can safely assume you're only dealing with
  the ASCII character set, so you may ignore accented characters.
- (unit) test your string normalizer

One way to avoid string joins in the middle of an ETL pipeline is to normalize
the data upon ingestion or when transforming landing zone data into a cleaned
form. Especially with categorical variables, like province names, this works
well. For people more familiar with databases, this is simply the process of
normalizing data, using & creating _dimension tables_ as you go along.

**Cache everything**

When you create a master table by joining and transforming several (base)
datasets, there will be times when a well-aimed `cache()` call on a Spark
DataFrame can do wonders. The trick is to know when to apply it, because wen
the memory gets full, caching will incur a performance hit: some data will need
to be unpersisted or moved to the (much slower) hard drive (even if that is an
SSD).

So when is it a good idea to call `cache`? Whenever you reuse a dataframe for a
different purpose. To put it differently, anytime you can trace the lineage of
two dataframes and find a common ancestor, it's at that point where the paths
start diverging that you will want to cache.

Example:

```python

frame1 = frame0.filter(...).select(...).join(...)

frame2 = frame1.filter(..).distinct().groupBy(...).count()

frame3 = frame1.select(..).join(..).show()
```

In this example, `frame3` and `frame2` have a common ancestor: `frame1`.
Because Spark DataFrames are evaluated lazily, the action that triggers the
calculation of `frame3` will re-execute the transformations that lead to
`frame1`. However, those same transformations have also been done when `frame2`
was being calculated. In order to prevent the transformations that lead to
`frame1` from being re-executed, `cache`-ing `frame1` is an excellent idea.
 
[lookup_airport]: https://www.transtats.bts.gov/Download_Lookup.asp?Lookup=L_AIRPORT
[lookup_carrier]: https://www.transtats.bts.gov/Download_Lookup.asp?Lookup=L_CARRIER_HISTORY
