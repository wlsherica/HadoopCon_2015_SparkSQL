# HadoopCon_2015_SparkSQL

Tutorial of Spark SQL on HadoopCon 2015. This is a file of [IPython notebook](http://ipython.org/notebook.html)/[Jupyter](https://jupyter.org/) by using the **Python** language.

## Requirements

This training material requires Spark 1.4.1

## Introduction

In this tutorial, you will learn how to initialize Spark SQL with SQLContext (HiveContext), manipulate DataFrames, import data, user defined functions, and operate cache(). For example, 

### Python API
Spark 1.4.1:
```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
```

```python
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
```

## Reference
Please check [Spark SQL and DataFrame Guide](http://spark.apache.org/docs/latest/sql-programming-guide.html) and [Apache Spark](http://spark.apache.org/) for more details.
