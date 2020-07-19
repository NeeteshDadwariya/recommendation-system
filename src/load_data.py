import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

#links_df = spark.read.option("header", "true").csv("/*.csv")
