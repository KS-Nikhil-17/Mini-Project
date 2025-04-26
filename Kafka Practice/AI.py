from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.functions import *
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(authors, research_papers):
          df = authors.merge(research_papers , on ="paper_id")
        window_spec = W.partitionBy("paper_id").orderBy("author_id")
        result= df.withColumn("row_number", row_number().over(window_spec))
        return (result.select('author_id','name','paper_id','row_number'))