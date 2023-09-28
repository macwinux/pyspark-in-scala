#!/usr/bin/env python
# coding: utf-8
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from py4j.java_gateway import java_import
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

@pandas_udf(IntegerType())
def slen(s: pd.Series) -> pd.Series:
    return s.str.len()

if __name__ == "__main__":
    gateway = pyspark.java_gateway.launch_gateway()
    java_import(gateway.jvm, "com.example.pysparkscala.PysparkScala")
    jsc = gateway.jvm.PysparkScala.getJsc()
    jconf = gateway.jvm.PysparkScala.getConf()
    conf = pyspark.conf.SparkConf(True, gateway.jvm, jconf)
    sc = pyspark.SparkContext(gateway=gateway, jsc=jsc, conf=conf)
    spark = SparkSession(sc)
    
    
    df = spark.sql("SELECT * FROM table")
    df_processed = df.withColumn("len", length('language').alias('len'))
    df_processed.createOrReplaceTempView("table")

    #This provoke an error in DAGScheduler but it works
    spark.udf.register("slen", slen)
    df_udf = spark.sql("SELECT language, users_count, len, slen(language) as udf_len FROM table")
    df_udf.createOrReplaceTempView("table")
    
    
    
    sys.exit(0)
    