package com.example.pysparkscala

import org.apache.spark.sql.SparkSession
import org.apache.spark.deploy.PythonRunner

import ru.mardaunt.python.PySparkApp
import ru.mardaunt.python.logger.SimpleLogger
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf

object PysparkScala {

  def getJsc(): JavaSparkContext = jsc
  def getConf(): SparkConf = conf
  var jsc: JavaSparkContext = _ 
  var conf: SparkConf = _
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("Spark Python Runner")
                    .master("local[1]")
                    .getOrCreate()
    
    jsc = new JavaSparkContext(spark.sparkContext)
    conf = jsc.getConf
    import spark.implicits._
    val columns = Seq("language","users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF(columns:_*)
    df.createOrReplaceTempView("table")
    PythonRunner.main(Array(
      "src/main/python/example.py",
      "src/main/python/example.py"
    ))

    spark.sql("SELECT * FROM table").show()

    //spark.stop()
  }
}

//class PysparkScala(spark: SparkSession, logger: Logger)
//  extends PySparkApp(mainPyName = "pyspark_main.py", needKerberosAuth = false)(spark, logger) {
//
//  override protected val starterTool: String = "spark-submit"
//}
//
//object PysparkScala extends App {
//  lazy val spark = SparkSession.builder()
//                               .master("local[*]").appName("Spark Python Runner")
//                               .getOrCreate()
//
//  import spark.implicits._
//  val columns = Seq("language","users_count")
//  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
//  val rdd = spark.sparkContext.parallelize(data)
//  val df = rdd.toDF()
//  df.createOrReplaceTempView("table")
//  
//  new PysparkScala(spark, SimpleLogger()).run()
//
//
//  spark.sql("Select * from table").show()
//}