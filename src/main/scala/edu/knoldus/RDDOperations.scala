package edu.knoldus

import java.io.PrintWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Neelaksh on 30/8/17.
  */
object RDDOperations extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Spark Demo")
  val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
  val sc = sparkSession.sparkContext
  val topPagesOPFile = "src/main/resources/toppages.txt"
  val logger = Logger.getLogger("spark-app")

  //answer 1
  val fileData = sc.textFile("src/main/resources/pagecounts-20151201-220000").cache()
  //nothing as its a lazy evaluation

  //answer 2
  val topData = fileData.zipWithIndex().filter {
    case (_, key) => key < 10
  }.keys.collect().toList.mkString("\n")

  new PrintWriter(topPagesOPFile) {
    write(topData)
    close()
  }
  // data written to src/main/resources/toppages.txt

  // answer 3
  val pageCount = fileData.count()
  logger.info(pageCount)
  // total count- 7598006

  //answer 4
  val onlyEnglishData = fileData.map(data => data.split(" ")).filter(data => data(0) == "en").map(data => data.toList.toString())
  //not evaluated as lazy operation

  //answer 5
  val englishPagesCount = onlyEnglishData.count()
  logger.info(englishPagesCount)
  //Total count - 2278417


  // answer 6
  val highlyRequestedData = fileData.map { data =>
    val op = data.split(" ")
    (op(1), op(2).toInt)
  }.reduceByKey(_ + _).filter(_._2.toInt > 200000).collect().toList.foreach(data => logger.info(s"${data._1} - ${data._2}"))

  /*
  17/09/01 22:07:43 INFO spark-app: de - 1040678
  17/09/01 22:07:43 INFO spark-app: ru - 477729
  17/09/01 22:07:43 INFO spark-app: Special:HideBanners - 1362459
  17/09/01 22:07:43 INFO spark-app: pt - 320239
  17/09/01 22:07:43 INFO spark-app: Main_Page - 450191
  17/09/01 22:07:43 INFO spark-app: ja - 335982
  17/09/01 22:07:43 INFO spark-app: en - 4925925
  17/09/01 22:07:43 INFO spark-app: it - 746508
  17/09/01 22:07:43 INFO spark-app: pl - 223976
  17/09/01 22:07:43 INFO spark-app: fr - 783966
  17/09/01 22:07:43 INFO spark-app: es - 1083799
   */
}
