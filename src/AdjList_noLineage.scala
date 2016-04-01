/**
 * Created by Michael on 4/1/16.
 */
import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util
import java.util.logging._
import org.apache.spark.SparkContext._

import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.Tuple2
import java.util.{Collections, Calendar, StringTokenizer}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, MutableList}
import scala.reflect.ClassTag

//remove if not needed
import scala.collection.JavaConversions._


import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import scala.sys.process._

object AdjList_noLineage {
  val LIMIT: Int = 200000
  private val exhaustive = 0

  def main(args: Array[String]): Unit = {
    try {
      //set up logging
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)

      //set up spark configuration
      val sparkConf = new SparkConf().setMaster("local[8]")
      sparkConf.setAppName("AdjacencyList_FaultSeeding")
        .set("spark.executor.memory", "4g")


      //set up spark context
      val ctx = new SparkContext(sparkConf)

      //Prepare for Hadoop MapReduce - for correctness test only
      /*
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/AdjList.jar", "org.apache.hadoop.examples.AdjList", "-m", "3", "-r", "1", "/Users/Michael/IdeaProjects/AdjacencyList/edges_31", "output").!!
      */

      //start recording delta-debugging + lineage time + goNext
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record total time starts at " + LineageStartTimestamp)

      //spark program starts here

      val lines = ctx.textFile("../AdjacencyList/edges_31.data", 1)

      //      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/AdjList_FaultSeeding/lineageResult"))

      //print out the result for debugging purpose
      //      for (o <- output_result) {
      //        println(o._1._1 + ": " + o._1._2 + " - " + o._2 + "\n")
      //      }


      //      print out the resulting list for debugging purposes
      //      println("*************************")
      //      for (l <- listl) {
      //        println(l)
      //      }
      //      println("*************************")



      //      linRdd.show.collect().foreach(s => {
      //        pw.append(s.toString)
      //        pw.append('\n')
      //      })

      //      pw.close()


      //use this version for the test without goNext
            val mappedRDD = lines.map(s => {
              val str = s.toString
              val index = str.indexOf(",")
              val key = str.substring(0, index)
              val value = str.substring(index + 1, str.length)
              (key, value)
            })

      mappedRDD.cache()


      //      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/AdjList_FaultSeeding/lineageResult", 1)

      //      val num = lineageResult.count()
      //      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")

      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/AdjList_FaultSeeding/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      //start recording delta debugging time
      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /* *****************
       * *************
       */
      //lineageResult.cache()


      //this version is for test without goNext
      val delta_debug = new DD_NonEx_v2[(String, String)]
      val returnedRDD = delta_debug.ddgen(mappedRDD, new Test, new Split_v2, lm, fh)

      val ss = returnedRDD.collect.foreach(println)


      //For version without goNext
      //      ss.foreach(println)

      //The end of delta debugging, record time
      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " microseconds")

      //total time
      logger.log(Level.INFO, "Record total time: Delta-Debugging + Linegae + goNext:" + (DeltaDebuggingEndTime - LineageStartTime)/1000 + " microseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }

      println("Job's DONE!")
      ctx.stop()

    }
  }
}
