/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext._
import org.apache.spark.lineage.rdd.ShowRDD
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.sys.process._
import scala.io.Source
import scala.util.control.Breaks._

import java.io.File
import java.io._

import org.apache.spark.delta.DeltaWorkflowManager



class Test extends userTest[(String, String)] with Serializable {
  val LIMIT: Int = 200000
  var num = 0
  def usrTest(inputRDD: RDD[(String, String)], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass (which returns false)
    var returnValue = false

    val finalRDD = inputRDD
      .flatMap(s => {
        val listOfEdges: MutableList[(String, String)] = MutableList()
        val outList = "from{" + s._1 + "}:to{}"
        var inList = "from{}:to{" + s._2 + "}"
        val out = Tuple2(s._1, inList)
        val in = Tuple2(s._2, outList)
        listOfEdges += out
        listOfEdges += in
        listOfEdges
      })
      .groupByKey()
      .map(pair => {
        var fromList: MutableList[String] = MutableList()
        var toList: MutableList[String] = MutableList()
        var fromLine = new String()
        var toLine = new String()
        var vertex = new String()
        //       var itr:util.Iterator[String] = null
        //        try {
        val itr = pair._2.toIterator
        //       }catch{
        //         case e:Exception =>
        //           println("**************************")
        //       }
        while (itr.hasNext) {
          breakable {
            val str = itr.next()
            val strLength = str.length
            val index = str.indexOf(":")
            if (index == -1) {
              println("Wrong input: " + str)
              break
            }
            if (index > 6) {
              fromLine = str.substring(5, index - 1)
            }
            if (index + 5 < strLength) {
              toLine = str.substring(index + 4, strLength - 1)
            }
            if (!fromLine.isEmpty) {
              val itr2 = new StringTokenizer(fromLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!fromList.contains(vertex) && fromList.size < LIMIT) {
                  fromList += vertex
                }
              }
            }
            if (!toLine.isEmpty) {
              val itr2 = new StringTokenizer(toLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!toList.contains(vertex) && toList.size < LIMIT) {
                  toList += vertex
                }
              }
            }
          }
        }
        fromList = fromList.sortWith((a, b) => if (a < b) true else false)
        toList = toList.sortWith((a, b) => if (a < b) true else false)
        var fromList_str = new String("")
        var toList_str = new String("")
        for (r <- 0 until fromList.size) {
          if (fromList_str.equals("")) fromList_str = fromList(r)
          else fromList_str = fromList_str + "," + fromList(r)
        }
        for (r <- 0 until toList.size) {
          if (toList_str.equals("")) toList_str = toList(r)
          else toList_str = toList_str + "," + toList(r)
        }
        val outValue = new String("from{" + fromList_str + "}:to{" + toList_str + "}")
        (pair._1, outValue)
      })

    val start = System.nanoTime

    val out = finalRDD.collect()
    logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000)
    num = num + 1
    logger.log(Level.INFO, "TestRuns : " + num)
    println(s""">>>>>>>>>>>>>>>>>>>>>>>>>>>> Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")

    for (o <- out) {
      val index = o._2.lastIndexOf(":")
      val substr = o._2.substring(index + 4, o._2.length - 1)
      val toList = substr.split(",")
      val substr2 = o._2.substring(5, index - 1)
      val fromList = substr2.split(",")
      if (toList.contains("VertexIDA") || toList.contains("VertexIDB") || toList.contains("VertexIDC") || toList.contains("VertexIDD")
        || fromList.contains("VertexIDA") || fromList.contains("VertexIDB") || fromList.contains("VertexIDC") || fromList.contains("VertexIDD")) {
        returnValue = true
      }
    }
    returnValue
  }

  //FOR LOCAL COMPUTATION TEST WILL ALWAYS PASS
  def usrTest(inputRDD: Array[(String,String)], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass (which returns false)
    var returnValue = false

    val finalRDD = inputRDD
      .flatMap(s => {
        val listOfEdges: MutableList[(String, String)] = MutableList()
        val outList = "from{" + s._1 + "}:to{}"
        var inList = "from{}:to{" + s._2 + "}"
        val out = Tuple2(s._1, inList)
        val in = Tuple2(s._2, outList)
        listOfEdges += out
        listOfEdges += in
        listOfEdges
      })
      .groupBy(_._1)
      .map(pair => {
        var fromList: MutableList[String] = MutableList()
        var toList: MutableList[String] = MutableList()
        var fromLine = new String()
        var toLine = new String()
        var vertex = new String()
        //       var itr:util.Iterator[String] = null
        //        try {
        val itr = pair._2.toIterator
        //       }catch{
        //         case e:Exception =>
        //           println("**************************")
        //       }
        while (itr.hasNext) {
          breakable {
            val str = itr.next()
            val strLength = str._2.length
            val index = str._2.indexOf(":")
            if (index == -1) {
              println("Wrong input: " + str._2)
              break
            }
            if (index > 6) {
              fromLine = str._2.substring(5, index - 1)
            }
            if (index + 5 < strLength) {
              toLine = str._2.substring(index + 4, strLength - 1)
            }
            if (!fromLine.isEmpty) {
              val itr2 = new StringTokenizer(fromLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!fromList.contains(vertex) && fromList.size < LIMIT) {
                  fromList += vertex
                }
              }
            }
            if (!toLine.isEmpty) {
              val itr2 = new StringTokenizer(toLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!toList.contains(vertex) && toList.size < LIMIT) {
                  toList += vertex
                }
              }
            }
          }
        }
        fromList = fromList.sortWith((a, b) => if (a < b) true else false)
        toList = toList.sortWith((a, b) => if (a < b) true else false)
        var fromList_str = new String("")
        var toList_str = new String("")
        for (r <- 0 until fromList.size) {
          if (fromList_str.equals("")) fromList_str = fromList(r)
          else fromList_str = fromList_str + "," + fromList(r)
        }
        for (r <- 0 until toList.size) {
          if (toList_str.equals("")) toList_str = toList(r)
          else toList_str = toList_str + "," + toList(r)
        }
        val outValue = new String("from{" + fromList_str + "}:to{" + toList_str + "}")
        (pair._1, outValue)
      })

    val out = finalRDD
    num = num +1
    println(s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
      //   println(o)
      val index = o._2.lastIndexOf(":")
      val substr = o._2.substring(index + 4, o._2.length - 1)
      val toList = substr.split(",")
      val substr2 = o._2.substring(5, index - 1)
      val fromList = substr2.split(",")
      if (toList.contains("VertexIDA") || toList.contains("VertexIDB") || toList.contains("VertexIDC") || toList.contains("VertexIDD")
        || fromList.contains("VertexIDA") || fromList.contains("VertexIDB") || fromList.contains("VertexIDC") || fromList.contains("VertexIDD")) {
        returnValue = true
      }
    }
    returnValue

  }

}

