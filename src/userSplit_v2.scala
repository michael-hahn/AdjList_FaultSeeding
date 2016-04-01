/**
 * Created by Michael on 4/1/16.
 */

import org.apache.spark.api.java.JavaRDD
import java.util.List

import org.apache.spark.rdd.RDD

//remove if not needed
import scala.collection.JavaConversions._

trait userSplit_v2[T] {

  def usrSplit(inputList: RDD[T], splitTimes: Int): Array[RDD[T]]
}
