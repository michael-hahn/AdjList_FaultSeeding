/**
 * Created by Michael on 4/1/16.
 */
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

//remove if not needed
import scala.collection.JavaConversions._


class Split_v2 extends userSplit_v2[(String, String)] {

  def usrSplit(inputList: RDD[(String, String)], splitTimes: Int): Array[RDD[(String, String)]] = {
    val weights = Array.ofDim[Double](splitTimes)
    for (i <- 0 until splitTimes) {
      weights(i) = 1.0 / splitTimes.toDouble
    }
    val rddList = inputList.randomSplit(weights)
    rddList
  }
}
