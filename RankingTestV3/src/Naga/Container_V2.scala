package Naga

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by gavinzjchao on 2016/8/3.
  */
class Container_V2 extends Serializable {
  private def featureEncoding(input: RDD[(String, (String, Double))]): mutable.HashMap[String, (Int, Int)] = {
    val dict = mutable.HashMap[String, (Int, Int)]()
    val curArr = input.map{case (f, (k, w)) => f -> 1}.reduceByKey(_ + _).collect()

    var idx = 0
    for ((k, v) <- curArr) {
      dict += ((k, (v, idx)))
      idx += 1
    }
    dict
  }

  private def extractLabel(uniqLine: String): Double = {
    try {
      uniqLine.substring(uniqLine.lastIndexOf("^") + 1).toDouble
    } catch {
      case e: Exception => 0.0
    }
  }

  private def sortAndDistinct(iter: Array[(Int, Double)]): Array[(Int, Double)] = {
    if (iter.length <= 1) return iter

    val sortedArr = iter.sortWith((x, y) => x._1 < y._1)
    val res = mutable.ArrayBuffer[(Int, Double)](iter(0))

    var i = 1
    var cur = sortedArr(0)._1
    while (i < sortedArr.length) {
      if (sortedArr(i)._1 != cur) {
        cur = sortedArr(i)._1
        res += sortedArr(i)
      }

      i += 1
    }

    res.toArray
  }

  def fit(sc: SparkContext, input: RDD[(String, (String, Double))], threshold: Int = 30, numPartitions: Int): RDD[LabeledPoint] = {
    val hasher = new HashPartitioner(numPartitions)

    /** 1. generate feature dict */
    val fDict = featureEncoding(input)
    val hfDict = fDict.filter(_._2._1 >= threshold)
    val bHfDict = sc.broadcast(hfDict)
    val lfFeatures = sc.parallelize(fDict.filter(_._2._1 < threshold).toSeq)
      .partitionBy(hasher)

    /** 2. feature shuffle */
    val transFeatures = input.mapPartitions( iter => {
      val res = mutable.ArrayBuffer[(String, (String, Double, Char))]()
      val _hfDict = bHfDict.value
      for ((f, (k, w)) <- iter) {
        if (_hfDict.contains(f)) {
          res += ((k, (_hfDict(f)._2.toString, w, 'h')))
        }
        else {
          res += ((f, (k, w, 'l')))
        }
      }
      res.iterator
    }, preservesPartitioning = true)

    /** 3. generate inverted index: with tricks */
    val invertedIndex = transFeatures.leftOuterJoin(lfFeatures)
      .map{ case ((f: String, (left: (String, Double, Char), right: Option[(Int, Int)]))) =>
        var featureIndex = 0
        var uniqLine = ""
        var weight = 0.0

        if (left._3 == 'h') {
          featureIndex = left._1.toInt
          uniqLine = f
          weight = left._2
        }
        else {
          if (right.isDefined) {
            featureIndex = right.get._2
            uniqLine = left._1
            weight = left._2
          }
        }
        (uniqLine, (featureIndex, weight))
      }

    /** 4. group all features in unique line and get labeled point */
    val featureSpace = fDict.size
    val trainingData = invertedIndex.filter(_._1 != "").groupByKey(hasher).map{ case (uniqLine, iter) =>
      val label = extractLabel(uniqLine)
      val curSeq = sortAndDistinct(iter.toArray)
      LabeledPoint(label, Vectors.sparse(featureSpace, curSeq))
    }

    trainingData
  }

}

object Container_V2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("news recommend offline ranking")
      .setMaster("local[5]")

    val sc = new SparkContext(conf)

    val data = sc.parallelize(Seq(("f1", ("^1", 1.0)), ("f2", ("^1", 2.0)), ("f1", ("^0", 1.0)), ("f3", ("^0", 3.0))))
    val container = new Container_V2

    val res = container.fit(sc,data,0,3)
    res.foreach(println(_))
  }
}
