package NagaFeatureAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object ChiSquare {

  def ChiSquareMulti(sc: SparkContext, data: RDD[String],
                     delimiter: String = ",", indices: Array[Int] = Array[Int]()):
                      Array[(Int, Int, Double)] = {
    // 1. couting samples
    var isRelease = false
    if (data.getStorageLevel == StorageLevel.NONE) {isRelease = true; data.cache()}
    val nFeatures = data.first.split(delimiter).length - 1
    val _indices = if (indices.nonEmpty) indices else getAllFeatureIndices(data.first.split(delimiter).length - 1)

    // 2. transform data to <featureIndex, featureValue, label>
    val flatXY = data.filter(_.split(delimiter).length == nFeatures + 1).map{ line =>
      val features = line.trim.split(delimiter)
      val label = features(features.length - 1)
      val res = mutable.ArrayBuffer[(Int, String, String)]()
      for (idx <- _indices) {
        res += ((idx, features(idx), label))
      }
      res += ((features.length - 1, label, label))
      res += ((features.length, "COUNTER_TRICK", "COUNTER_TRICK"))

      res.toArray
    }

    // 3. compute marginal counter: c(x) & c(y)
    val cXorY = flatXY
      .flatMap(arr => arr.map{case (i, x, y) => ((i, x), 1)})
      .reduceByKey(_ + _)

    val degreeFreedom = cXorY.map{ case (((i, x), cnt)) => (i, 1)}.reduceByKey(_ + _)

    val bcXorY = sc.broadcast(cXorY.collectAsMap)
    val bDegreeFreedom = sc.broadcast(degreeFreedom.collectAsMap)

    // 4. compute chi square: sum((observe_ij - expect_ij)^2 / expect_ij)
    val chiSquare = flatXY
      .flatMap(arr => arr.map{case (i, x, y) => ((i, x, y), 1)})
      .reduceByKey(_ + _)
      .filter(_._1._1 < nFeatures)
      .map{ case (((i, x, y), cxy)) =>
        val _cXorY = bcXorY.value
        val _degreeFreedom = bDegreeFreedom.value
        val _cx = _cXorY((i, x))
        val _cy = _cXorY((nFeatures, y))
        val _cn = _cXorY((nFeatures + 1, "COUNTER_TRICK"))
        val _cxy = if (i >= nFeatures) 0.0 else cxy

        val _exy = _cx.toDouble * _cy / _cn
        val curChiSquare = math.pow(_cxy - _exy, 2) / _exy
        val df = (_degreeFreedom(i) - 1) * (_degreeFreedom(nFeatures) - 1)
        ((i, df), curChiSquare)
      }
      .reduceByKey(_ + _)
      .map{case (((i: Int, df: Int), chi: Double)) => (i, df, chi)}
      .collect()

    // 5. release cached internal RDDs
    if (isRelease) data.unpersist()
    bcXorY.unpersist()
    bDegreeFreedom.unpersist()

    chiSquare
  }

  def ChiSquareOHE(data: RDD[String], label_index: Int = 0,
                   delimiter: String = ",", indices: Array[Int]): RDD[(Int, Double)] = {
    val N = data.count()

    val chi = data.flatMap{line =>
      val feature = line.split(delimiter)
      val label = feature(label_index).toInt
      // map to (i, (A, B, C, D)) where i is column index of feature
      // set (A, B, C, D) according to (feature, label)
      indices.map { i =>
        val x = feature(i).toInt
        val y = label
        (x, y) match {
          case (1, 1) => (i, (1, 0, 0, 0))
          case (1, 0) => (i, (0, 1, 0, 0))
          case (0, 1) => (i, (0, 0, 1, 0))
          case (0, 0) => (i, (0, 0, 0, 1))
          case _ => throw new Exception(s"non supported type please use ChiSquareOHE")
        }
      }}
      .reduceByKey( (a, b) =>
        (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
      )
      .map{case (i, t) =>
        val A = t._1
        val B = t._2
        val C = t._3
        val D = t._4
        val curChi = (N * math.pow(A * D - C * B, 2)) / ((A + C) * (B + D) * (A + B) * (C + D))
        (i, curChi)
      }
    chi
  }

  private def getAllFeatureIndices(len: Int): Array[Int] = {
    val ret = new Array[Int](len)

    var i = 0
    while (i < len) {
      ret(i) = i

      i += 1
    }

    ret
  }

}
