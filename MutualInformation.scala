package NagaFeatureAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by zongqianliu on 2016/8/17.
  */
object MutualInformation {

  @inline private def computeEntropy(_pxy: Double, _px: Double, _py: Double): Double = {
    /** pxy * log(pxy / (px * py)) */
    if (_pxy != 0.0 && _px != 0.0 && _py != 0.0)
      _pxy * (math.log(_pxy / (_px * _py)) / math.log(2))
    else
      0.0
  }

  /**
    * Compute Mutual Information between given feature dimensions and label
    * @param sc
    * @param data RDD[String]: each line is one sample with label and features
    * @param indices Array[Int]: specify features to compute mutual information with label
    * @param delimiter String: values of ",", "\t" which can split label and features in data
    * @param label_index Int: specify which column is label
    * @return
    */
  def computeMutualInfo(sc: SparkContext, data: RDD[String],
                        label_index: Int = 0,
                        delimiter: String = "\t",
                        indices: Array[Int] = Array[Int]()): Array[(Int, Double)] = {
    val feature_len = data.first.trim.split(delimiter).length
    val _indices = if (indices.nonEmpty) indices else (0 until feature_len).filter(i => i != label_index).toArray
    val _sd = "^#" // split delimiter
    // flat data to (i, x, label) and (i, label, label)
    val flatten_data = data.flatMap { line =>
      val feature = line.trim.split(delimiter)
      val label = feature(label_index)
      _indices.flatMap { idx =>
        feature(idx).split("\\|").flatMap{
          f_x => Seq[(Int, String, String)](
            (idx,s"f${_sd}" + f_x.split(":")(0), label), // (i, x, label)
            (idx, s"l${_sd}" + label, label)             // (i, label, label)
          )}
      }
    }.persist()
    // counting (i, x) and (i, label)
    val cXorY = flatten_data.map{case (i, x, y) => ((i, x), 1)}.reduceByKey(_ + _).persist

    // counting instances of (i) column
    val cInstance = cXorY.filter{case ((i, x), cnt) => x.startsWith(s"f${_sd}")}
      .map{case ((i, x), cnt) => (i, cnt)}
      .reduceByKey(_ + _)
      .collectAsMap()
    // broadcast
    val bCInstance = sc.broadcast(cInstance)
    val bCXorY = sc.broadcast(cXorY.collectAsMap())
    // calculate mutual information
    val MI = flatten_data.filter{case (i, x, y) => x.startsWith(s"f${_sd}")}.map{case (i, x, y) => ((i, x, y), 1)}
      .reduceByKey(_ + _)
      .map{case ((i, x, y), cnt) =>
        val N = bCInstance.value(i).toDouble
        val _pXY = cnt / N
        val _pX = bCXorY.value((i, x)) / N
        val _pY = bCXorY.value((i, s"l${_sd}$y")) / N
        (i, computeEntropy(_pXY, _pX, _pY))
      }
      .reduceByKey(_+_)
      .collect()
    // free memory
    flatten_data.unpersist()
    cXorY.unpersist()
    // return
    MI

  }
}
