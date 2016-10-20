package NagaFeatureAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created by zongqianliu on 2016/8/17.
  */
object ConditionalMutualInformation {

  def computeConditionalMutualInfo(sc: SparkContext, data: RDD[String],
                                   label_index: Int = 0,
                                   delimiter: String = "\t",
                                   indices_pair: Array[(Int, Int)] = Array[(Int, Int)]()):
                                    Array[((Int,Int), Double)] = {
    if (data.getStorageLevel == StorageLevel.NONE) data.cache()
    val feature_len = data.first.split(delimiter).length
    val _indices_pair = if (indices_pair.nonEmpty) indices_pair else get_indices_pair(feature_len, label_index)
    val _sd = "^#" // split delimiter
    val flatten_data = data.flatMap { arr =>
      val features = arr.trim.split(delimiter)
      val label = features(label_index)
      _indices_pair.flatMap{case (i, j) =>
        features(i).split("\\|").flatMap(f_x =>
          features(j).split("\\|").map(f_y =>
            (i, f_x.split(":")(0), j, f_y.split(":")(0), label)))}
    }.persist()
    val cXorYorZ = flatten_data.flatMap{case (i, x, j, y, z) => Seq[((Int, Int, String, String), Int)](
      ((i, j, s"x${_sd}$x", z), 1), // ((i, j, x), 1)
      ((i, j, s"y${_sd}$y", z), 1), // ((i, j, y), 1)
      ((i, j, s"z${_sd}$z", z), 1)  // ((i, j, z), 1)
    )}.reduceByKey(_ + _).persist()
    val cInstance = cXorYorZ.filter{case ((i, j, x, z), cnt) => x.startsWith(s"z${_sd}")}
      .map{case ((i, j, x, z), cnt) => ((i, j), cnt)}.reduceByKey(_ + _)
    val bCXorYorZ = sc.broadcast(cXorYorZ.collectAsMap())
    val bCInstance = sc.broadcast(cInstance.collectAsMap())

    val CMI = flatten_data.map{case (i, x, j, y, z) => ((i, x, j, y, z), 1) }.reduceByKey(_ + _)
      .map{case ((i, x, j, y, z), cnt) =>
        val N = bCInstance.value((i, j)).toDouble
        val _pXYZ = cnt / N
        val _pZ=  bCXorYorZ.value((i, j, s"z${_sd}$z", z)) / N
        val _pXZ = bCXorYorZ.value((i, j, s"x${_sd}$x", z)) / N
        val _pYZ = bCXorYorZ.value((i, j, s"y${_sd}$y", z)) / N
        ((i, j), computeConditionalEntropy(_pXYZ, _pZ, _pXZ, _pYZ))
      }.reduceByKey(_ + _).collect()

    // free memory
    flatten_data.unpersist()
    cXorYorZ.unpersist()

    CMI
  }

  @inline private def computeConditionalEntropy(_pxyz: Double, _pz:Double, _pxz: Double, _pyz: Double): Double = {
    if (closeTo(_pxyz, 0.0) || closeTo(_pz, 0.0) || closeTo(_pxz, 0.0) || closeTo(_pyz, 0.0)) {
      0.0
    }
    else {
      _pxyz * (math.log((_pz * _pxyz) / (_pxz * _pyz)) / math.log(2))
    }
  }

  // for comparison of double variables
  @inline private def closeTo(lhs: Double, rhs: Double, precision: Double = 1e-6): Boolean = {
    math.abs(lhs - rhs) <= precision
  }

  private def get_indices_pair(len: Int, label_index: Int = 0): Array[(Int, Int)] = {
    val ret = mutable.ArrayBuffer[(Int, Int)]()

    for(i <- 0 until len - 1 if i != label_index; j <- (i + 1) until len if (j != label_index)) {
      ret += ((i, j))
    }
    ret.toArray
  }

}
