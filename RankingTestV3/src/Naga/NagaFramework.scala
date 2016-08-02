package Naga

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object NagaFramework {
  def featureEngineering(sc: SparkContext, numFeatures: Int, paramsSize: Int, formatterPath: Array[String], samples: RDD[(String, String, Array[String])], testDate: String): RDD[LabeledPoint] = {
    // 3.1 initialize formatter
    val formatter = new Formatter(numFeatures, paramsSize)
    formatter.load(formatterPath)
    val bcFormatter = sc.broadcast(formatter)

    // 3.2 initialize container
    val container = new Container(formatter)

    // 3.3 construct namePool
    val encodedSamples = container.transform(samples, bcFormatter.value, testDate)
      .persist(StorageLevel.MEMORY_ONLY)

    val namepool = container.setNamePool(encodedSamples.map(_._3), bcFormatter.value)

    // 3.4 construct NamePoolWithOffset
    val npwo = new NamePoolWithOffset(namepool)

    npwo.fit()
    val bcNamePoolWithOffset = sc.broadcast(npwo)

    // 3.5 fit encodedSamples to labeledPoint
    val trainData = container.fit(encodedSamples.map(x => x._2 -> x._3), bcNamePoolWithOffset.value)

//    //TODO
//    trainData.take(100).foreach { x =>
//      val label = x.label.toString
//      val Vec = x.features
//      val idx = Vec.toSparse.indices.mkString("{", ",", "}")
//      val vals = Vec.toSparse.values.mkString("{", ",", "}")
//      val sz = Vec.toSparse.size.toString
//
//      println(s"label: ${x.label}, features size: $sz, features indices: $idx, features values: $vals")
//    }

    //    val indexToFeaturePool = container.getIdxToFeaturePool /** saving this for deserialization */

    // 3.6 clear all caching data for feature engineering & caching all train data and indexToFeaturePool
    //    bcFormatter.unpersist()
    //    bcFeaturePool.unpersist()
    //    encodedSamples.unpersist()

    trainData
  }
}
