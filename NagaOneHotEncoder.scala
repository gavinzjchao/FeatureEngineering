package NagaFramework

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection._

case class RevertFeature(feature: String, sampleID: Int, weight: Double, formatterID: Int)
case class HighFreqFeatureList(sampleID: Int, label: Double, features: Array[(Int, Double)])

class NagaOneHotEncoder(val formatter: Formatter, val numPartitions: Int, val highFrequencyThreshold: Int = 40, val countingThreshold: Int = 5) extends Serializable {
  var featureSpace = -1
  val parter = new HashPartitioner(numPartitions)

  /** feature transform factory */
  private def _getUniqueFeature(rawLabel: String, sampleID: Int, features: Array[String], numFeatures: Int):
      Option[(Double, Int, Array[Array[(String, Double)]])] = {

    val label = formatter.labeler.getOrElse(rawLabel, -2.0)
    val curNumFeatures = features.length
    if (label == -2.0 || curNumFeatures != numFeatures) {
      None
    }
    else {
      val curMatrix = mutable.ArrayBuffer[Array[(String, Double)]]()

      var i = 0
      while (i < features.length) {
        val curFeature = features(i)
        val curArr = formatter.formatter(i).getTransformedFeature(curFeature)
        curMatrix += curArr

        i += 1
      }
      Some(label, sampleID, curMatrix.toArray)
    }
  }

  private def getUniqueFeatures(sc: SparkContext, input: RDD[(String, Int, Array[String])], numFeatures: Int) = {
    val uniVariables = input.mapPartitions( iter => {
      val partialArr = mutable.ArrayBuffer[(Double, Int, Array[Array[(String, Double)]])]()
      while (iter.hasNext) {
        val cur = iter.next
        val curUniqueFeature = _getUniqueFeature(cur._1, cur._2, cur._3, numFeatures)
        if (curUniqueFeature.isDefined) partialArr += curUniqueFeature.get
      }
      partialArr.iterator
    }, preservesPartitioning = true)

    uniVariables
  }

  private def getCrossFeatures(
      sc: SparkContext,
      uniVariables: RDD[(Double, Int, Array[Array[(String, Double)]])],
      numFeatures: Int): RDD[(Double, Int, Array[Array[(String, Double)]])] = {

    uniVariables.map { case (label, sampleID, features) =>
      (label, sampleID, features ++ formatter.getTransformedInteractionFeature(features))
    }

  }

  private def _getFinalVariables(matrix: Array[Array[(String, Double)]], numFeatures: Int): Array[(String, Double, Int)] = {
    val ret = mutable.ArrayBuffer[(String, Double, Int)]()

    var idx = 0
    while (idx < matrix.length) {
      if (idx < numFeatures && formatter.formatter(idx).isInclude || idx >= numFeatures)
        ret ++= matrix(idx).map{ case (fea, wei) => (fea, wei, idx)}
      idx += 1
    }

    ret.toArray
  }


  private def getFinalVariables(
     uniqueAndCrossVariables: RDD[(Double, Int, Array[Array[(String, Double)]])],
     numFeatures: Int): RDD[(Double, Int, Array[(String, Double, Int)])] = {

    uniqueAndCrossVariables.mapPartitions( iter => {
      val partialArr = mutable.ArrayBuffer[(Double, Int, Array[(String, Double, Int)])]()
      while (iter.hasNext) {
        val ret = mutable.ArrayBuffer[(String, Double, Int)]()
        val (label, sampleID, mat) = iter.next

        var idx = 0
        while (idx < mat.length) {
          if (idx < numFeatures && formatter.formatter(idx).isInclude || idx >= numFeatures)
            ret ++= mat(idx).map{ case (fea, wei) => (fea, wei, idx)}
          idx += 1
        }

        partialArr += ((label, sampleID, ret.toArray))
      }
      partialArr.iterator
    }, preservesPartitioning = true)

  }

  def transform(sc: SparkContext, input: RDD[(String, Int, Array[String])]):
      RDD[(Double, Int, Array[(String, Double, Int)])] = {
    val finalVariables = input.mapPartitions( iter => {
      val partialArr = mutable.ArrayBuffer[(Double, Int, Array[(String, Double, Int)])]()
      val numFeatures = formatter.numFeatures

      while (iter.hasNext) {
        val cur = iter.next
        val uniqueFeaturesWithLabel = _getUniqueFeature(cur._1, cur._2, cur._3, numFeatures)
        if (uniqueFeaturesWithLabel.isDefined) {
          val (label: Double, sampleID: Int, uniqueFeatures: Array[Array[(String, Double)]]) = uniqueFeaturesWithLabel.get
          val crossFeatures = formatter.getTransformedInteractionFeature(uniqueFeatures)
          val features = _getFinalVariables(uniqueFeatures ++ crossFeatures, numFeatures)

          partialArr += ((label, sampleID, features))
        }
      }
      partialArr.iterator
    })

    finalVariables
  }

  def transform2(sc: SparkContext, input: RDD[(String, Int, Array[String])]):
      RDD[(Double, Int, Array[(String, Double, Int)])] = {

    // 1. transform and filtering uni-variable
    val uniVariables = getUniqueFeatures(sc, input, formatter.numFeatures)

    // 2. transform cross-variable
    val uniqueAndCrossVariables = getCrossFeatures(sc, uniVariables, formatter.numFeatures)

    // 3. flatten all features to Array
    getFinalVariables(uniqueAndCrossVariables, formatter.numFeatures)
  }

  /** feature encoding core */
  def fit(sc: SparkContext, input: RDD[(Double, Int, Array[(String, Double, Int)])], featureDictPath: String):
      (mutable.HashMap[Int, String], RDD[(Int, LabeledPoint)]) = {

    // 1. get feature dict: <featureName, featureCount, featureIndex>
    val (lowFrequencyRDD, highFrequencyDict, featureDict) = getFeatureDict(sc, input, featureDictPath)
    lowFrequencyRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // 2. high frequency features encoding based on HashMap
    val bcHFDict = sc.broadcast(highFrequencyDict)
    val (encodedHighFreqFeatures, revertedLowFreqFeatures) = encodeHighAndLowFeatures(input, bcHFDict.value)

    // 3. low frequency features generate revert index and join with feature dict:
    //    low frequency features: <featureName, sampleID, featureWeight, formatterID>
    //    feature dict: <featureName, featureCount, featureIndex>
    val encodedLowFreqFeatures = getLowFreqFeatures(lowFrequencyRDD, revertedLowFreqFeatures)

    // 4. group low frequency features by sampleID and leftOuterJoin to high frequency
    //    features' RDD
    val samples = getLabeledPoint(encodedHighFreqFeatures.map(hff => hff.sampleID -> hff), encodedLowFreqFeatures).persist(StorageLevel.MEMORY_ONLY)

    // 5. clean all cache data sets and return labeled point

    println(s"Total Samples Size: ${samples.count}")
    lowFrequencyRDD.unpersist(false)
    bcHFDict.unpersist(false)
    encodedHighFreqFeatures.unpersist(false)
    revertedLowFreqFeatures.unpersist(false)
    encodedLowFreqFeatures.unpersist(false)
    input.unpersist(false)

    // 6. return final result
    (featureDict, samples)
  }

  private def getLabeledPoint(
     encodedHighFreqFeatures: RDD[(Int, HighFreqFeatureList)],
     encodedLowFreqFeatures: RDD[(Int, Array[(Int, Double)])]): RDD[(Int, LabeledPoint)] = {

    encodedHighFreqFeatures.partitionBy(parter)
      .leftOuterJoin(encodedLowFreqFeatures)
      .map{ case ((sampleID: Int, (hff: HighFreqFeatureList, lff: Option[Array[(Int, Double)]]))) =>
        val _features = sortAndDistinct(hff.features ++ lff.getOrElse(Array.empty[(Int, Double)]))
        (sampleID, LabeledPoint(hff.label, Vectors.sparse(featureSpace, _features)))
      }
  }

  private def sortAndDistinct(iter: Array[(Int, Double)]): Array[(Int, Double)] = {
    if (iter.length <= 1) return iter

    /** TODO[gavinzjchao] here I drop the features which is less than normalizeThreshold */
    val sortedArr = iter.filter(_._1 != 0).sortBy(_._1)
    val res = mutable.ArrayBuffer[(Int, Double)](sortedArr(0))

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

  private def getLowFreqFeatures(lowFrequencyRDD: RDD[(String, (Int, Int))], revertedLowFreqFeatures: RDD[(String, RevertFeature)]):
      RDD[(Int, Array[(Int, Double)])] = {

    val rdd = revertedLowFreqFeatures.leftOuterJoin(lowFrequencyRDD)
      .map{ case ((feature: String, (reverted: RevertFeature, featureInfo: Option[(Int, Int)]))) =>
        val sampleID = reverted.sampleID
        val weight = reverted.weight
        val featureIdx = {
          if (featureInfo.isDefined) {
            featureInfo.get._2
          }
          else {
            getDefaultFeatureIndex(reverted.formatterID)
          }
        }
        (sampleID, (featureIdx, weight))
      }

    rdd.groupByKey(parter).map(pair => pair._1 -> pair._2.toArray)
  }

  @inline private def getDefaultFeatureIndex(formatterID: Int): Int = {
    // TODO using formatter to set specific values
    0
  }

  private def encodeHighAndLowFeatures(
      input: RDD[(Double, Int, Array[(String, Double, Int)])],
      highFreqFeatures: mutable.HashMap[String, (Int, Int)]): (RDD[HighFreqFeatureList], RDD[(String, RevertFeature)]) = {

    val _highAndLow = input.mapPartitions( iter => {
      val partialArr = mutable.ArrayBuffer[(HighFreqFeatureList, Array[RevertFeature])]()

      while (iter.hasNext) {
        val (label, sampleID, rawFeatures) = iter.next()

        val _high = mutable.ArrayBuffer[(Int, Double)]()
        val revertFeatureArr = mutable.ArrayBuffer[RevertFeature]()
        for ((fea, wei, formatterID) <- rawFeatures) {
          if (highFreqFeatures.contains(fea)) {
            val featureIdx = highFreqFeatures(fea)._2
            _high += ((featureIdx, wei))
          }
          else {
            revertFeatureArr += RevertFeature(fea, sampleID, wei, formatterID)
          }
        }

        val highFreq = HighFreqFeatureList(sampleID, label, _high.toArray)
        partialArr += ((highFreq, revertFeatureArr.toArray))
      }
      partialArr.iterator
    })

    val encodedHighFreqFeatures = _highAndLow.map(_._1).persist(StorageLevel.MEMORY_ONLY)
    val revertedLowFreqFeatures = _highAndLow.flatMap(x => x._2.map(rf => rf.feature -> rf))
      .partitionBy(parter)
      .persist(StorageLevel.MEMORY_ONLY)

    (encodedHighFreqFeatures, revertedLowFreqFeatures)
  }

  private def getUniqueDefaultValues(): Array[(String, Int)] = {
    val ret = mutable.ArrayBuffer[(String, Int)]()

    for (feature <- formatter.formatter) {
      val k = feature.getDefaultFeature
      ret += ((k, 100))
    }

    ret.toArray
  }

  private def getCrossDefaultValues(): Array[(String, Int)] = {
    val ret = mutable.ArrayBuffer[(String, Int)]()
    for (crosser <- formatter.interactioner) {
      val indices = crosser._4
      var k = ""
      for (i <- indices) {
        k += s"${formatter.formatter(i).getDefaultFeature}_"
      }

      ret += ((k.dropRight(1), 100))
    }

    ret.toArray
  }

  private def getFeatureDict(sc: SparkContext, input: RDD[(Double, Int, Array[(String, Double, Int)])], featureDictPath: String):
      (RDD[(String, (Int, Int))], mutable.HashMap[String, (Int, Int)], mutable.HashMap[Int, String]) = {

    val _featuresRDD = input.flatMap{ case ((l:Double, s: Int, features: Array[(String, Double, Int)])) => features.map(x => x._1 -> 1)}
      .reduceByKey(_ + _)
      .filter(_._2 >= countingThreshold)
      .sortBy(pair => pair._1)
      .zipWithIndex()
      .map{case ((fea, cnt), index) => (fea, (cnt, index.toInt + 1))} /** here I use 0 as DUMMY index, which won't join to computation */


    featureSpace = _featuresRDD.count.toInt + 1
//    _featuresRDD.map{triple => triple._2._2 -> triple._1}.saveAsTextFile(featureDictPath)

    val (lowRDD, highMap) = frequencyBasedSplit(sc, _featuresRDD)
    (lowRDD, highMap, mutable.HashMap.empty[Int, String])

//    var idx = 1
//    for ((k, v) <- _featuresArr) {
//      dict += ((k, (v, idx)))
//      idx += 1
//    }
//
//    val uniqueDefaultValues = getUniqueDefaultValues()
//    for ((k, v) <- uniqueDefaultValues) {
//      if (!dict.contains(k)) {
//        dict += ((k, (v, idx)))
//        idx += 1
//      }
//    }
//
//    val crossDefaultValues = getCrossDefaultValues()
//    for ((k, v) <- crossDefaultValues) {
//      if (!dict.contains(k)) {
//        dict += ((k, (v, idx)))
//        idx += 1
//      }
//    }
//
//    /** TODO[gavinzjchao] using hard code for output partitions */
//    println(s"Feature dict size is: ${dict.size}")
//    /** (feature_index, feature_name) */
//    sc.parallelize(dict.map(triple => s"${triple._2._2}\t${triple._1}").toSeq, 20).saveAsTextFile(featureDictPath)
//
//    featureSpace = idx /** update feature space */
//    val (lowRDD, highMap) = frequencyBasedSplit(sc, dict)
//    (lowRDD, highMap, dict.map(triple => triple._2._2 -> triple._1))
  }

  private def frequencyBasedSplit(sc: SparkContext, featuresRDD: RDD[(String, (Int, Int))]):
      (RDD[(String, (Int, Int))], mutable.HashMap[String, (Int, Int)]) = {

    val lowFrequencyFeatures = featuresRDD.filter(_._2._1 < highFrequencyThreshold)

    val highFreqFeatureArr = featuresRDD.filter(_._2._1 >= highFrequencyThreshold).collect()
    val highFrequencyFeatures = mutable.HashMap[String, (Int, Int)]()
    for (triple <- highFreqFeatureArr) {
      highFrequencyFeatures += triple
    }

    (lowFrequencyFeatures, highFrequencyFeatures)
  }



  private def frequencyBasedSplit(sc: SparkContext, dict: mutable.HashMap[String, (Int, Int)]):
  (RDD[(String, (Int, Int))], mutable.HashMap[String, (Int, Int)]) = {

    val _lowDict = dict.filter(_._2._1 < highFrequencyThreshold)
    val lowFrequencyFeatures = sc.parallelize(_lowDict.toSeq, numPartitions)
      .partitionBy(parter)
      .persist(StorageLevel.MEMORY_ONLY)

    val highFrequencyFeatures = dict.filter(_._2._1 >= highFrequencyThreshold)

    (lowFrequencyFeatures, highFrequencyFeatures)
  }

}

object NagaOneHotEncoder {

  def featureEngineering(
      sc: SparkContext,
      input: RDD[(String, Int, Array[String])],
      numPartitions: Int,
      numFeatures: Int,
      formatterPath: String,
      featureDictPath: String,
      normalizeThreshold: Int = 5,
      highFrequencyThreshold: Int = 40): (mutable.HashMap[Int, String], RDD[(Int, LabeledPoint)]) = {

    val formatter = new Formatter(numFeatures, normalizeThreshold)
    val formatLines = sc.textFile(formatterPath, 1).collect()
    formatter.load(formatLines)
    formatter.formatter.foreach(x => println(s"formatter->formatter: ${x.toString}"))
    formatter.interactioner.foreach{
      x => println(s"formatter->interactioner: ${x._1}, ${x._2}, ${x._3}, ${x._4.mkString(",")}")
    }
    formatter.labeler.foreach(x => println(s"formatter->labeler: $x"))
    val bcFormatter = sc.broadcast(formatter)

    val naga = new NagaOneHotEncoder(bcFormatter.value, numPartitions, highFrequencyThreshold, normalizeThreshold)
    val transSamples = naga.transform(sc, input)
    transSamples.persist(StorageLevel.MEMORY_AND_DISK)

//    transSamples.take(100).foreach(x => println(s"${x._1}\t${x._2}\t${x._3.mkString(",")}"))
    val (featureDict, encodedSamples) = naga.fit(sc, transSamples, featureDictPath)
    encodedSamples.take(100).foreach(x => println(s"encoded samples: ${x._1}, ${x._2}"))

    transSamples.unpersist(false)
    bcFormatter.unpersist(false)

    (featureDict, encodedSamples)
  }
}
