package NagaFramework

import NagaFramework.DiscretizeType._
import NagaFramework.FeatureType._

import scala.collection._
import scala.collection.immutable.Set

/**
  * @author gavinzjchao@tencent.com all rights reserved
  * @param numFeatures feature numbers, support for scalable, dynamic feature enrichment
  * @param normalizeThreshold dropping low frequency features
  * @param paramsSize feature's parameter size
  */
class Formatter(val numFeatures: Int, val normalizeThreshold: Int = 5, private val paramsSize: Int = 13) extends Serializable {
  var formatter: Array[Feature] = Array[Feature]()
  var interactioner: Array[(String, Double, Boolean, Array[Int])] = Array[(String, Double, Boolean, Array[Int])]()
  var labeler: Map[String, Double] = Map[String, Double]()

  def getParamsSize: Int = paramsSize

  /** recursive feature interactions, tiny & beautiful */
  private def combine(
       m: Array[Array[(String, Double)]],
       indices: Array[Int],
       isWeighted: Boolean,
       start: Int,
       result: mutable.ArrayBuffer[(String, Double)],
       holdString: String,
       holdWeight: Double): Unit = {

    if (start == indices.length) {
      result += ((holdString.drop(1), holdWeight))
    }
    else {
      val idx = indices(start)
      var i = 0
      while (i < m(idx).length) {
        val (curFeature, curWeight) = m(idx)(i)
        val _curWeight = if (isWeighted) curWeight else 1.0
        combine(m, indices, isWeighted, start + 1, result, s"${holdString}_$curFeature", holdWeight * _curWeight)
        i += 1
      }
    }
  }

  private def setInteractions(matrix: Array[Array[(String, Double)]]): Array[Array[(String, Double)]] = {
    val ret = mutable.ArrayBuffer[Array[(String, Double)]]()

    for (curInteraction <- interactioner) {
      val curArray = mutable.ArrayBuffer[(String, Double)]()
      val (_, _, isWeighted, indices) = curInteraction
      combine(matrix, indices, isWeighted, 0, curArray, "", 1.0)
      ret += curArray.toArray
    }

    ret.toArray
  }

  def getEncodedInteractionFeature(matrix: Array[Array[(String, Double)]]): Array[Array[String]] = {
    setInteractions(matrix).map(arr => arr.map(_._1))
  }

  def getTransformedInteractionFeature(matrix: Array[Array[(String, Double)]]): Array[Array[(String, Double)]] = {
    setInteractions(matrix)
  }

  private def getUnique(line: String): Feature = {
    val splits = line.split("@", -1)
    assert(splits.length == 2, s"$line is not label format")
    val params = splits(1).split(";", -1)
    assert(params.length >= 14, s"${params.mkString(",")} length is ${params.length} not equal to $paramsSize")
    require(params(0).trim.nonEmpty, s"Empty name found, not allowed")

    // 1. parse parameters
    val name: String = params(0).trim
    /** FeatureType */
    val fType: FeatureType = try {
      FeatureType.withName(params(1).trim)
    } catch {
      case e: NoSuchElementException => FeatureType.CATEGORICAL
    }
    val isSkip: Boolean = if (params(2).trim.toLowerCase == "true" || params(2).trim == "1") true else false
    val isWeighted: Boolean = if (params(3).trim.toLowerCase == "true" || params(3).trim == "1") true else false
    val unseen: Int = try {
      params(4).trim.toInt
    } catch {
      case e: Exception => -1
    }

    val isInclude: Boolean = if (params(5).trim.toLowerCase == "false" || params(5).trim == "0") false else true
    val isComplex: Boolean = if (params(6).trim.toLowerCase == "true" || params(6).trim == "1") true else false

    val isHashing: Boolean = if (params(7).trim.toLowerCase == "true" || params(7).trim == "1") true else false
    val hashingRange: Int = try {
      params(8).trim.toInt
    } catch {
      case e: Exception => 1 << 15
    }
    val stopWords: Option[Set[String]] = {
      if (params(9).trim.nonEmpty)
        Some(params(9).trim.split(":").toSet)
      else
        None
    }
    val countingThreshold: Int = try {
      params(10).trim.toInt
    } catch {
      case e: Exception => 0
    }
    val lowerBound: Option[Double] = try {
      Some(params(11).trim.toDouble)
    } catch {
      case e: Exception => None
    }
    val upperBound: Option[Double] = try {
      Some(params(12).trim.toDouble)
    } catch {
      case e: Exception => None
    }
    val discretize: Option[DiscretizeType] = try {
      Some(DiscretizeType.withName(params(13).trim))
    } catch {
      case e: NoSuchElementException => None
    }
    val bins: Option[Array[Double]] = try {
      if (params(14).trim.isEmpty)
        None
      else {
        val intervals = params(14).trim.split(":")
        val arr = mutable.ArrayBuffer[Double]()
        for (num <- intervals) {
          if (num == "-inf")
            arr += Double.NegativeInfinity
          else {
            if (num == "inf")
              arr += Double.PositiveInfinity
            else {
              val _num = try {
                num.toDouble
              } catch {
                case e: Exception => throw new Exception(s"bins contains non double strings")
              }

              arr += _num
            }
          }
        }
        if (arr.isEmpty) None else Some(arr.toArray)
      }
    }

    val paramsClass = new Params(isSkip, isWeighted, unseen, isInclude, isComplex, isHashing, hashingRange, stopWords, countingThreshold, lowerBound, upperBound, discretize, bins)
    generateFeatures(fType, name, paramsClass)
  }

  @inline
  private def generateFeatures(fType: FeatureType, name: String, params: Params): Feature = {
    val feature = fType match {
      case CATEGORICAL =>
        new CategoryFeature(name)
      case NUMERICAL =>
        new NumericalFeature(name)
      case ORDINAL =>
        new OrdinalFeature(name)
      case TEXTUAL =>
        new TextualFeature(name)
      case _ =>
        throw new IllegalArgumentException(s"not support feature type $fType")
    }

    feature.init(params)
    feature
  }

  private def getLabel(line: String): mutable.HashMap[String, Double] = {
    val splits = line.split("@", -1)
    assert(splits.length == 2, s"$line is not label format")
    val ret = mutable.HashMap[String, Double]()

    val labelsWithValues = splits(1).split(";", -1)
    for (labelWithValue <- labelsWithValues) {
      try {
        if (labelWithValue.nonEmpty) {
          val pairs = labelWithValue.split(":")
          assert(pairs.length >= 2, s"$labelWithValue's length is not equal to 2")
          val v = pairs(1).toDouble
          ret += ((pairs(0).trim, v))
        }
      }  catch {
        case e: Exception => throw new RuntimeException(s"illegal label value: $labelWithValue ")
      }
    }

    ret
  }

  private def getCross(line: String): (String, Double, Boolean, Array[String]) = {
    val splits = line.split("@", -1)
    assert(splits.length == 2, s"$line is not cross format")

    val crossInfo = splits(1).split(";", -1)
    assert(crossInfo.length >= 3, s"$line size is ${crossInfo.length} illegal")
    val isWeighted = if (crossInfo(1).trim.toLowerCase == "true" || crossInfo(1).trim == "1") true else false
    val threshold = try { crossInfo(2).toDouble } catch { case e: Exception => 0.0}
    val features = crossInfo(0).split(":").map(_.trim)
    assert(features.length > 1, s"cross need at least two features")
    val featureName = features.mkString("_")

    (featureName, threshold, isWeighted, features)
  }

  @inline
  private def getInteractioner(
      interactionerBuffer: mutable.ArrayBuffer[(String, Double, Boolean, Array[String])],
      nameToIndexMapBuffer: mutable.Map[String, Int]): Array[(String, Double, Boolean, Array[Int])] = {
    try {
      interactionerBuffer.map{ case (fName, threshold, isWeighted, fNameArr) =>
        (fName, threshold, isWeighted, fNameArr.map(nameToIndexMapBuffer.apply))
      }.toArray
    } catch {
      case e: Exception => throw new Exception(s"wrong feature name in cross features")
    }
  }

  def load(lines: Array[String]): Boolean = {
    val formatterBuffer = mutable.ArrayBuffer[Feature]()
    val nameToIndexMapBuffer = mutable.Map[String, Int]()
    val interactionerBuffer = mutable.ArrayBuffer[(String, Double, Boolean, Array[String])]()
    var labelerBuffer = mutable.HashMap[String, Double]()

    var idx = 0
    for (line <- lines) {
      val splits = line.split("@", -1)

      splits(0).trim match {
        case "UNIQUE" =>
          val curFeature = getUnique(line)
          formatterBuffer += curFeature
          nameToIndexMapBuffer += ((curFeature.name, idx))
          idx += 1
        case "CROSS" =>
          interactionerBuffer += getCross(line)
        case "LABEL" =>
          labelerBuffer = getLabel(line)
        case _ =>
          val dummy = "dummy"
      }
    }

    require(formatterBuffer.size == numFeatures,
      s"formatter size: ${formatterBuffer.size} is not equal to numFeatures: $numFeatures")
    formatter = formatterBuffer.toArray
    interactioner = getInteractioner(interactionerBuffer, nameToIndexMapBuffer)
    labeler = labelerBuffer.toMap

    true
  }
}
