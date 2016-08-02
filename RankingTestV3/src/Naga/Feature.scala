package Naga

/**
  * @author gavinzjchao@tencent.com all rights reserved
  */

import scala.collection.mutable

object FeatureType extends Enumeration {
  type FeatureType = Value
  val CATEGORIES, NUMERICAL, ORDINAL, TEXTUAL, COMPACT = Value
}

object DiscretizeType extends Enumeration {
  type DiscretizeType = Value
  val DUMMY, BINARIZE, BUCKETIZE, FREQUENCY, DISTANCE = Value
}

object TextualType extends Enumeration {
  type TextualType = Value
  val OHE, HASH = Value
}

import Naga.DiscretizeType._
import Naga.FeatureType._

case class Params(
     isSkip: Boolean = false,
     isWeighted: Boolean = false,
     defaultWeight: Double = 0.0,
     unseen: Int = -1,
     isInclude: Boolean = true,
     isHashing: Boolean = false,
     hashingRange: Int = 1024,
     stopWords: Option[Set[String]] = None,
     countingThreshold: Int = 0,
     lowerBound: Option[Double] = None,
     upperBound: Option[Double] = None,
     discretize: Option[DiscretizeType] = None,
     bins: Option[Array[Double]] = None)

abstract class Feature(val name: String, val fType: FeatureType = CATEGORIES) extends Serializable {
  var isSkip: Boolean = false
  var isWeighted: Boolean = false
  var defaultWeight: Double = 0.0
  var unseen: Int = -1
  var isInclude: Boolean = true
  var isHashing: Boolean = false
  var hashingRange: Int = 1024
  var stopWords: Option[Set[String]] = None
  var countingThreshold: Int = 0
  var lowerBound: Option[Double] = None
  var upperBound: Option[Double] = None
  var discretize: Option[DiscretizeType] = None
  var bins: Option[Array[Double]] = None

  def init(params: Params): this.type = {
    this.isSkip = params.isSkip
    this.isWeighted = params.isWeighted
    this.defaultWeight = params.defaultWeight
    this.unseen = params.unseen
    this.isInclude = params.isInclude
    this.isHashing = params.isHashing
    this.hashingRange = params.hashingRange
    this.stopWords = params.stopWords
    this.countingThreshold = params.countingThreshold
    this.lowerBound = params.lowerBound
    this.upperBound = params.upperBound
    this.discretize = params.discretize
    this.bins = params.bins
    this
  }



  def getEncodedFeature[T](value: T): String

  def getDeafualtValue(): String = {
    fType match {
      case NUMERICAL => name
      case COMPACT => unseen.toString
      case TEXTUAL => {
        val w = if (isWeighted) defaultWeight else 1.0
        if(isHashing)
          s"${new MurMur3Improved(hashingRange).hashingTrick(encodeFeature(name, unseen.toString)).toString}|$w"
        else
          s"${encodeFeature(name, unseen.toString)}|$w"
      }
      case _ => encodeFeature(name, unseen.toString)
    }
  }

  def getWeight[T](value: T): Double

  def transformFeature[T](value: T): (String, Double)

  protected def encodeFeature(name: String, value: String): String = {
    name + "^" + value
  }

  protected def _valid(s: String): Boolean = {
    if (s.isEmpty || s == "\\N") false else true
  }
}

class CategoryFeature(override val name: String, override val fType: FeatureType = CATEGORIES) extends Feature(name, fType) {
  def getEncodedFeature[T](value: T): String = {
    if (!_valid(value.toString)) {
      if (isSkip)
        return "SKIP"
      else
        return encodeFeature(name, unseen.toString)
    }

    val v = value.toString.split(":")(0)
    if (!_valid(v)) {
      if (isSkip)
        return "SKIP"
      else
        return encodeFeature(name, unseen.toString)
    }

    if (stopWords.isDefined && stopWords.get.contains(v)) {
      if (isSkip) "SKIP" else encodeFeature(name, unseen.toString)
    }
    else{
      encodeFeature(name, v.toString)
    }
  }

  def getWeight[T](value: T): Double = {
    if (!_valid(value.toString))
      return defaultWeight

    val pair = value.toString.split(":")
    if (isWeighted) {
      if (pair.length == 2) {
        try {
          pair(1).toDouble
        }
        catch {
          case e: Exception => defaultWeight
        }
      }
      else
        defaultWeight
    }
    else {
      1.0
    }
  }

  def transformFeature[T](value: T): (String, Double) = {
    (getEncodedFeature(value), getWeight(value))
  }
}

class TextualFeature(override val name: String, override val fType: FeatureType = TEXTUAL) extends Feature(name, fType) {
  @inline private def _getTextualDefault(): String = {
    val w = if (isWeighted) defaultWeight else 1.0
    if(isHashing)
      s"${new MurMur3Improved(hashingRange).hashingTrick(encodeFeature(name, unseen.toString)).toString}|$w"
    else
      s"${encodeFeature(name, unseen.toString)}|$w"
  }

  def _getEncodedFeature[T](value: T): String = {
    if (!_valid(value.toString)) {
      if (isSkip)
        return "SKIP"
      else
        return encodeFeature(name, unseen.toString)
    }

    val v = value.toString.split(":")(0)
    if (!_valid(v)) {
      if (isSkip)
        return "SKIP"
      else
        return encodeFeature(name, unseen.toString)
    }

    if (stopWords.isDefined && stopWords.get.contains(v)) {
      if (isSkip) "SKIP" else encodeFeature(name, unseen.toString)
    }
    else{
      encodeFeature(name, v.toString)
    }
  }

  def _getWeight[T](value: T): Double = {
    if (!_valid(value.toString))
      return defaultWeight

    val pair = value.toString.split(":")
    if (isWeighted) {
      if (pair.length == 2) {
        try {
          pair(1).toDouble
        }
        catch {
          case e: Exception => defaultWeight
        }
      }
      else
        defaultWeight
    }
    else {
      1.0
    }
  }

  def getWeight[T](value: T): Double = {
    1.0
  }

  // trick: (word1:word2:word3:...:wordn|weight1:weight2:...:weightn, 1.0)
  def getEncodedFeature[T](value: T): String = {
    if (!_valid(value.toString)) {
      if (isSkip)
        return "SKIP"
      else
        _getTextualDefault()
    }

    val splits = value.toString.split("\\|")
    var head = ""
    val distincter = mutable.HashSet[String]()
    val ws = mutable.ArrayBuffer[Double]()
    var maxW = Double.NegativeInfinity
    var minW = Double.PositiveInfinity
    var i = 0

    val murmur: Option[MurMur3Improved] = if (isHashing) Some(new MurMur3Improved(hashingRange)) else None
    while (i < splits.length) {
      var _v = _getEncodedFeature[String](splits(i))

      // textual features hashing trick: murmur3 hash
      if (isHashing) {
        _v = murmur.get.hashingTrick(_v).toString
      }
      val _w = _getWeight[String](splits(i))

      if (_v != "SKIP" && !distincter.contains(_v)) {

        distincter.add(_v)
        head += s"${_v}:"
        // scaling
        ws += _w
        if (_w > maxW) {
          maxW = _w
        }
        else {
          if (_w < minW) {
            minW = _w
          }
        }
      }

      i += 1
    }

    if (head.isEmpty || ws.isEmpty || maxW <= 0){
      if (isSkip)
        "SKIP"
      else
        encodeFeature(name, unseen.toString) + "|" + "1.0"
    }
    else {
      var tail = ""
      for (w <- ws) {
        tail += s"${(1.0 * w / 100).formatted("%.4f")}:"
      }

      s"${head.dropRight(1)}|${tail.dropRight(1)}"
    }
  }

  def transformFeature[T](value: T): (String, Double) = {
    (getEncodedFeature(value), 1.0)
  }
}

class NumericalFeature (override val name: String, override val fType: FeatureType = NUMERICAL) extends Feature(name, fType) {
  def getEncodedFeature[T](value: T): String = {
    if (!_valid(value.toString)) {
      if (isSkip)
        return "SKIP"
      else
        return name
    }

    var v = try {
      val tmp = value.toString
      tmp.toDouble
    }
    catch {
      case e: Exception => Double.NegativeInfinity
    }

    if (lowerBound.isDefined && v < lowerBound.get || upperBound.isDefined && v > upperBound.get)
      v = Double.NegativeInfinity

    if (v.equals(Double.NegativeInfinity) && isSkip) "SKIP" else name
  }

  def getWeight[T](value: T): Double = {
    var v = try {
      val tmp = value.toString
      tmp.toDouble
    }
    catch {
      case e: Exception => Double.NegativeInfinity
    }

    if (lowerBound.isDefined && v < lowerBound.get || upperBound.isDefined && v > upperBound.get)
      v = Double.NegativeInfinity

    if (v.equals(Double.NegativeInfinity)) defaultWeight else v
  }

  // here do not transform value to name_value, just keep it
  def transformFeature[T](value: T): (String, Double) = {
    (value.toString, getWeight(value))
  }
}

class OrdinalFeature(override val name: String, override val fType: FeatureType = ORDINAL) extends Feature(name, fType) {
  def getEncodedFeature[T](value: T): String = {
    if (!_valid(value.toString)) {
      if (isSkip)
        return "SKIP"
      else
        return encodeFeature(name, unseen.toString)
    }

    val v = try {
      val tmp = value.toString
      tmp.toDouble
    }
    catch {
      case e: Exception => Double.NegativeInfinity
    }

    if (lowerBound.isDefined && v < lowerBound.get
      || upperBound.isDefined && v > upperBound.get
      || stopWords.isDefined && stopWords.get.contains(v.formatted("%.0f"))) {
      if (isSkip)
        return "SKIP"
      else
        return encodeFeature(name, unseen.toString)
    }

    // if not define discrete type then use dummy as default
    val dis = if (discretize.isEmpty) DUMMY else discretize.get
    dis match {
      case DUMMY =>
        encodeFeature(name, v.formatted("%.0f"))
      case BINARIZE =>
        require(bins.getOrElse(Array[Int]()).length == 1,
          s"[ERROR] ordinal features, choose type $discretize but bins's size is ${bins.size}, not equal to 1")
        val pivot: Double = bins.get(0)
        val idx = if (v <= pivot) "0" else "1"
        encodeFeature(name, idx)
      case BUCKETIZE =>
        require(bins.getOrElse(Array[Int]()).nonEmpty,
          s"[ERROR] ordinal features, choose type $discretize but bins's size is 0")
        val bucket = binarySearchForBuckets(bins.get, v)
        if (bucket < 0) {if (isSkip) "SKIP" else encodeFeature(name, unseen.toString) } else encodeFeature(name, bucket.toInt.toString)
      case _ =>
        throw new RuntimeException(s"unsupported discretize type")
    }
  }


  def getWeight[T](value: T): Double = {
    if (!_valid(value.toString))
      return defaultWeight

    val pair = value.toString.split(":")
    if (isWeighted) {
      if (pair.length == 2) {
        try {
          pair(1).toDouble
        }
        catch {
          case e: Exception => defaultWeight
        }
      }
      else
        defaultWeight
    }
    else {
      1.0
    }
  }

  def transformFeature[T](value: T): (String, Double) = {
    (getEncodedFeature(value), getWeight(value))
  }

  def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
    if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = java.util.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          if (isSkip) Double.NegativeInfinity else unseen.toDouble
        } else {
          insertPos - 1
        }
      }
    }
  }

}

class CompactFeature(override val name: String, override val fType: FeatureType = COMPACT) extends Feature(name, fType) {
  def getEncodedFeature[T](value: T): String = {
    if (!_valid(value.toString)) {
      if (isSkip)
        return "SKIP"
      else
        return unseen.toString
    }

    val v = try {
      val tmp = value.toString
      tmp.toDouble
    }
    catch {
      case e: Exception => unseen
    }

    if (lowerBound.isDefined && v < lowerBound.get
      || upperBound.isDefined && v > upperBound.get
      || stopWords.isDefined && stopWords.get.contains(value.toString)) {
      if (isSkip) {
        "SKIP"
      }
      else {
        encodeFeature(name, unseen.toString)
      }

    }
    else {
      encodeFeature(name, v.formatted("%.0f"))
    }
  }

  def getWeight[T](value: T): Double = {
    1.0
  }

  // return original value so I can generate sparse vector:
  //  Vector.sparse(upperbound, Array(_getFeatureValue(value)), Array(1.0))
  private def _getFeatureValue[T](value: T): String = {
    if (!_valid(value.toString)) {
      if (isSkip)
        return "SKIP"
      else
        return unseen.toString
    }

    val v = try {
      val tmp = value.toString
      tmp.toDouble
    }
    catch {
      case e: Exception => unseen.toDouble
    }

    if (lowerBound.isDefined && v < lowerBound.get
      || upperBound.isDefined && v > upperBound.get
      || stopWords.isDefined && stopWords.get.contains(value.toString)) {
      if (isSkip) {
        "SKIP"
      }
      else {
        unseen.toString
      }

    }
    else {
      v.formatted("%.0f")
    }
  }

  def transformFeature[T](value: T): (String, Double) = {
    require(upperBound.isDefined, s"Compact feature's upper bound must be specified")
    (_getFeatureValue(value), 1.0)
  }
}
