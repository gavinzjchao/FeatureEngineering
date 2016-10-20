package NagaFramework

import scala.annotation.tailrec
import scala.collection.mutable
object FeatureType extends Enumeration {
  type FeatureType = Value
  val CATEGORICAL, NUMERICAL, ORDINAL, TEXTUAL = Value
}

object DiscretizeType extends Enumeration {
  type DiscretizeType = Value
  val DUMMY, BINARIZE, BUCKETIZE, FREQUENCY, DISTANCE = Value
}

object TextualType extends Enumeration {
  type TextualType = Value
  val OHE, HASH = Value
}

import NagaFramework.DiscretizeType._
import NagaFramework.FeatureType._

case class Params(
     isSkip: Boolean = false,
     isWeighted: Boolean = false,
     unseen: Int = -1,
     isInclude: Boolean = true,
     isComplex: Boolean = false,
     isHashing: Boolean = false,
     hashingRange: Int = 1 << 15,
     stopWords: Option[Set[String]] = None,
     countingThreshold: Int = 0,
     lowerBound: Option[Double] = None,
     upperBound: Option[Double] = None,
     discretize: Option[DiscretizeType] = None,
     bins: Option[Array[Double]] = None)

abstract class Feature(val name: String, val fType: FeatureType = CATEGORICAL) extends Serializable {
  var isSkip: Boolean = false
  var isWeighted: Boolean = false
  var unseen: Int = -1
  var isInclude: Boolean = true
  var isComplex: Boolean = false
  var isHashing: Boolean = false
  var hashingRange: Int = 1 << 15
  var stopWords: Option[Set[String]] = None
  var countingThreshold: Int = 0
  var lowerBound: Option[Double] = None
  var upperBound: Option[Double] = None
  var discretize: Option[DiscretizeType] = None
  var bins: Option[Array[Double]] = None
  var murmur: Option[MurMur3Improved] = None

  def init(params: Params): this.type = {
    this.isSkip = params.isSkip
    this.isWeighted = params.isWeighted
    this.unseen = params.unseen
    this.isInclude = params.isInclude
    this.isComplex = params.isComplex
    this.isHashing = params.isHashing
    this.hashingRange = params.hashingRange
    this.stopWords = params.stopWords
    this.countingThreshold = params.countingThreshold
    this.lowerBound = params.lowerBound
    this.upperBound = params.upperBound
    this.discretize = params.discretize
    this.bins = params.bins

    this.murmur = Some(new MurMur3Improved(this.hashingRange))
    this
  }

  def getEncodedFeature(value: String): Array[String]
  def getDefaultFeature: String = {
    fType match {
      case NUMERICAL => name
      case TEXTUAL   =>
        if (isHashing)
          s"${this.murmur.get.hashingTrick(encodeFeature(name, unseen.toString)).toString}"
        else
          encodeFeature(name, unseen.toString)
      case _ => encodeFeature(name, unseen.toString)
    }
  }
  def getTransformedFeature(value: String): Array[(String, Double)]

  protected def encodeFeature(name: String, value: String): String = {
    name + "^" + value
  }

  override def toString(): String = {
    s"[Feature]: name = ${name}, fType = ${fType}, isWeighted = ${isWeighted}," +
      s" unseen = ${unseen}, isInclude = ${isInclude}, isComplex = ${isComplex}," +
      s" stopWords = ${stopWords.getOrElse(Set[String]()).mkString(",")}," +
      s" lowerBound = ${lowerBound}, upperBound = ${upperBound}, discretize = ${discretize}, " +
      s" bins = ${bins.getOrElse(Array[Double]()).mkString(",")}"
  }

}

class CategoryFeature(override val name: String, override val fType: FeatureType = CATEGORICAL) extends Feature(name, fType) {
  override def getEncodedFeature(value: String): Array[String] = {
    val v = value.trim
    if (v.isEmpty) return Array[String](encodeFeature(name, unseen.toString))

    isComplex match {
      case true =>
        getComplexCatFeature(v).map(_._1)
      case _ =>
        Array[String](getSingleCatFeature(v)._1)
    }
  }

  override def getTransformedFeature(value: String): Array[(String, Double)] = {
    val v = value.trim
    if (v.isEmpty) return Array[(String, Double)]((encodeFeature(name, unseen.toString), 1.0))

    isComplex match {
      case true =>
        getComplexCatFeature(v)
      case _ =>
        Array[(String, Double)](getSingleCatFeature(v))
    }
  }

  private def multiCondMatches(value: String): (String, Boolean) = {
    if (value.nonEmpty && (stopWords.isEmpty || !stopWords.get.contains(value)))
      (encodeFeature(name, value), true)
    else
      (encodeFeature(name, unseen.toString), false)
  }

  private def getSingleCatFeature(value: String): (String, Double) = {
    isWeighted match {
      case true =>
        val splits = value.split(":")
        if (splits.length != 2)
          (encodeFeature(name, unseen.toString), 1.0)
        else {
          val (f, isLegal) = multiCondMatches(splits(0))
          val w = try {splits(1).toDouble} catch { case e: Exception => -1.0}
          if (w == -1.0 || !isLegal)
            (encodeFeature(name, unseen.toString), 1.0)
          else
            (f, w)
        }
      case _ =>
        (multiCondMatches(value)._1, 1.0)
    }
  }

  private def getComplexCatFeature(values: String): Array[(String, Double)] = {
    val candis = values.trim.split("\\|")
    val ret = mutable.ArrayBuffer[(String, Double)]()

    for (value <- candis) {
      ret += getSingleCatFeature(value)
    }

    ret.toArray
  }
}

class TextualFeature(override val name: String, override val fType: FeatureType = TEXTUAL) extends Feature(name, fType) {

  // TODO
  override def getEncodedFeature(value: String): Array[String] = {
    Array[String]()
  }

  // TODO
  override def getTransformedFeature(value: String): Array[(String, Double)] = {
    Array[(String, Double)]()
  }
//  @inline private def _getTextualDefault(): String = {
//    val w = if (isWeighted) defaultWeight else 1.0
//    if(isHashing)
//      s"${new MurMur3Improved(hashingRange).hashingTrick(encodeFeature(name, unseen.toString)).toString}|$w"
//    else
//      s"${encodeFeature(name, unseen.toString)}|$w"
//  }
//
//  def _getEncodedFeature[T](value: T): String = {
//    if (!_valid(value.toString)) {
//      if (isSkip)
//        return "SKIP"
//      else
//        return encodeFeature(name, unseen.toString)
//    }
//
//    val v = value.toString.split(":")(0)
//    if (!_valid(v)) {
//      if (isSkip)
//        return "SKIP"
//      else
//        return encodeFeature(name, unseen.toString)
//    }
//
//    if (stopWords.isDefined && stopWords.get.contains(v)) {
//      if (isSkip) "SKIP" else encodeFeature(name, unseen.toString)
//    }
//    else{
//      encodeFeature(name, v.toString)
//    }
//  }
//
//  def _getWeight[T](value: T): Double = {
//    if (!_valid(value.toString))
//      return defaultWeight
//
//    val pair = value.toString.split(":")
//    if (isWeighted) {
//      if (pair.length == 2) {
//        try {
//          pair(1).toDouble
//        }
//        catch {
//          case e: Exception => defaultWeight
//        }
//      }
//      else
//        defaultWeight
//    }
//    else {
//      1.0
//    }
//  }
//
//  def getWeight[T](value: T): Double = {
//    1.0
//  }
//
//  // trick: (word1:word2:word3:...:wordn|weight1:weight2:...:weightn, 1.0)
//  def getEncodedFeature[T](value: T): String = {
//    if (!_valid(value.toString)) {
//      if (isSkip)
//        return "SKIP"
//      else
//        _getTextualDefault()
//    }
//
//    val splits = value.toString.split("\\|")
//    var head = ""
//    val distincter = mutable.HashSet[String]()
//    val ws = mutable.ArrayBuffer[Double]()
//    var maxW = Double.NegativeInfinity
//    var minW = Double.PositiveInfinity
//    var i = 0
//
//    val murmur: Option[MurMur3Improved] = if (isHashing) Some(new MurMur3Improved(hashingRange)) else None
//    while (i < splits.length) {
//      var _v = _getEncodedFeature[String](splits(i))
//
//      // textual features hashing trick: murmur3 hash
//      if (isHashing) {
//        _v = murmur.get.hashingTrick(_v).toString
//      }
//      val _w = _getWeight[String](splits(i))
//
//      if (_v != "SKIP" && !distincter.contains(_v)) {
//
//        distincter.add(_v)
//        head += s"${_v}:"
//        // scaling
//        ws += _w
//        if (_w > maxW) {
//          maxW = _w
//        }
//        else {
//          if (_w < minW) {
//            minW = _w
//          }
//        }
//      }
//
//      i += 1
//    }
//
//    if (head.isEmpty || ws.isEmpty || maxW <= 0){
//      if (isSkip)
//        "SKIP"
//      else
//        encodeFeature(name, unseen.toString) + "|" + "1.0"
//    }
//    else {
//      var tail = ""
//      for (w <- ws) {
//        tail += s"${(1.0 * w / 100).formatted("%.4f")}:"
//      }
//
//      s"${head.dropRight(1)}|${tail.dropRight(1)}"
//    }
//  }
//
//  def transformFeature[T](value: T): (String, Double) = {
//    (getEncodedFeature(value), 1.0)
//  }
}

class NumericalFeature (override val name: String, override val fType: FeatureType = NUMERICAL) extends Feature(name, fType) {
  override def getEncodedFeature(value: String): Array[String] = { Array[String](name) }

  override def getTransformedFeature(value: String): Array[(String, Double)] = {
    val v = value.trim
    val _w = try { v.toDouble } catch { case e: Exception => unseen }
    val w = if (lowerBound.isDefined && _w < lowerBound.get || upperBound.isDefined && _w > upperBound.get) unseen else _w
    Array[(String, Double)]((name, w))
  }
}

class OrdinalFeature(override val name: String, override val fType: FeatureType = ORDINAL) extends Feature(name, fType) {
  override def getEncodedFeature(value: String): Array[String] = {
    Array[String](getDiscrete(value)._1)
  }

  override def getTransformedFeature(value: String): Array[(String, Double)] = {
    Array[(String, Double)](getDiscrete(value))
  }

  private def multiCondMatches(value: String): (Double, Boolean) = {
    val _k = try { value.toDouble } catch { case e: Exception => Double.MinValue }

    if (_k == Double.MinValue
      || lowerBound.isDefined && _k < lowerBound.get
      || upperBound.isDefined && _k > upperBound.get
      || stopWords.isDefined && stopWords.get.contains(value))
      (unseen, false)
    else
      (_k, true)
  }

  private def getSingleOrdFeature(value: String): (Double, Double) = {
    isWeighted match {
      case true =>
        val splits = value.split(":")
        if (splits.length != 2)
          (unseen, 1.0)
        else {
          val (k, isLegal) = multiCondMatches(splits(0))
          val w = try { splits(1).toDouble } catch { case e: Exception => -1.0 }
          if (w == -1.0 || !isLegal)
            (unseen, 1.0)
          else
            (k, w)
        }
      case _ =>
        (multiCondMatches(value)._1, 1.0)
    }
  }

  private def getCleanedPair(value: String): (Double, Double) = {
    val v = value.trim
    if (v.isEmpty) return (unseen, 1.0)

    getSingleOrdFeature(v)
  }

  private def getDiscrete(value: String): (String, Double) = {
    val (k, w) = getCleanedPair(value)

    val discrete = if (discretize.isEmpty) DUMMY else discretize.get
    val bkt: Int = {
      discrete match {
        case DUMMY =>
          k.toInt
        case BINARIZE =>
          require(bins.getOrElse(Array[Int]()).length == 1, s"input the bins if you want to use binary discrete")
          if (k < bins.get(0)) 0 else 1
        case BUCKETIZE =>
          require(bins.getOrElse(Array[Int]()).nonEmpty, s"input the bins if you want to use bucket discrete")
          getBucket(k)
        case _ => throw new RuntimeException(s"unsupported discrete type, coming soon...")
      }
    }

    (encodeFeature(name, bkt.toString), w)
  }

  private def getBucket(k: Double): Int = {
    binarySearchForBuckets(bins.get, k)
  }

  @tailrec
  private def lowerBoundBinarySearch(buckets: Array[Double], target: Double, lower: Int, upper: Int): Int = {
    var mid: Int = 0
    if (lower <= upper) {
      mid = (lower + upper) / 2

      if (target >= buckets(mid)) {
        lowerBoundBinarySearch(buckets, target, mid + 1, upper)
      }
      else {
        lowerBoundBinarySearch(buckets, target, lower, mid - 1)
      }
    }
    else {
      upper
    }
  }

  private def binarySearchForBuckets(splits: Array[Double], feature: Double): Int = {
    if (splits.length == 1) {
      if (feature < splits(0)) 0 else 1
    }
    else {
      lowerBoundBinarySearch(splits, feature, 0, splits.length - 1)
    }
  }

}
