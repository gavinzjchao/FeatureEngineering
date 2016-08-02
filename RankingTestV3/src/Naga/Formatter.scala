package Naga

import java.io.{FileNotFoundException, IOException}

import Naga.DiscretizeType._
import Naga.FeatureType._

import scala.collection._
import scala.collection.immutable.Set
import scala.util.Try

/**
  * @author gavinzjchao@tencent.com all rights reserved
  * @param numFeatures feature numbers, support for scalable, dynamic feature enrichment
  * @param paramsSize feature's parameter size
  */

class Formatter(private val numFeatures: Int, private val paramsSize: Int = 11) extends Serializable {
  var formatter: Array[Feature] = Array[Feature]()
  /** in case of confusion, covert name to index and using ascend ordering */
  private var nameToIndexMap: Map[String, Int] = Map[String, Int]()
  var interactioner: Array[(String, Double, Boolean, Array[Int])] = Array[(String, Double, Boolean, Array[Int])]()
  var labeler: Map[String, Double] = Map[String, Double]()

  def getNumFeatures: Int = numFeatures
  def getParamsSize: Int = paramsSize

  /** recursive feature interactions, tiny & beautiful */
  private def combine(m: mutable.ArrayBuffer[Array[(String, Double)]], curFormatter: mutable.ArrayBuffer[Feature], isWeighted: Boolean, start: Int, result: mutable.ArrayBuffer[(String, Double)], holdString: String, holdWeight: Double): Unit = {
    if (start == m.length) {
      result += ((holdString.drop(1), holdWeight))
    }
    else {
      var i = 0
      while (i < m(start).length) {
        val value = m(start)(i)
        val curFeature = {
          if (curFormatter(start).fType == CATEGORIES || curFormatter(start).fType == TEXTUAL)
            value._1
          else
            curFormatter(start).getEncodedFeature(value._1)
        }
        val curWeight = if (isWeighted) value._2 else 1.0
        combine(m, curFormatter, isWeighted, start + 1, result, s"${holdString}_$curFeature", holdWeight * curWeight)
        i += 1
      }
    }
  }

  @inline private def checkContianTextFeature(indices: Array[Int]): Boolean = {
    for (i <- indices) {
      if (formatter(i).fType == TEXTUAL)
        return true
    }

    false
  }

  private def decodePairs(v: String, w: Double, f: Feature): Array[(String, Double)] = {
    if (f.fType == TEXTUAL) {
      val res = mutable.ArrayBuffer[(String, Double)]()
      val pair = v.split("\\|")
      if (pair.length != 2)
        return Array((f.getDeafualtValue().split(":")(0), if (f.isWeighted) f.defaultWeight else 1.0))
      val values = pair(0).split(":")
      val weights = pair(1).split(":")
      if (values.length != weights.length)
        return Array((f.getDeafualtValue().split(":")(0), if (f.isWeighted) f.defaultWeight else 1.0))

      var i = 0
      while (i < values.length) {
        val fea = values(i)
        val wei = try {
          weights(i).toDouble
        } catch {
          case e: Exception => if (f.isWeighted) f.defaultWeight else 1.0
        }
        res += ((fea, wei))

        i += 1
      }
      res.toArray
    }
    else {
      val _v = {
        if (f.fType == CATEGORIES)
          v
        else
          f.getEncodedFeature(v)
      }

      Array((_v, w))
    }
  }

  private def encodePairs(arr: mutable.ArrayBuffer[(String, Double)]): (String, Double) = {
    var head = ""
    var tail = ""
    for ((v, w) <- arr) {
      head += s"${v}:"
      tail += s"${w.formatted("%.4f")}:"
    }

    (s"${head.dropRight(1)}|${tail.dropRight(1)}", 1.0)
  }

  def setInteractions(uniVariates: Array[(String, Double)]): Array[(String, Double)] = {
    val ret = mutable.ArrayBuffer[(String, Double)]()

    for (curInter <- interactioner) {
//      val (name, threshold, isWeighted, indices): (String, Double, Boolean, Array[Int]) = curInter
      val name = curInter._1
      val threshold = curInter._2
      val isWeighted = curInter._3
      val indices = curInter._4


      val isTextFeature = checkContianTextFeature(indices)

      if (isTextFeature) {
        val curFormatter = mutable.ArrayBuffer[Feature]()
        val curMatrix = mutable.ArrayBuffer[Array[(String, Double)]]()

        for (idx <- indices) {
          if (idx < uniVariates.length) {
            val (v, w): (String, Double) = uniVariates(idx)
            val f = formatter(idx)
            val curArr = decodePairs(v: String, w: Double, f: Feature)
            curFormatter += f
            curMatrix += curArr
          }
        }

        val tempRes = mutable.ArrayBuffer[(String, Double)]()
        combine(curMatrix, curFormatter, isWeighted, 0, tempRes, "", 1.0)
        ret += encodePairs(tempRes)
      }
      else {
        var i = 0
        var f = ""
        var w = 1.0
        var skiped = false
        while (i < indices.length) {
          val idx = indices(i)
          require(idx < uniVariates.length && idx < formatter.length, s"illegal index during interaction")
          val curf = {
            if (formatter(idx).fType == CATEGORIES)
              uniVariates(idx)._1
            else
              formatter(idx).getEncodedFeature(uniVariates(idx)._1)
          }
          if (curf == "SKIP") skiped = true
          f += s"${curf}_"
          if (isWeighted) w *= uniVariates(idx)._2
          i += 1
        }
        val tp = if (f.length <= 1 || skiped) (s"${name}^-1", w) else (f.dropRight(1), w)
        ret += tp
      }
    }
    ret.toArray
  }

  private def createFeature(params: Array[String]): Feature = {
    // 1. parse parameters
    require(params(0).trim.nonEmpty, s"Empy name found, not allowed")
    val name: String = params(0).trim
    /** FeatureType */
    val fType: FeatureType = try {
      FeatureType.withName(params(1).trim)
    } catch {
      case e: NoSuchElementException => FeatureType.CATEGORIES
    }
    val isSkip: Boolean = if (params(2).trim.toLowerCase == "true" || params(2).trim == "1") true else false
    val isWeighted: Boolean = if (params(3).trim.toLowerCase == "true" || params(3).trim == "1") true else false
    val defaultWeight: Double = try {
      params(4).trim.toDouble
    } catch {
      case e: Exception => 0.0
    }
    val unseen: Int = try {
      params(5).trim.toInt
    } catch {
      case e: Exception => -1
    }
    val isInclude: Boolean = if (params(6).trim.toLowerCase == "false" || params(6).trim == "0") false else true
    val isHashing: Boolean = if (params(7).trim.toLowerCase == "true" || params(7).trim == "1") true else false
    val hashingRange: Int = try {
      params(8).trim.toInt
    } catch {
      case e: Exception => 1024
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
    val paramsClass = new Params(isSkip, isWeighted, defaultWeight, unseen, isInclude, isHashing, hashingRange, stopWords, countingThreshold, lowerBound, upperBound, discretize, bins)

    // 2. build feature based on parsed parameters
      val feature = fType match {
        case CATEGORIES =>
          new CategoryFeature(name)
        case NUMERICAL =>
          new NumericalFeature(name)
        case ORDINAL =>
          new OrdinalFeature(name)
        case COMPACT =>
          new CompactFeature(name)
        case TEXTUAL =>
          new TextualFeature(name)
        case _ =>
          throw new IllegalArgumentException(s"not supoort feature type $fType")
      }
    feature.init(paramsClass)
    feature
  }

  def load(lines: Array[String]): Boolean = {
    val formatterBuffer = mutable.ArrayBuffer[Feature]()
    val nameToIndexMapBuffer = mutable.Map[String, Int]()
    val interactionerBuffer = mutable.ArrayBuffer[(String, Double, Boolean, Array[String])]()
    val labelerBuffer = mutable.HashMap[String, Double]()

    try {
      var idx = 0
      var atIdx = 0
      for (line <- lines) {
        if (!line.trim.startsWith("#") && line.trim.nonEmpty) {
          if (line.trim.startsWith("@interaction@")) {
            val pairs = line.trim.split("@").last.trim.split(";")
            val names = pairs(0).trim.split(":")
            val threshold = Try(pairs(1).trim.toDouble).getOrElse(0.0)
            val weighted = if (pairs(2).trim.toLowerCase == "true" || pairs(2).trim == "1") true else false
            if (names.length > 1) {
              val featureName = names.mkString("_")
              interactionerBuffer += ((featureName, threshold, weighted, names))
            }
          }
          else {
            if (line.trim.startsWith("@label@")) {
              val pairs = line.trim.split("@").last.trim.split(";")
              for (pair <- pairs) {
                val labelWithValue = pair.trim.split(":")
                require(labelWithValue.length == 2, s"specified label length is illegal: ${labelWithValue.length}")
                try{
                  val label = labelWithValue(0).trim
                  val value = labelWithValue(1).trim.toDouble
                  labelerBuffer += ((label, value))
                } catch {
                  case e: ClassCastException => throw new ClassCastException(s"label's value is not a double type: ${labelWithValue(1)}")
                }
              }
            }
            else {
              val params = line.trim.split(";")
              require(params.size == paramsSize, s"current formatter: $params not legal $line, size: ${params.size}")
              val feature: Feature = createFeature(params)
              formatterBuffer += feature
              nameToIndexMapBuffer += ((feature.name, idx))
              idx += 1
            }
          }
        }
      }
    } catch {
      case e: FileNotFoundException => println(s"file  not found")
      case e: IOException => println(s"file  got a IOException")
    }

    require(formatterBuffer.size == numFeatures, s"formatter size: ${formatterBuffer.size} is not equal to numFeatures: $numFeatures")
    formatter = formatterBuffer.toArray
    nameToIndexMap = nameToIndexMapBuffer.toMap
    interactioner = interactionerBuffer.map(pair => (pair._1, pair._2, pair._3, pair._4.map(nameToIndexMap.apply))).toArray
    labeler = labelerBuffer.toMap
    true
  }
}
