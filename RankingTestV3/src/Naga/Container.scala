package Naga

import Naga.DiscretizeType._
import Naga.FeatureType._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import utilities.AuxUtil

import scala.collection.mutable

/**
  * @author gavinzjchao@tencent.com all rights reserved
  * @param formatter factory class for dealing with different feature types
  */
class Container(val formatter: Formatter) extends Serializable {

  private val namePool: Array[NameNode] = Array[NameNode]()
  private val featurePool: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()

  private def flattenSample(cur:(Double, Array[(String, Double)]), npwo: NamePoolWithOffset): LabeledPoint = {
    require(npwo.featureSize > 0, s"namepool with offset length is 0")
    val featureSize = npwo.featureSize
    val label = cur._1
    val rawFeatures = cur._2
    val bcNamePool = npwo.namepool
    val offsets = npwo.offsets

    val indices = mutable.ArrayBuffer[Int]()
    val values = mutable.ArrayBuffer[Double]()

    var i = 0
    while (i < rawFeatures.length) {
      val curPair = rawFeatures(i)
      val curNode = bcNamePool(i)
      val fType = curNode.feature.fType

      if (fType == TEXTUAL) {
        val tp = curPair._1.split("\\|")
        if (tp.length == 2) {
          val tvs = tp(0).split(":")
          val tws = tp(1).split(":")

          if (tvs.length == tws.length) {
            var j = 0
            while (j < tvs.length) {
              val __v = if (curNode.feature.isHashing) {
                var _idx = try {
                  tvs(j).toInt
                } catch {
                  case e: Exception => curNode.feature.getDeafualtValue().toInt
                }

                if (_idx < 0 || _idx >= curNode.feature.hashingRange)
                  _idx = curNode.feature.getDeafualtValue().toInt

                _idx
              }
              else {
                val _idx2 = try {
                  if (curNode.values.contains(tvs(j))) {
                    curNode.values(tvs(j)).toInt
                  }
                  else {
                    curNode.values(curNode.feature.getDeafualtValue()).toInt
                  }

                } catch {
                  case e: Exception => curNode.values(curNode.feature.getDeafualtValue()).toInt
                }

                _idx2
              }

              val _v = __v + offsets(i)
              val _w = try {
                tws(j).toDouble
              } catch {
                case e: Exception => curNode.feature.getWeight()
              }
              indices += _v
              values += _w

              j += 1
            }
          }
          else {
            indices += {
              if (curNode.feature.isHashing)
                curNode.feature.getDeafualtValue().toInt
              else
                curNode.values(curNode.feature.getDeafualtValue()).toInt
            }
            values += curNode.feature.defaultWeight
          }
        }
      }
      else {
        val (v: Int, w: Double) = try {
          fType match {
            case NUMERICAL =>
              val _v = offsets(i) + 1
              val _w = curPair._1.toDouble
              (_v, _w)
            case COMPACT =>
              val __v = try {
                curPair._1.toInt
              } catch {
                case e: Exception => curNode.feature.getDeafualtValue().toInt
              }
              val _v = offsets(i) + __v
              val _w = curPair._2
              (_v, _w)
            case ORDINAL =>
              val __v = try {
                if (curNode.values.contains(curPair._1)) {
                  curNode.values(curPair._1).toInt
                }
                else {
                  curNode.values(curNode.feature.getDeafualtValue()).toInt
                }
              }
              val _v = __v
              val _w = curPair._2
              (_v, _w)
            case CATEGORIES =>
              val __v = try {
                if (curNode.values.contains(curPair._1)) {
                  curNode.values(curPair._1).toInt
                }
                else {
                  curNode.values(curNode.feature.getDeafualtValue()).toInt
                }
              }
              val _v = __v
              val _w = curPair._2
              (_v, _w)
            case _ => throw new Exception(s"unsupport feature type")
          }
        } catch {
          case e: Exception => (-1, 0.0)
        }

        if (v != -1) {
          indices += v
          values += w
        }
      }

      i += 1
    }

    LabeledPoint(label, Vectors.sparse(featureSize, indices.toArray, values.toArray))
  }

  def fit(input: RDD[(Double, Array[(String, Double)])], npwo: NamePoolWithOffset): RDD[LabeledPoint] = {
    input.mapPartitions( iter => {
      val holder = mutable.ArrayBuffer[LabeledPoint]()
      while (iter.hasNext) {
        val cur = iter.next
        holder += flattenSample(cur, npwo)
      }
      holder.iterator
    }, preservesPartitioning = true)

  }



  private def hashingSamples(cur:(Int, Double, Array[(String, Double)]), bcFormatter: Formatter, hashingRange: Int, murmur: MurMur3Improved): (Int, LabeledPoint) = {
    val isTrain = cur._1
    val label = cur._2
    val rawFeatures = cur._3
    val bcNamePool = initNamePool(bcFormatter)

    val kvs = mutable.HashMap[Int, Double]()

    var i = 0
    while (i < rawFeatures.length) {
      val curPair = rawFeatures(i)
      val curNode = bcNamePool(i)
      val fType = curNode.feature.fType

      if (fType == TEXTUAL) {
        val tp = curPair._1.split("\\|")
        if (tp.length == 2) {
          val tvs = tp(0).split(":")
          val tws = tp(1).split(":")

          if (tvs.length == tws.length) {
            var j = 0
            while (j < tvs.length) {
              val _v = murmur.hashingTrick(tvs(j))
              val _w = try {
                tws(j).toDouble
              } catch {
                case e: Exception => curNode.feature.getWeight()
              }
              kvs += ((_v, _w))

              j += 1
            }
          }
          else {
            kvs += ((murmur.hashingTrick(curNode.feature.getDeafualtValue()), curNode.feature.defaultWeight))
          }
        }
      }
      else {
        val v = {
          if (fType == ORDINAL || fType == CATEGORIES)
            murmur.hashingTrick(curPair._1)
          else
            murmur.hashingTrick(curNode.feature.getEncodedFeature(curPair._1))
        }
        val w = curPair._2

        kvs += ((v, w))
      }
      i += 1
    }

    (isTrain, LabeledPoint(label, Vectors.sparse(hashingRange, kvs.toArray.sortBy(_._1))))
  }

  def fit(input: RDD[(Int, Double, Array[(String, Double)])], bcFormatter: Formatter, hashingRange: Int, seed: Int): RDD[(Int, LabeledPoint)] = {
    input.mapPartitions( iter => {
      val holder = mutable.ArrayBuffer[(Int, LabeledPoint)]()
      val murmur = new MurMur3Improved(hashingRange, seed)
      while (iter.hasNext) {
        val cur = iter.next
        holder += hashingSamples(cur, bcFormatter, hashingRange, murmur)
      }
      holder.iterator
    }, preservesPartitioning = true)
  }

  def decodeMultiFeatures(_k: String): Array[(String, Double)] = {
    val splits = _k.trim.split("\\|")
    val res = mutable.ArrayBuffer[(String, Double)]()

    var i = 0
    while (i < splits.length) {
      val pair = splits(i).split(":")
      if (pair.length == 2) {
        val v = try {
          pair(1).toDouble
        } catch {
          case e: Exception => 1.0
        }

        res += ((pair(0), v))
      }
      else {
        res += ((splits(i), 1.0))
      }

      i += 1
    }
    res.toArray
  }

  def transform(input: RDD[(String, String, Array[String])], formatter: Formatter): RDD[(Double, Array[String])] = {
    var isSkip: Boolean = false
    val isTrain = 1

    // 1. transform and filtering uni-variable
    val uniVariables = input.map{ case (dt, orignalLabel, features) =>
      val featureVec = mutable.ArrayBuffer[(String, Double)]()

      if (orignalLabel.trim.isEmpty || !formatter.labeler.contains(orignalLabel.trim) || features.length != formatter.formatter.length) {
        (1, -1.0, featureVec.toArray, true)
      }
      else {
        var i = 0
        while (i < features.length) {
          val curPair = formatter.formatter(i).transformFeature(features(i))

          if (curPair._1 == "SKIP") isSkip = true
          featureVec += curPair
          i += 1
        }
        (isTrain, formatter.labeler(orignalLabel.trim), featureVec.toArray, isSkip)
      }
    }.filter(!_._4).map(sample => (sample._1, sample._2, sample._3))

    // 2. transform cross-variable and return final raw feature matrix
    val finalVariable = uniVariables.map { case (isTrain: Int, label: Double, vec: Array[(String, Double)]) =>
      (isTrain, label, vec ++ formatter.setInteractions(vec))
    }

    val flattenedRDD = finalVariable.map{ case (dt, label, features) =>
      val flattenedVariables = mutable.ArrayBuffer[String]()
        for ((_k, _v) <- features) {
          if (_k.indexOf("\\|") >= 0) {
            flattenedVariables ++= decodeMultiFeatures(_k).map(_._1)
          } else {
            flattenedVariables += _k
          }
        }

      (label, flattenedVariables.toArray)
    }

    flattenedRDD
  }

  def FeatureIndexing(dict: Array[(String, Int)], formatter: Formatter): mutable.HashMap[String, Int] = {
    val res = mutable.HashMap[String, Int]()
    var idx = 0

    /** put samples feature dict to res */
    for ((k, v) <- dict) {
      res += ((k, idx))
      idx += 1
    }

    /** add unseen to feature dict */
    for (cur <- formatter.formatter) {
      val v = cur.getDeafualtValue()
      if (!res.contains(v)) {
        res += ((v, idx))
        idx += 1
      }
    }

    for (cur <- formatter.interactioner) {
      val indices = cur._4
      var v = ""
      for (i <- indices) {
        v += s"${formatter.formatter(i).getDeafualtValue()}_"
      }
      v = v.dropRight(1)

      if (!res.contains(v)) {
        res += ((v, idx))
        idx += 1
      }
    }

    res
  }

  def genericFeatureDict(input: RDD[(Double, Array[String])], featureSelectThres: Int, formatter: Formatter): mutable.HashMap[String, Int] = {
    val featureDict = {
      val _featureDict = input.map(x => x._2)
        .flatMap(x => x.map(y => (y, 1)))
        .reduceByKey(_ + _)
        .filter(_._2 >= featureSelectThres)
        .collect()

      FeatureIndexing(_featureDict, formatter)
    }

    featureDict
  }

  def transform(input: RDD[(String, String, Array[String])], formatter: Formatter, testDate: String): RDD[(Int, Double, Array[(String, Double)])] = {
    var isSkip: Boolean = false
    var isTrain: Int = 1

    // 1. transform and filtering uni-variable
    val uniVariables = input.map{ case (date, orignalLabel, features) =>
      val featureVec = mutable.ArrayBuffer[(String, Double)]()

      isTrain = AuxUtil.extractTrainingSamples(date, testDate)
      if (orignalLabel.trim.isEmpty || !formatter.labeler.contains(orignalLabel.trim) || features.length != formatter.formatter.length) {
        (1, -1.0, featureVec.toArray, true)
      }
      else {
        var i = 0
        while (i < features.length) {
          val curPair = formatter.formatter(i).transformFeature(features(i))

          if (curPair._1 == "SKIP") isSkip = true
          featureVec += curPair
          i += 1
        }
        (isTrain, formatter.labeler(orignalLabel.trim), featureVec.toArray, isSkip)
      }
    }.filter(!_._4).map(sample => (sample._1, sample._2, sample._3))

    // 2. transform cross-variable and return final raw feature matrix
    val finalVariable = uniVariables.map { case (isTrain: Int, label: Double, vec: Array[(String, Double)]) =>
      (isTrain, label, vec ++ formatter.setInteractions(vec))
    }

    finalVariable
  }

  private def createInteractionFeature(name: String, thr: Int = 0, iw: Boolean = false, isTextual: Boolean): Feature = {
    val isSkip: Boolean = false
    val isWeighted: Boolean = false
    val defaultWeight: Double = 0.0
    val unseen: Int = -1
    val isInclude: Boolean = true
    val isHashing: Boolean = false
    val hashingRange: Int = 1024
    val stopWords: Option[Set[String]] = None
    val countingThreshold: Int = 0
    val lowerBound: Option[Double] = None
    val upperBound: Option[Double] = None
    val discretize: Option[DiscretizeType] = None
    val bins: Option[Array[Double]] = None

    val paramsClass = new Params(isSkip, isWeighted, defaultWeight, unseen, isInclude, isHashing, hashingRange, stopWords, countingThreshold, lowerBound, upperBound, discretize, bins)

    val feature = if (isTextual) new TextualFeature(name) else new CategoryFeature(name)
    feature.init(paramsClass)
    feature.isWeighted = iw
    feature.countingThreshold = thr
    feature
  }

  @inline private def checkContianTextFeature(indices: Array[Int], formatter: Array[Feature]): Boolean = {
    for (i <- indices) {
      if (formatter(i).fType == TEXTUAL)
        return true
    }

    false
  }

  private def initNamePool(formatter: Formatter): Array[NameNode] = {
    val _namePool = mutable.ArrayBuffer[NameNode]()

    // 1. loading uni-variable from formatter
    for (format <- formatter.formatter) {
      val name = format.name
      val fType = format.fType
      val cur = fType match{
        case NUMERICAL => new NumericalNameNode(format)
        case ORDINAL => new OrdinalNameNode(format)
        case TEXTUAL => new TextualNameNode(format)
        case COMPACT => new CompactNameNode(format)
        case _ => new CategoriesNameNode(format)
      }
      cur.values.clear()
      _namePool += cur
    }

    // 2. loading cross-variable from interactioner
    for (interact <- formatter.interactioner) {
      val isTextualFeature = checkContianTextFeature(interact._4, formatter.formatter)
      val name = interact._1
      val thre = interact._2.toInt
      val iw = interact._3
      val format = createInteractionFeature(name, thre, iw, isTextualFeature)

      val cur = format.fType match {
        case TEXTUAL => new TextualNameNode(format)
        case _ => new CategoriesNameNode(format)
      }
      cur.values.clear()
      _namePool += cur
    }

    _namePool.toArray
  }

  private def updateNamePool(c: Array[NameNode], v: Array[(String, Double)]): Boolean = {
    var i = 0
    while (i < v.length && i < c.length) {
      val curNode = c(i)
      val curPair = v(i)

      val ft = curNode.feature.fType
      if (ft == CATEGORIES || (ft == TEXTUAL && !curNode.feature.isHashing)) {
        curNode.updateValues(curPair)
      }

      i += 1
    }

    true
  }

  private def combineNamePool(c1: Array[NameNode], c2: Array[NameNode]): Boolean = {
    var i = 0
    while (i < c1.length && i < c2.length) {
      val ft = c1(i).feature.fType
      if (ft == CATEGORIES || (ft == TEXTUAL && !c1(i).feature.isHashing)) {
        val dst = c2(i)
        c1(i).updateNode(dst)
      }

      i += 1
    }

    true
  }

  def setNamePool(input: RDD[Array[(String, Double)]], formatter: Formatter): Array[NameNode] = {
    val _namePool = initNamePool(formatter)

    val res = input.treeAggregate(_namePool)(
      seqOp = (c, v) => {
        updateNamePool(c, v)
        c
      },
      combOp = (c1, c2) => {
        combineNamePool(c1, c2)
        c1
      })

    res
  }

  // GAVIN tell you: only single array can store all the stuff, otherwise go to hell
  def getIdxToFeaturePool: Array[String] = {
    require(featurePool.size > 0, s"feature pool is not initialized")
    val res = new Array[String](featurePool.size)
    for ((value, idx) <- featurePool) {
      res(idx) = value
    }
    res
  }

  def getNamePool: Array[NameNode] = this.namePool
  def getFeaturePool: mutable.HashMap[String, Int] = this.featurePool
}

class NamePoolWithOffset(val namepool: Array[NameNode]) extends Serializable {
  // here I store the total length in offsets.last, so I need: namepool.length + 1
  val offsets = new Array[Int](namepool.length + 1)
  var featureSize: Int = 0

  def fit(): Unit = {
    var i = 0
    offsets(0) = 0
    while (i < namepool.length) {
      val node = namepool(i)
      if (node.feature.isInclude) {
        // 1. update values (key, counter) -> (key, index) and remove the keys less than countingThreshold
        val curThreshold = node.feature.countingThreshold
        val curValues = node.values
        if (curValues.nonEmpty) {
          val sortedValues = curValues.toSeq.filter(x => x._2 >= curThreshold).sortBy(-_._2).map(_._1).view.zipWithIndex.map(pair => (pair._1, pair._2))

          for (pair <- sortedValues) {
            if (curValues.contains(pair._1))
              curValues(pair._1) = pair._2
          }
        }

        // 2. update offsets
        val ft = node.feature.fType
        val stepsize: Int = ft match {
          case NUMERICAL => 1
          case COMPACT => node.feature.upperBound.get.toInt + 1
          case ORDINAL => if (node.feature.bins.isDefined) node.feature.bins.get.length else 3
          case TEXTUAL => if (node.feature.isHashing) node.feature.hashingRange else if (node.values.size <= 0) 3 else node.values.size
          case _ => if (node.values.size <= 0) 3 else node.values.size
        }
        offsets(i + 1) = offsets(i) + stepsize
      }
      else {
        offsets(i + 1) = offsets(i)
      }

      i += 1
    }

//    //TODO
//    var jj = 0
//    var res = s"offsets length: ${offsets.size}, feature numbers: ${offsets.last + namepool.last.getValues.size}, "
//    while (jj < offsets.length) {
//      res += s"(index -> ${jj}, offset -> ${offsets(jj)}), "
//      jj += 1
//    }
//    println(res)

    featureSize = offsets.last
  }
}