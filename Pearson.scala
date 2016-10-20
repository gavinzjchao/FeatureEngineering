package NagaFeatureAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Pearson {

  def PearsonCorrelation(sc: SparkContext, featuresAndLabel: RDD[(Array[Double], Double)],
                         featureLen: Int): Array[(Int, Double)] = {
    val acc = new Array[Double](featureLen + 2)
    initArray(acc, featureLen + 2)

    // 1. compute mean
    val cntArray = featuresAndLabel.treeAggregate(acc)(
      seqOp = (c, v) => {
        var i = 0
        while (i < v._1.length) {
          c(i) += v._1(i)
          i += 1
        }
        c(i) += v._2
        c(i + 1) += 1
        c
      },
      combOp = (c1, c2) => {
        addArray(c1, c2)
      })

    val meanArray = cntArray.map(_ / cntArray.last).dropRight(1)

    // 2. transform: x_i - mean
    val transformedFeaturesAndLabel = featuresAndLabel.map { case ((features, label)) =>
      var i = 0
      while (i < features.length) {
        features(i) -= meanArray(i)
        i += 1
      }
      (features, label - meanArray.last)
    }

    // 3. transform (x_i - mean) => ((x_i - x_mean) * (y_i - y_mean), (x_i - mean)^2), y ignore first factor
    val expressionArray = transformedFeaturesAndLabel.map{ case ((tFeatures, tLabel)) =>
      val expressionPair = mutable.ArrayBuffer[(Double, Double)]()
      var i = 0
      while (i < tFeatures.length) {
        expressionPair += ((tFeatures(i) * tLabel, math.pow(tFeatures(i), 2)))
        i += 1
      }
      (expressionPair.toArray, math.pow(tLabel, 2))
    }

    // 4. compute: pearson return pearson array
    val pRawScore = new Array[(Double, Double)](featureLen + 1)
    val _pRawScore = initRawScore(pRawScore, featureLen+1)
    val pearsonRawScore = expressionArray.treeAggregate(_pRawScore)(
      seqOp = (c, v) => {
        updateExpression(c, v)
      },
      combOp = (c1, c2) => {
        combineExpression(c1, c2)
      })

    val pearsonScores = mutable.ArrayBuffer[(Int, Double)]()
    val yMinusYSquareSqrt = math.sqrt(pearsonRawScore.last._2)
    var i = 0
    while (i < pearsonRawScore.length - 1) {
      val xMinusXSquareSqrt = math.sqrt(pearsonRawScore(i)._2)
      val xMutiY = pearsonRawScore(i)._1
      val curPearsonScore = xMutiY / (xMinusXSquareSqrt * yMinusYSquareSqrt)
      pearsonScores += ((i, curPearsonScore))

      i += 1
    }

    pearsonScores.toArray
  }

  @inline private def initArray(acc: Array[Double], len: Int): Array[Double] = {
    var i = 0
    while (i < len) {
      acc(i) = 0
      i += 1
    }
    acc
  }

  @inline private def initRawScore(rawArr: Array[(Double, Double)], len: Int): Array[(Double, Double)] = {
    var i = 0
    while (i < len) {
      rawArr(i) = (0,0)
      i += 1
    }
    rawArr
  }

  @inline private def addArray(c1: Array[Double], c2: Array[Double]): Array[Double] = {
    var i = 0
    while (i < c1.length) {
      c1(i) += c2(i)
      i += 1
    }
    c1
  }

  private def updateExpression(c: Array[(Double, Double)], v: (Array[(Double, Double)], Double)): Array[(Double, Double)] = {
    val res = mutable.ArrayBuffer[(Double, Double)]()

    var i = 0
    val features = v._1
    val label = v._2
    while (i < features.length) {
      res += ((c(i)._1 + features(i)._1, c(i)._2 + features(i)._2))
      i += 1
    }
    res += ((0.0, label + c(i)._2))

    res.toArray
  }

  private def combineExpression(c1: Array[(Double, Double)], c2: Array[(Double, Double)]): Array[(Double, Double)] = {
    val res = mutable.ArrayBuffer[(Double, Double)]()
    var i = 0
    while (i < c1.length) {
      res += ((c1(i)._1 + c2(i)._1, c1(i)._2 + c2(i)._2))
      i += 1
    }

    res.toArray
  }
}
