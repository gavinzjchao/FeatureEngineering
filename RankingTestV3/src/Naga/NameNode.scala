package Naga

import scala.collection.mutable

/**
  * @param feature feature
  */
abstract class NameNode(val feature: Feature) extends Serializable {
  val values: mutable.HashMap[String, Double] = mutable.HashMap[String, Double]()

  /** if value exists values(value) += 1, else values(value) = 1 */
  def updateValues(pair: (String, Double)): Boolean
  /** merge dst[NameNode] to current node */
  def updateNode(dst: NameNode): Boolean
  /** dropping features: counter < counterThreshold */
  def getValues: mutable.HashMap[String, Double]
  def getNumValues: Int
}

class CategoriesNameNode(override val feature: Feature) extends NameNode(feature) {
  override def updateValues(pair: (String, Double)): Boolean = {
    if (pair._1.nonEmpty && pair._1 != "SKIP") {
      if (values.contains(pair._1))
        values(pair._1) += 1
      else
        values += ((pair._1, 1))
    }

    true
  }

  override def updateNode(dst: NameNode): Boolean = {
    val dstArray = dst.getValues
    if (dstArray.nonEmpty) {
      for ((key, cnt) <- dstArray) {
        if (values.contains(key))
          values(key) += cnt
        else
          values += ((key, cnt))
      }
    }
    true
  }

  override def getNumValues: Int = values.size
  override def getValues: mutable.HashMap[String, Double] = values
}

class TextualNameNode(override val feature: Feature) extends NameNode(feature) {
  override def updateValues(pair: (String, Double)): Boolean = {
    if (pair._1.nonEmpty && pair._1 != "SKIP") {
      val p = pair._1.split("\\|")
      if (p.length == 2) {
        val words = p(0).split(":")
        for (word <- words) {
          if (values.contains(word))
            values(word) += 1
          else
            values += ((word, 1))
        }
      }
    }

    true
  }

  override def updateNode(dst: NameNode): Boolean = {
    val dstArray = dst.getValues
    if (dstArray.nonEmpty) {
      for ((key, cnt) <- dstArray) {
        if (values.contains(key))
          values(key) += cnt
        else
          values += ((key, cnt))
      }
    }
    true
  }

  override def getNumValues: Int = values.size
  override def getValues: mutable.HashMap[String, Double] = values
}

class NumericalNameNode(override val feature: Feature) extends NameNode(feature) {
  override def updateValues(pair: (String, Double)): Boolean = {

    true
  }

  override def updateNode(dst: NameNode): Boolean = {

    true
  }

  override def getNumValues: Int = 1
  override def getValues: mutable.HashMap[String, Double] = values
}

class CompactNameNode(override val feature: Feature) extends NameNode(feature) {
  override def updateValues(pair: (String, Double)): Boolean = {

    true
  }

  override def updateNode(dst: NameNode): Boolean = {

    true
  }

  // upper bound plus one, in case of overflow
  override def getNumValues: Int = feature.upperBound.get.toInt + 1
  override def getValues: mutable.HashMap[String, Double] = values
}

class OrdinalNameNode(override val feature: Feature) extends NameNode(feature) {

  override def updateValues(pair: (String, Double)): Boolean = {

    true
  }

  override def updateNode(dst: NameNode): Boolean = {

    true
  }

  override def getNumValues: Int = if (feature.bins.isDefined) feature.bins.get.length + 1 else 3
  override def getValues: mutable.HashMap[String, Double] = values
}
