package NagaFramework

import java.lang.Integer.{rotateLeft => rotl}

import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable

/**
  * Created by gavinzjchao on 2016/6/28.
  */

class MurMur3Improved(numFeatures: Int = 1024, seed: Int = 42) extends Serializable {
  final def mix(hash: Int, data: Int): Int = {
    var h = mixLast(hash, data)
    h = rotl(h, 13)
    h * 5 + 0xe6546b64
  }

  /** May optionally be used as the last mixing step. Is a little bit faster than mix,
    *  as it does no further mixing of the resulting hash. For the last element this is not
    *  necessary as the hash is thoroughly mixed during finalization anyway. */
  final def mixLast(hash: Int, data: Int): Int = {
    var k = data

    k *= 0xcc9e2d51
    k = rotl(k, 15)
    k *= 0x1b873593

    hash ^ k
  }

  /** Finalize a hash to incorporate the length and make sure all bits avalanche. */
  final def finalizeHash(hash: Int, length: Int): Int = avalanche(hash ^ length)

  /** Force all bits of the hash to avalanche. Used for finalizing the hash. */
  private final def avalanche(hash: Int): Int = {
    var h = hash

    h ^= h >>> 16
    h *= 0x85ebca6b
    h ^= h >>> 13
    h *= 0xc2b2ae35
    h ^= h >>> 16

    h
  }

  /** Compute the hash of a string */
  final def stringHash(str: String, seed: Int): Int = {
    var h = seed
    var i = 0
    while (i + 1 < str.length) {
      val data: Int = (str.charAt(i) << 16) + str.charAt(i + 1)
      h = mix(h, data)
      i += 2
    }
    if (i < str.length) h = mixLast(h, str.charAt(i))
    finalizeHash(h, str.length)
  }

  final def stringHash4Bytes(str: String, seed: Int): Int = {
    var h = seed
    var i = 0
    while (i + 3 < str.length) {
      val data: Int = (str.charAt(i) << 24) + (str.charAt(i + 1) << 16) + (str.charAt(i + 2) << 8) + str.charAt(i + 3)
      h = mix(h, data)
      i += 4
    }

    if (i < str.length) {
      val data = {
        str.length - i match {
          case 3 => (str.charAt(i) << 16) + (str.charAt(i + 1) << 8) + str.charAt(i + 2)
          case 2 => (str.charAt(i) << 8) + str.charAt(i + 1)
          case 1 => str.charAt(i)
        }
      }
      h = mixLast(h, data)
    }

    finalizeHash(h, str.length)
  }

  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def hashingTrick(str: String): Int = {
    val murmur = stringHash(str, seed)
    nonNegativeMod(murmur, numFeatures)
  }

  def hashingTrick(document: Iterable[String]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    document.foreach { term =>
      val i = hashingTrick(term)
      termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }
}
