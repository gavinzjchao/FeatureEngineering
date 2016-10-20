package NagaFeatureAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * @author gavinzjchao, zongqianliu, zhijunfan
  */

object FeatureSelection {
  /**
    * Compute Mutual Information between given feature dimensions and label
    * @param sc
    * @param data RDD[String]: each line is one sample with label and features
    * @param indices Array[Int]: specify features to compute mutual information with label
    * @param delimiter String: values of ",", "\t" which can split label and features in data
    * @param label_index Int: specify which column is label
    * @return
    */
  def computeMutualInfo(sc: SparkContext, data: RDD[String],
                        label_index: Int = 0,
                        delimiter: String = "\t",
                        indices: Array[Int] = Array[Int]()): Array[(Int, Double)] = {
      MutualInformation.computeMutualInfo(sc, data, label_index, delimiter, indices)
  }

  def computeConditionalMutualInfo(sc: SparkContext, data: RDD[String],
                                   label_index: Int = 0,
                                   delimiter: String = "\t",
                                   indices_pair: Array[(Int, Int)] = Array[(Int, Int)]()):
                                    Array[((Int,Int), Double)] = {
      ConditionalMutualInformation.computeConditionalMutualInfo(sc, data, label_index, delimiter, indices_pair)
  }

  def ChiSquareMulti(sc: SparkContext, data: RDD[String],
                     delimiter: String = ",", indices: Array[Int] = Array[Int]()):
                Array[(Int, Int, Double)] = {
      ChiSquare.ChiSquareMulti(sc, data, delimiter, indices)
  }

  def ChiSquareOHE(data: RDD[String], label_index: Int = 0,
                   delimiter: String = ",", indices: Array[Int]): RDD[(Int, Double)] = {
      ChiSquare.ChiSquareOHE(data, label_index, delimiter, indices)
  }
  def PearsonCorrelation(sc: SparkContext, featuresAndLabel: RDD[(Array[Double], Double)],
                         featureLen: Int): Array[(Int, Double)] = {
      Pearson.PearsonCorrelation(sc, featuresAndLabel, featureLen)
  }

}
