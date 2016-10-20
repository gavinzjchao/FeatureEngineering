package NagaFeatureAnalysis

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by zongqianliu on 2016/8/17.
  */
object Util {

  def write2File(data: Array[String], file_name: String): Unit = {
    val file = new File(file_name)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- data) {
      bw.write(line + "\n")
    }
    bw.close()
  }

  /**
    * cross features of left_indices and right_indices
    * Attention: the label column is put in the first column
    * @param data
    * @param left_indices
    * @param right_indices
    * @param label_index
    * @param delimiter
    * @return
    */
  def create_cross_data(data: RDD[String],
                              left_indices: Array[Int],
                              right_indices: Array[Int],
                              label_index: Int = 0,
                              delimiter: String = "\t"): RDD[String] = {
    val combine_data = data.map { line =>
      val raw_feature = line.trim.split(delimiter)
      val label = raw_feature(label_index)
      val new_feature_buffer = mutable.ArrayBuffer[String]()
      new_feature_buffer += label
      for (left_idx <- left_indices) {
        for (right_idx <- right_indices) {
          var feature_buffer = mutable.ArrayBuffer[String]()
          for (left <- raw_feature(left_idx).split("\\|")) {
            for (right <- raw_feature(right_idx).split("\\|")) {
              val left_val = left.split(":")(0)
              val right_val = right.split(":")(0)
              feature_buffer += s"${left_val}_$right_val"
            }
          }
          val feature = feature_buffer.mkString("|")
          new_feature_buffer += feature
        }
      }
      new_feature_buffer.mkString(delimiter)
    }
    combine_data
  }

  /**
    * Create cross feature corresponding names
    * @param left_indices
    * @param right_indices
    * @param names
    * @return
    */
  def create_cross_name(left_indices: Array[Int],
                        right_indices: Array[Int],
                        names: Array[String]): Array[String] = {
    val name_buffer = mutable.ArrayBuffer[String]()
    name_buffer += "label"
    if(right_indices.isEmpty) {
      for(idx <- left_indices) {
        name_buffer += s"${names(idx)}"
      }
    }
    else {
      for(left_idx <- left_indices) {
        for(right_idx <- right_indices) {
          name_buffer += s"${names(left_idx)}_${names{right_idx}}"
        }
      }
    }
    name_buffer.toArray
  }


  def create_part_combination_data(data: RDD[String],
                                   remain_indices: Array[Int],
                                   left_indices: Array[Int],
                                   right_indices: Array[Int],
                                   label_index: Int = 0,
                                   delimiter: String = "\t"): RDD[String] = {
    data.map { line =>
      val raw_feature = line.trim.split(delimiter)
      val new_feature_buffer = mutable.ArrayBuffer[String]()
      for (remain_idx <- remain_indices) {
        new_feature_buffer += raw_feature(remain_idx)
      }
      for (left_idx <- left_indices) {
        for (right_idx <- right_indices) {
          var feature_buffer = mutable.ArrayBuffer[String]()
          for (left <- raw_feature(left_idx).split("\\|")) {
            for (right <- raw_feature(right_idx).split("\\|")) {
              val left_val = left.split(":")(0)
              val right_val = right.split(":")(0)
              feature_buffer += s"${left_val}_$right_val"
            }
          }
          val feature = feature_buffer.mkString("|")
          new_feature_buffer += feature
        }
      }
      new_feature_buffer.mkString(delimiter)
    }
  }
}
