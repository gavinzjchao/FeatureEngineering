package Utils

import org.apache.spark.rdd.RDD

/**
  * Created by zongqianliu on 2016/8/20.
  */
object Filter {
  /**
    * filter data with date within (endDate - shiftHour, endDate], in hour
    * @param data
    * @param date_index: using which column as date to do the filter
    * @param delimiter
    * @param endDate
    * @param shiftHour
    * @return
    */
  def filterWithDate(data: RDD[String], date_index: Int, delimiter: String,
                     endDate: String, shiftHour: Int=1 ) : RDD[String] = {
    data.filter{line =>
      val fields = line.split(delimiter)
      val startDate = AuxUtil.hourUtil.getLastNumStr(endDate, shiftHour)
      fields(date_index) > startDate && fields(date_index) <= endDate
    }
  }
}
