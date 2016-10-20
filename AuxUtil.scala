package Utils

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Utils.DateOfType.DateOfType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

/**
  * Created by gavinzjchao on 2016/6/3.
  */

object AuxUtil {
  val dateUtil = new DateUtil()
  val hourUtil = new DateUtil("yyyyMMddHH", DateOfType.HOUR)

  def concatPaths(headPath: String, specDate: String, slideWindow: Int, dateType: DateOfType): String = {
    var path = ""

    val intervals = {
      dateType match {
        case DateOfType.HOUR =>
          AuxUtil.hourUtil.getLastNumList(specDate, slideWindow)
        case DateOfType.DAY =>
          AuxUtil.dateUtil.getLastNumList(specDate, slideWindow)
        case _ =>
          throw new Exception(s"unspport Date of Type: $dateType")
      }
    }

    for (interval <- intervals) {
      path += s"$headPath/$interval,"
    }

    path.dropRight(1)
  }

  def getLatestPaths(headPath: String, specDate: String, slideWindow: Int, dateType: DateOfType, isUsingDate: Boolean, cluster: String): String = {
    val partitions = getLatestPartitions(headPath, slideWindow, dateType, cluster, specDate, isSpecificDate = isUsingDate)

    var path = ""
    for (interval <- partitions) {
      path += s"$headPath/$interval,"
    }

    path.dropRight(1)
  }

  def getInterestPartition(path: String, donePath: String, cluster: String): String = {
    val fs = FileSystem.get(new URI(cluster), new Configuration())

    val pathsArr = fs.listStatus(new Path(donePath))
    val latestPartition = pathsArr.last.getPath.toString.trim.split("\\/").last.substring(0, 8)

    s"$path/$latestPartition"
  }

  def getLatestPartitions(
     path: String,
     interval: Int,
     dateOfType: DateOfType,
     cluster: String = "hdfs://tl-ad-nn-tdw.tencent-distribute.com:54310/",
     specificDate: String = "",
     isSpecificDate: Boolean = false): Array[String] = {

    val fs = FileSystem.get(new URI(cluster), new Configuration())
    val partitionLength = {
      dateOfType match {
        case DateOfType.HOUR => 10
        case DateOfType.DAY  => 8
        case _ => -1
      }
    }

    // 1. if isSpecificDate then generate date
    val _specificDate = {
      if (isSpecificDate && specificDate.length > 0) {
        dateOfType match {
          case DateOfType.DAY =>
            specificDate.substring(0, 8)
          case DateOfType.HOUR =>
            specificDate
          case _ =>
            ""
        }
      }
      else {
        ""
      }
    }

    // 2. get latest or specific date's latest partitions and check if it has check file
    val ret = mutable.ArrayBuffer[String]()
    def isExists(fs: FileSystem, curPath: String, curPartition: String): Boolean = {
      fs.exists(new Path(s"$curPath/$curPartition.check"))
    }

    val pathsArr = fs.listStatus(new Path(path))
    var cnt = interval
    var i = pathsArr.length - 1
    while (i >= 0 && cnt > 0) {
      val curPath = pathsArr(i).getPath
      val curPartition = try {
        curPath.toString.trim.split("\\/").last
      }
      catch {
        case e: Exception => ""
      }

      if (partitionLength == curPartition.length && isExists(fs, curPath.toString, curPartition)) {
        if (_specificDate.length == partitionLength) {
          if (curPartition <= _specificDate) {
            cnt -= 1
            ret += curPartition
          }
        }
        else {
          cnt -= 1
          ret += curPartition
        }
      }

      i -= 1
    }

    assert(ret.length == interval, s"get partition length: ${ret.length} not equal to interval: $interval")

    ret.toArray
  }

  def getMultiPath(sc: SparkContext, numPart: Int, headPath: String, specDate: String, slideWindow: Int, dateType: DateOfType): RDD[String] = {
    val intervals = {
      dateType match {
        case DateOfType.HOUR =>
          AuxUtil.hourUtil.getLastNumList(specDate, slideWindow)
        case DateOfType.DAY =>
          AuxUtil.dateUtil.getLastNumList(specDate, slideWindow)
        case _ =>
          throw new Exception(s"unspport Date of Type: $dateType")
      }
    }
    _getMultiPath(sc, numPart, headPath, intervals)
  }

  def _getMultiPath(sc: SparkContext, numPart: Int, headPath: String, intervals: Array[String]): RDD[String] = {
    if (headPath.trim.isEmpty || intervals.isEmpty) throw new Exception(s"path or interval not legal")
    var ret = sc.textFile(s"${headPath}/${intervals(0)}", numPart)

    var i = 1
    while (i < intervals.length) {
      val curPath = s"${headPath}/${intervals(i)}"
      val cur = sc.textFile(curPath, numPart)
      ret = sc.union(ret, cur)

      i += 1
    }

    println(s"click total numbers: ${ret.count()}")
    ret
  }

  def lastNDay(s: String, num: Int): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    val cur = df.parse(s)
    cal.setTime(cur)
    cal.getTime

    cal.add(Calendar.DAY_OF_MONTH, -1 * num)
    df.format(cal.getTime)
  }

}

object DateOfType extends Enumeration {
  type DateOfType = Value
  val MINUTE, HOUR, DAY, MONTH, YEAR = Value
}

class DateUtil(var form: String = "yyyyMMdd", var dateType: DateOfType = DateOfType.DAY) {
  val dateFormatter = new SimpleDateFormat(form)

  def getCurrentDateString: String = {
    val cal = Calendar.getInstance()
    dateFormatter.format(cal.getTime)
  }

  def getCurrentCal: Calendar = {
    Calendar.getInstance()
  }

  def getCurrentDate: Date = {
    val cal = Calendar.getInstance()
    cal.getTime
  }

  def getDate(ss: String, format: String = "yyyyMMddHH"): Date = {
    if (ss.isEmpty)
      return getCurrentDate

    try {
      val df = new SimpleDateFormat(format)
      val cal = Calendar.getInstance()
      val cur = df.parse(ss)
      cal.setTime(cur)
      cal.getTime
    } catch {
      case e: Exception => getCurrentDate
    }
  }

  def getLastNumStr(num: Int): String = {
    val cal = Calendar.getInstance()
    val addType = {
      dateType match {
        case DateOfType.HOUR => Calendar.HOUR_OF_DAY
        case _ => Calendar.DAY_OF_MONTH
      }
    }
    cal.add(addType, -1 * num)
    dateFormatter.format(cal.getTime)
  }

  def getLastNumStr(time: String, num: Int): String = {
    val cal = Calendar.getInstance()
    val _date = getDate(time)
    cal.setTime(_date)
    val addType = {
      dateType match {
        case DateOfType.HOUR => Calendar.HOUR_OF_DAY
        case _ => Calendar.DAY_OF_MONTH
      }
    }
    cal.add(addType, -1 * num)
    dateFormatter.format(cal.getTime)
  }

  def getLastNumList(num: Int): Seq[String] = {
    val res = mutable.ArrayBuffer[String]()

    val direction = if (num >= 0) 1 else -1
    var i = 0
    while (i < math.abs(num)) {
      val _s = getLastNumStr(direction * i)
      res.append(_s)
      i += 1
    }

    res.toSeq
  }

  def getLastNumList(time: String, num: Int): Array[String] = {
    val res = mutable.ArrayBuffer[String]()

    val direction = if (num >= 0) 1 else -1
    var i = 0
    while (i < math.abs(num)) {
      val _s = getLastNumStr(time, direction * i)
      res.append(_s)
      i += 1
    }

    res.toArray
  }

  def getYearOfDate(ss: String, format: String = "yyyyMMdd"): String = {
    val _date = getDate(ss, format)
    val cal = Calendar.getInstance()
    cal.setTime(_date)
    cal.get(Calendar.YEAR).toString
  }

  def getMonthOfDate(ss: String, format: String = "yyyyMMdd"): String = {
    val _date = getDate(ss, format)
    val cal = Calendar.getInstance()
    cal.setTime(_date)
    (cal.get(Calendar.MONTH) + 1).toString
  }

  def getDayOfDate(ss: String, format: String = "yyyyMMdd"): String = {
    val _date = getDate(ss, format)
    val cal = Calendar.getInstance()
    cal.setTime(_date)
    cal.get(Calendar.DATE).toString
  }

}



object UnitTest{
  def main(args: Array[String]): Unit = {

    val s = "2016050110"
    println(AuxUtil.hourUtil.getCurrentDateString)
    println(AuxUtil.hourUtil.getLastNumStr(3))
    println(AuxUtil.hourUtil.getLastNumStr(-3))

    println(AuxUtil.hourUtil.getLastNumStr(s, 4))
    println(AuxUtil.hourUtil.getLastNumStr(s, -4))

    println(AuxUtil.hourUtil.getLastNumList(s, 5).mkString("{", ",", "}"))
    println(AuxUtil.hourUtil.getLastNumList(6).mkString("{", ",", "}"))

  }
}

