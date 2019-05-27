package com.hynoo.spark.cdn

import java.util.Date

import com.hynoo.spark.util.{LogConvertUtils, FileUtils}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

object CdnJob extends Logging {
  val checkPeriod = 20000
  val checkTimes = 9
  val coalesceSize = 128
  val partitions = "d,h,m5"
  val inputPath = ""
  val outputPath = ""

  def getTemplate: String = {
    var template = ""
    val partitionArray = partitions.split(",")
    for (i <- 0 until partitionArray.length)
      template = template + "/" + partitionArray(i) + "=*"
    template
  }

  val fastDateFormat = FastDateFormat.getInstance("yyMMddHHmm")

  def doJob(parentContext: HiveContext, fileSystem: FileSystem, tableName: String, loadTime: String, fileCount: Int): String = {
    var response = "成功"
    var jobContext: HiveContext = null

    try {

      //TODO... 添加业务逻辑
      var begin = new Date().getTime

      try {
        val fileUploaded = FileUtils.checkFileUpload(fileSystem, inputPath, fileCount, checkPeriod, checkTimes, 0)

        if (fileUploaded < fileCount) {
          logError("失败 文件上传不完全,文件路径[" + inputPath + "]期望文件数[" + fileCount + "]实际文件数[" + fileUploaded + "]")
          System.exit(1)
        }

        val coalesce = FileUtils.makeCoalesce(fileSystem, inputPath, coalesceSize)
        logInfo(s"$inputPath , $coalesceSize, $coalesce")

        var logDF = jobContext.read.text(inputPath).coalesce(coalesce)

        logDF = jobContext.createDataFrame(logDF.map(x =>
          LogConvertUtils.parseLog(x.getString(0))).filter(_.length != 1), LogConvertUtils.struct)

        logDF.write.mode(SaveMode.Overwrite).format("orc")
          .partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + loadTime)


        begin = new Date().getTime

        val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadTime + getTemplate + "/*.orc"))
        val filePartitions = new mutable.HashSet[String]
        for (i <- 0 until outFiles.length) {
          val nowPath = outFiles(i).getPath.toString
          filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadTime, "").substring(1))
        }

        FileUtils.moveTempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)


        begin = new Date().getTime
        filePartitions.foreach(partition => {
          var d = ""
          var h = ""
          var m5 = ""
          partition.split("/").map(x => {
            if (x.startsWith("d=")) {
              d = x.substring(2)
            }
            if (x.startsWith("h=")) {
              h = x.substring(2)
            }
            if (x.startsWith("m5=")) {
              m5 = x.substring(3)
            }
            null
          })
          if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
            val sql = s"alter table test_cdncol add IF NOT EXISTS partition(d='$d', h='$h',m5='$m5')"
            logInfo(s"partition $sql")
            jobContext.sql(sql)
          }
        })
      } catch {
        case e: Exception =>
          logError("执行异常", e)
          var message = e.getMessage
          if (message.indexOf('\r') != -1)
            message = message.substring(0, message.indexOf('\r'))
          if (message.indexOf('\n') != -1)
            message = message.substring(0, message.indexOf('\n'))
          response = "执行异常" + message
      } finally {
        if (jobContext != null) {
          jobContext.clearCache()
          jobContext = null
        }
      }
      response
    }
  }
}