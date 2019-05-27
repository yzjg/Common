package com.hynoo.spark

import java.sql.{DriverManager, ResultSet, Statement, Connection}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object QueryRegister extends Logging {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("QueryRegister")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    parse(sqlContext)

    sc.stop()
  }

  // 获取数据库中元数据信息
  def getMetaData(sqlContext: SQLContext) = {
    val url = sqlContext.getConf("spark.sql.query.datasource.url",
      "jdbc:mysql://localhost:3306/ext_source?user=root&password=root")

    val dataSourceTable = sqlContext.getConf("spark.sql.query.datasource.table",
      "ExDataSource")

    // Load the driver
    Class.forName("com.mysql.jdbc.Driver")

    //store data source
    val dataSources = new ListBuffer[DataSource]()

    var conn: Connection = null
    var stat: Statement = null
    var rs: ResultSet = null

    try {
      //setup the connection
      conn = DriverManager.getConnection(url)

      //configure to be read only
      stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      //execute query
      rs = stat.executeQuery("SELECT * FROM " + dataSourceTable)

      //Iterate over ResultSet
      while (rs.next()) {
        val url = rs.getString("URL")
        val username = rs.getString("USERNAME")
        val password = rs.getString("PASSWORD")
        val sourceTable = rs.getString("SOURCE_TABLE")
        val targetTable = rs.getString("TARGET_TABLE")

        /**
         * 1:hive  2:mysql  3:oracle   4:db2  5:json  6:parquet  7:phoenix
         */
        val dataSourceType = rs.getString("DATASOURCE_TYPE")

        dataSources += DataSource(dataSourceType.toInt, url, sourceTable, targetTable, username, password)
      }
    } finally {
      rs.close()
      stat.close()
      conn.close()
    }
    dataSources
  }


  /**
   * 解析数据源配置表并注册成Spark SQL中的临时表
   */
  def parse(sqlContext: SQLContext): Unit = {

    // get the query configuration info from RDBMS
    val dataSources = getMetaData(sqlContext)

    for (dataSource <- dataSources) {
      dataSource.dataSourceType match {
        //hive
        case 1 =>
          logInfo("load hive:" + dataSource.sourceTable + " to " + dataSource.targetTable + " start...")
          sqlContext.table(dataSource.sourceTable).registerTempTable(dataSource.targetTable)
          logInfo("load hive:" + dataSource.sourceTable + " to " + dataSource.targetTable + " success...")

        //mysql
        case 2 => {
          logInfo("load mysql:" + dataSource.sourceTable + " to " + dataSource.targetTable + " start...")
          sqlContext.read.format("jdbc").options(
            Map(
              "url" -> dataSource.url,
              "dbtable" -> dataSource.sourceTable,
              "driver" -> "com.mysql.jdbc.Driver",
              "user" -> dataSource.username,
              "password" -> dataSource.password))
            .load.registerTempTable(dataSource.targetTable)
          logInfo("load mysql:" + dataSource.sourceTable + " to " + dataSource.targetTable + " success...")
        }

        //json
        case 5 =>
          logInfo("load json:" + dataSource.url + " to " + dataSource.targetTable + " start...")
          sqlContext.read.format("json").load(dataSource.url).registerTempTable(dataSource.targetTable)
          logInfo("load json:" + dataSource.url + " to " + dataSource.targetTable + " success...")

        //parquet
        case 6 =>
          logInfo("load parquet:" + dataSource.url + " to " + dataSource.targetTable + " start...")
          sqlContext.read.parquet(dataSource.url).registerTempTable(dataSource.targetTable)
          logInfo("load parquet:" + dataSource.url + " to " + dataSource.targetTable + " success...")

        case _ =>
      }
    }

    sqlContext.sql("select e.EMPNO, e.ENAME, d.deptno, d.dname from json_dept d join mysql_emp e on d.deptno = e.DEPTNO").show
  }
}
