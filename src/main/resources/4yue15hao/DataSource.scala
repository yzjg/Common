package com.hynoo.spark

/**
 * 数据源模型
 * @param dataSourceType  数据源类型 1:hive  2:mysql  3:oracle   4:db2  5:json  6:parquet  7:phoenix
 * @param url  源数据url
 * @param sourceTable  源数据表
 * @param targetTable  目标表
 * @param username  用户名
 * @param password  密码
 */
case class DataSource(dataSourceType: Int,
                      url: String,
                      sourceTable: String,
                      targetTable: String,
                      username: String,
                      password: String)