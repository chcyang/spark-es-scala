package com.chc.dp.spark.scala.es

object EsImportSparkProcessTest {

  def main(args: Array[String]): Unit = {

    val argus = Array("192.168.100.51:39203",
      "index_name/type",
      "E:\\data\\es_export",
      "hbase_rowkey")
    EsImportSparkProcess.main(argus)
  }

}
