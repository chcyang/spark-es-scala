package com.chc.dp.spark.scala.es

import org.apache.spark.SparkConf

trait EsSparkProcess {

  def getSparkConf(name: String, isDebug: Boolean): SparkConf = {

    lazy val sparkConf = new SparkConf().setAppName(name)
    if (isDebug) {
      sparkConf.setMaster("local[2]")
    }
    sparkConf.set("es.batch.write.retry.wait", "30")
    sparkConf.set("es.batch.write.retry.count", "10")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("es.http.timeout", "30s")
    sparkConf.set("es.http.retries", "3")
    sparkConf.set("es.action.heart.beat.lead", "50")
    sparkConf
  }

}
