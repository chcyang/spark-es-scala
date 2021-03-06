package com.chc.dp.spark.scala.es

import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.slf4j.{Logger, LoggerFactory}


object EsImportSparkProcess extends EsSparkProcess {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val isDebug: Boolean = false

  def main(args: scala.Array[scala.Predef.String]): scala.Unit = {
    if (args.length >= 3) {
      //es.nodes
      val esNodes = args(0)
      // es.resource
      val esResource = args(1)
      //input path
      val inPath = args(2)

      // document id
      var documentId: String = null
      if (args.length == 4) {
        documentId = args(3).toString
      }
      process(esNodes, esResource, inPath, documentId)
    } else {
      logger.error("param nums not correct,please check it!")
      logger.error("need params at least: es.nodes, es.resource, out.path")
    }
  }


  def process(esNodes: String, esResource: String, inPath: String, documentId: String): Unit = {

    val name = "Elasticsearch import by spark"
    lazy val sparkConf = getSparkConf(name, isDebug)
    sparkConf.set("es.nodes", esNodes)
    sparkConf.set("es.batch.size.bytes", "300000000")
    sparkConf.set("es.batch.size.entries", "50000")
    sparkConf.set("es.batch.write.refresh", "false")

    lazy val sc = new SparkContext(sparkConf)

    val RDD = sc.textFile(inPath).filter(f => f != null || f != "")

    if (documentId != null) {
      RDD.saveJsonToEs(esResource, Map("es.mapping.id" -> documentId))
    } else {
      RDD.saveJsonToEs(esResource)
    }

    sc.stop()
  }


}
