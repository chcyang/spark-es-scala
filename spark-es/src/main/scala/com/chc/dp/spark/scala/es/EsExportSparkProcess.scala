package com.chc.dp.spark.scala.es

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import org.slf4j.{Logger, LoggerFactory}

object EsExportSparkProcess extends EsSparkProcess {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val isDebug: Boolean = false

  def main(args: scala.Array[scala.Predef.String]): scala.Unit = {
    if (args.length >= 3 || args.length <= 4) {
      //es.nodes
      val esNodes = args(0)
      // es.resource
      val esResource = args(1)
      //output path
      val outPath = args(2)

      if (args.length == 4) {
        //query string
        //queryString = "?q=me*"
        val queryString = args(3)
        process(esNodes, esResource, outPath, queryString)
      } else {
        process(esNodes, esResource, outPath, null)
      }
    } else {
      logger.error("param nums not correct,please check it!")
      logger.error("need params at least: es.nodes, es.resource, out.path")
    }
  }


  def process(esNodes: String, esResource: String, outPath: String, queryString: String): Unit = {

    val name = "Elasticsearch export by spark"
    lazy val sparkConf = getSparkConf(name, isDebug)
    sparkConf.set("es.nodes", esNodes)

    lazy val sc = new SparkContext(sparkConf)

    var esRdd: RDD[(String, String)] = sc.esJsonRDD(esResource)
    if (null != queryString) {
      esRdd = sc.esJsonRDD(esResource, queryString)
    }
    esRdd.values.saveAsTextFile(outPath)

    sc.stop()
  }

}
