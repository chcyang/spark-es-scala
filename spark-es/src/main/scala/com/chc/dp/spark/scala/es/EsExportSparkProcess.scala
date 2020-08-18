package com.chc.dp.spark.scala.es

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.slf4j.LoggerFactory

object EsExportSparkProcess {

  val logger = LoggerFactory.getLogger(this.getClass)
  val  isDebug: Boolean = false

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
    lazy val sparkConf = new SparkConf().setAppName(name)
    if (isDebug) {
      sparkConf.setMaster("local[2]")
    }
    sparkConf.set("es.nodes", esNodes)
    sparkConf.set("es.batch.size.bytes", "300000000")
    sparkConf.set("es.batch.size.entries", "100000")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("es.http.timeout", "10m")
    sparkConf.set("es.http.retries", "50")
    sparkConf.set("es.action.heart.beat.lead", "50")
    sparkConf.set("es.scroll.size","50000")
    lazy val sc = new SparkContext(sparkConf)
    //    sc.esRDD("index/type")
    //    sc.esRDD("index/type","?q=me*")

    var esrdd: RDD[scala.Tuple2[scala.Predef.String, scala.Predef.String]] = sc.esJsonRDD(esResource);
    if (null != queryString) {
      esrdd = sc.esJsonRDD(esResource, queryString);
    }
    esrdd.values.saveAsTextFile(outPath);

    sc.stop()
  }

}
