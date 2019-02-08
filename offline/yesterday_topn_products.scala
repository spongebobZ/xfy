package offline

import java.text.SimpleDateFormat
import java.util.Calendar
import common.commonClasses.topn_sell
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.rdd.EsSpark
import scala.collection.mutable.ArrayBuffer


object yesterday_topn_products {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("yesterday topn products").enableHiveSupport()
      .config("es.index.auto.create", "true")
      .config("pushdown", "true")
      .config("es.nodes", "master")
      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()

    val dateformat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateformat.format(cal.getTime)
    val df_prior = spark.sql("select * from xfy.yesterday_product_prior where order_date = '" + yesterday + "'")
    val list_product_topn = df_prior.groupBy("product_id")
      .agg(count("order_id"))
      .toDF("product_id", "sellCnt")
      .orderBy(desc("sellCnt"))
      .takeAsList(3)
    val product_topn_info = new ArrayBuffer[(String, String)]()
    for (x <- 0 until list_product_topn.size) {
      product_topn_info += list_product_topn.get(x).get(0).toString -> list_product_topn.get(x).get(1).toString
    }
    if (product_topn_info.size < 3) {
      for (x <- product_topn_info.size until 3) {
        product_topn_info += "" -> "0"
      }
    }
    val top3ProductRDD = spark.sparkContext.makeRDD(
      Seq(topn_sell(yesterday.toLong, Map("product_id" -> product_topn_info(0)._1, "sells" -> product_topn_info(0)._2),
        Map("product_id" -> product_topn_info(1)._1, "sells" -> product_topn_info(1)._2),
        Map("product_id" -> product_topn_info(2)._1, "sells" -> product_topn_info(2)._2))))
    EsSpark.saveToEs(top3ProductRDD, s"$yesterday/topn_products")
    spark.stop()
  }
}
