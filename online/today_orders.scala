package online

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object today_orders {

  case class orderTrip(order_count: Long, money_sum: Double, order_date: String)

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      print("need 4 args, actually it is:" + args.length)
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count today total orders")
      .set("es.index.auto.create", "true")
      .set("pushdown", "true")
      .set("es.nodes", "master")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val df_product = spark.sql("select * from xfy.product")
    df_product.persist(newLevel = StorageLevel.MEMORY_AND_DISK_SER)


    val Array(zk, groupid, topic_product_prior, threads) = args
    val topicMap_product_prior = Map(topic_product_prior -> threads.toInt)
    val productPriorDStream = KafkaUtils.createStream(ssc, zk, groupid, topicMap_product_prior).map(_._2)

    val rdd2Df = (rdd: RDD[String]) => {
      val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
      import spark.implicits._
      val newRdd = rdd.map(_.split(",")).map(x => (x(0), x(1), x(2)))
      newRdd.toDF("order_id", "product_id", "add_to_card_order")
    }

    def count_distinct_orders(df: DataFrame): Long = {
      df.dropDuplicates("order_id").count()
    }

    def get_total_price(df: DataFrame): Double = {
      df_product.join(df.select("product_id"), df("product_id") === df_product("productid")).agg(sum("price")).first().getDouble(0)
    }


    val rddSaveEs = (rdd: RDD[orderTrip], location: String) => {
      EsSpark.saveToEs(rdd, location)
    }

    val dateformat = new SimpleDateFormat("yyyyMMdd")
    productPriorDStream.foreachRDD(rdd => {
      if (rdd.partitions.isEmpty) {
        println("this rdd is empty!!!")
      } else {
        val date = new Date()
        val today = dateformat.format(date)
        val df = rdd2Df(rdd)
        val orderCnt = count_distinct_orders(df)
        val moneySum = get_total_price(df)
        val rdd_distinct = SparkContext.getOrCreate().makeRDD(Seq(orderTrip(orderCnt, moneySum, today)))
        rddSaveEs(rdd_distinct, s"today_order_count/$today")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
