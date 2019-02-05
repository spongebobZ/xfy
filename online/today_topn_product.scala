package online

import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

object today_topn_product {

  case class topn_sell(date: Long, first_info: Map[String, String], second_info: Map[String, String], third_info: Map[String, String])

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      print("need 4 args, actually it is:" + args.length)
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count today topn products")
      .set("es.index.auto.create", "true")
      .set("pushdown", "true")
      .set("es.nodes", "master")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")

    val sc = new SparkContext(conf)


    val updateProductSell = (productid: String, currentSell: Option[Int], beforeSell: State[Int]) => {
      val lastestSell = currentSell.getOrElse(0) + beforeSell.getOption().getOrElse(0)
      val product_sell = (productid, lastestSell)
      beforeSell.update(lastestSell)
      product_sell
    }
    val Array(zk, groupid, topic_orders, threads) = args
    val topicMap_product_prior = Map(topic_orders -> threads.toInt)

    def getTsToTomorrow = {
      val cal = Calendar.getInstance()
      val now = cal.getTime.getTime
      cal.add(Calendar.DATE, 1)
      cal.set(Calendar.HOUR_OF_DAY, 0)
      cal.set(Calendar.MINUTE, 0)
      cal.set(Calendar.SECOND, 0)
      val tomorrow = cal.getTime.getTime
      tomorrow - now
    }

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")


    while (true) {
      val ssc = new StreamingContext(sc, Seconds(10))
      ssc.checkpoint("hdfs://master:9898/checkpoints/topn_sell/" + "[ ,:]".r.replaceAllIn(Calendar.getInstance().getTime.toString.substring(0, 13), "_"))

      val productPriorDStream = KafkaUtils.createStream(ssc, zk, groupid, topicMap_product_prior)
      val productidDStream = productPriorDStream.map(x => (x._2.split(",")(1), 1))
      val productSellStateDStream = productidDStream.mapWithState(StateSpec.function(updateProductSell)).stateSnapshots()
      productSellStateDStream.foreachRDD { rdd => {
        val time = dateFormat.format(new Date()).toLong
        if (!rdd.partitions.isEmpty) {
          println(Calendar.getInstance().getTime)
          val top3Product = rdd.top(3)(Ordering.by[(String, Int), Int](_._2))
          val top3ProductRDD = SparkContext.getOrCreate()
            .makeRDD(
              Seq(topn_sell(time, Map("productid" -> top3Product(0)._1, "sells" -> top3Product(0)._2.toString),
                Map("productid" -> top3Product(1)._1, "sells" -> top3Product(1)._2.toString),
                Map("productid" -> top3Product(2)._1, "sells" -> top3Product(2)._2.toString))))
          EsSpark.saveToEs(top3ProductRDD, "today_topn_product/" + time.toString.take(8))
        }
        else {
          println("this batch is empty")
        }
      }
      }


      ssc.start()
      ssc.awaitTerminationOrTimeout(getTsToTomorrow)
      ssc.stop(false, true)
      var ssc_flag = 0
      while (ssc_flag == 0) {
        if (ssc.getState().toString == "STOPPED") {
          println("ssc is stopped")
          ssc_flag = 1
        } else {
          println("ssc still running")
        }
      }

    }


  }

}
