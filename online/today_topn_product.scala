package online

import java.util.Calendar


import common.commonClasses.topn_sell
import common.commonFunc._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.spark.rdd.EsSpark

object today_topn_product {

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

    val Array(brokers, group_id, topics, offset) = args
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> offset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicArray = topics.split(",")


    val updateProductSell = (productid: String, currentSell: Option[Int], beforeSell: State[Int]) => {
      val lastestSell = currentSell.getOrElse(0) + beforeSell.getOption().getOrElse(0)
      val product_sell = (productid, lastestSell)
      beforeSell.update(lastestSell)
      product_sell
    }


    while (true) {
      val ssc = new StreamingContext(sc, Seconds(30))
      ssc.checkpoint("hdfs://master:9898/checkpoints/topn_sell/" + "[ ,:]".r.replaceAllIn(Calendar.getInstance().getTime.toString.substring(0, 13), "_"))

      val productPriorDStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topicArray, kafkaParams)).map(record => record.value())
      val productidDStream = productPriorDStream.map(x => (x.toString.split(",")(1), 1))
      val productSellStateDStream = productidDStream.mapWithState(StateSpec.function(updateProductSell)).stateSnapshots()

      productSellStateDStream.foreachRDD { rdd => {
        val time = getToday("yyyyMMddHHmmss")
        if (!rdd.partitions.isEmpty) {
          val top3Product = rdd.top(3)(Ordering.by[(String, Int), Int](_._2)).toBuffer
          if (top3Product.length < 3) {
            for (x <- top3Product.length until 3) {
              top3Product += "" -> 0
            }
          }
          val top3ProductRDD = SparkContext.getOrCreate()
            .makeRDD(
              Seq(topn_sell(time, Map("productid" -> top3Product.head._1, "sells" -> top3Product.head._2.toString),
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
      ssc.awaitTerminationOrTimeout(getTsToSpecDate(1))
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
