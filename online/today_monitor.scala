package online


import common.commonFunc.getToday
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

/*
  1.同时消费整个集群的内存日志，需要根据hostname区分不同节点
  2.所有节点的监控日志均发到kafka的topic：monitor，因此flume中需要使用interceptor在event的header中加上各自的hostname信息
  3.streaming接收到kafka的message后首先根据key来划分不同节点的数据，再根据数据类型来划分不同的监控标的
 */
object today_monitor {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      print("need 4 args, actually it is:" + args.length)
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("realtime memory used")
      .set("es.index.auto.create", "true")
      .set("pushdown", "true")
      .set("es.nodes", "master")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")
    val ssc = new StreamingContext(conf, Seconds(5))
    val Array(brokers, topics, groupid, offset) = args
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> offset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    def rdd2DF(rdd: RDD[Array[String]], rows: Seq[String]): DataFrame = {
      val spark = SparkSession.builder().config(conf).getOrCreate()
      import spark.implicits._
      rdd.toDF()
    }

    val topicArray = topics.split(",")
    val monitorMessage = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topicArray, kafkaParams))
    monitorMessage.foreachRDD(rdd => {
      val now = getToday("yyyyMMddHHmmss")
      val rdd_format = rdd.mapPartitions(part => {
        part.map(record => {
          val v = record.value().split(" ").filter(_.nonEmpty)
          try {
            v.length match {
              case 9 => record.key() -> Array(1D, v(3).toDouble)
              case 12 => record.key() -> Array(2D, v(2).toDouble, v(3).toDouble, v(4).toDouble)
              case 7 => record.key() -> Array(3D, v(2).toDouble, v(3).toDouble, v(4).toDouble)
              case 10 => record.key() -> Array(4D, v(3).toDouble, v(5).toDouble, v(6).toDouble)
              case _ => record.key() -> Array[Double]()
            }
          } catch {
            case e: NumberFormatException => record.key() -> Array[Double]()
          }
        }).filter(_._2.nonEmpty)
      })
      rdd_format.persist()
      val cpuRDD = rdd_format.mapPartitions(part => part.filter(_._2(0) == 1D).map(x => x._1 -> (x._2(1), 1)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .mapPartitions(part => part.map(x => Map("hostname" -> x._1, "cpuPercent" -> x._2._1 / x._2._2, "time" -> now)))

      val memoryRDD = rdd_format.mapPartitions(part => part.filter(_._2(0) == 2D).map(x => x._1 -> (x._2(1), x._2(2), x._2(3), 1)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
        .mapPartitions(part => part.map(x => Map("hostname" -> x._1, "freeMemory" -> x._2._1 / x._2._4, "usedMemory" -> x._2._2 / x._2._4, "memoryPercent" -> x._2._3 / x._2._4,"time" -> now))
        )

      val diskIORDD = rdd_format.mapPartitions(part => part.filter(_._2(0) == 3D).map(x => x._1 -> (x._2(1), x._2(2), x._2(3), 1)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
        .mapPartitions(part => part.map(x => Map("hostname" -> x._1, "tps" -> x._2._1 / x._2._4, "rtps" -> x._2._2 / x._2._4, "wtps" -> x._2._3 / x._2._4,"time" -> now)))

      val netIORDD = rdd_format.mapPartitions(part => part.filter(_._2(0) == 4D).map(x => x._1 -> (x._2(1), x._2(2), x._2(3), 1)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
        .mapPartitions(part => part.map(x => Map("hostname" -> x._1, "rxpck" -> x._2._1 / x._2._4, "rxkb" -> x._2._2 / x._2._4, "txkb" -> x._2._3 / x._2._4,"time" -> now)))

      rdd_format.unpersist()
      if (!cpuRDD.isEmpty()) {
        EsSpark.saveToEs(cpuRDD, s"realtime_cpu/${now.toString.take(8)}")
      }
      if (!memoryRDD.isEmpty()) {
        EsSpark.saveToEs(memoryRDD, s"realtime_memory/${now.toString.take(8)}")
      }
      if (!diskIORDD.isEmpty()) {
        EsSpark.saveToEs(diskIORDD, s"realtime_diskio/${now.toString.take(8)}")
      }
      if (!netIORDD.isEmpty()) {
        EsSpark.saveToEs(netIORDD, s"realtime_netio/${now.toString.take(8)}")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
