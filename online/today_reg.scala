package online

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import common.commonClasses.reg_log
import common.commonFunc._
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.spark.rdd.EsSpark

object today_reg {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      print("need 4 args, actually it is:" + args.length)
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count today register users")
      .set("es.index.auto.create", "true")
      .set("pushdown", "true")
      .set("es.nodes", "master")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))
    val Array(brokers, topics, group_id, offset) = args
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> offset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicArray = topics.split(",")
    val messages = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topicArray, kafkaParams)).map(_.value())
    //    对reg log每一行提取日期,日志级别,名字
    val formatMessages = messages.mapPartitions(part => {
      part.map(record => {
        val tmpArray = record.split(",").flatMap(x => x.split(" ")).flatMap(x => x.split(":"))
        if (tmpArray.length == 17) {
          Array(tmpArray(14), tmpArray(8))
        } else {
          Array("ERROR", "ERROR")
        }
      }).filter(x => x(1).toUpperCase != "ERROR")
    })
    //    保存每个batch的rdd的count至es(rdd已经过清洗)
    formatMessages.foreachRDD(rdd => {
      val today = getToday()
      val regCnt = rdd.count()
      val cntRDD = SparkContext.getOrCreate().makeRDD(Seq(reg_log(today, regCnt)))
      EsSpark.saveToEs(cntRDD, s"today_reg/$today")
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
