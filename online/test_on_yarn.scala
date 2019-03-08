package online


import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object test_on_yarn {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("test for scala").setMaster("local[*]")

    val Array(brokers, topics, group_id, offset) = args
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> offset,
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topicArray = topics.split(",")
    val ssc = new StreamingContext(conf, Seconds(10))

    val DStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topicArray, kafkaParams))
    val keyStream = DStream.map(_.key())
    val valueStream = DStream.map(_.value())
    valueStream.foreachRDD(rdd => {
      println("================RDD================")
      rdd.foreachPartition(part=>{
        println(part.isEmpty)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

