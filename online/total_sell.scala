package online

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object total_sell {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      print("need 3 args, actually it is:" + args.length)
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("count total sell")
    val ssc = new StreamingContext(conf, Seconds(5))
//    ssc.checkpoint("./src/checkpoints")
    val updateFunc = (value: Seq[Long], old: Option[Long]) => {
      val oldValue = old.getOrElse(0L)
      val newValue = value.sum
      Some(oldValue + newValue)
    }

    val Array(zk,groupid,topic) = args
    val topic_map = Map(topic -> 1)
    val orderStream = KafkaUtils.createStream(ssc,zk,groupid,topic_map)
    orderStream.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
