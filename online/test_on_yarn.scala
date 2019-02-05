package online

import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object test_on_yarn {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("test for scala")
    val sc = new SparkContext(conf)


    val updateFunc = (product_id: String, newValue: Option[Int], oldState: State[Int]) => {
      val newState = newValue.getOrElse(0) + oldState.getOption().getOrElse(0)
      val wordCount = (product_id, newState)
      oldState.update(newState)
      wordCount
    }
    while (true) {
      val ssc = new StreamingContext(sc, Seconds(10))
      ssc.checkpoint("hdfs://master:9898/checkpoints/test/" + "[ ,:]".r.replaceAllIn(Calendar.getInstance().getTime.toString.substring(0, 16), "_"))
      val DStream = KafkaUtils.createStream(ssc, "master:2181,slave1:2181,slave2:2181", "testgrp", Map("product_prior" -> 1))
      val ds_format = DStream.map(_._2.split(","))
      ds_format.map(x => (x(1), 1)).mapWithState(StateSpec.function(updateFunc)).stateSnapshots().repartition(2).foreachRDD(rdd =>
        if (!rdd.isEmpty()) {
          {
            rdd.top(10)(Ordering.by[(String, Int), Int](_._2)).foreach(println(_))
            println("================================")
          }
        })

      ssc.start()
      ssc.awaitTerminationOrTimeout(60000)
      ssc.stop(false, true)
    }
  }

}
