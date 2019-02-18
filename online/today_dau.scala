package online

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.AccumulatorV2
import common.commonFunc.getToday
import common.commonFunc.getTsToSpecDate
import org.elasticsearch.spark.rdd.EsSpark

class LoginAccumulator extends AccumulatorV2[String, ArrayBuffer[String]] {
  private var res = ArrayBuffer[String]()

  override def isZero: Boolean = {
    true
  }

  override def reset = {
  }

  override def add(v: String): Unit = {
    res += v
  }

  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    other match {
      case any: LoginAccumulator =>
        res ++= any.value.diff(this.res)
      case _ => throw new UnsupportedOperationException(s"cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: ArrayBuffer[String] = {
    res
  }

  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = {
    val newAcc = new LoginAccumulator
    newAcc.res = this.res
    newAcc
  }

  def clear() = {
    res.clear()
  }
}


object today_dau {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      print("need 4 args, actually it is:" + args.length)
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count today login users dau")
      .set("es.index.auto.create", "true")
      .set("pushdown", "true")
      .set("es.nodes", "master")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")

    val sc = new SparkContext(conf)
    val loginAcc = new LoginAccumulator
    sc.register(loginAcc, "login acc")
    val Array(brokers, topics, group_id, offset) = args
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> offset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicArray = topics.split(",")

    while (true) {
      //      初始化当天已统计的用户列表
      val ssc = new StreamingContext(sc, Seconds(10))
      // 初始化当天已登录列表
      loginAcc.clear()
      val loginDStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topicArray, kafkaParams)).map(_.value())
      //      筛选出登录结果非ERROR的记录

      val loginSuccDStream = loginDStream.mapPartitions(part => {
        part.map(record => {
          val tmpArray = record.split(",").flatMap(x => x.split(" ")).flatMap(x => x.split(":"))
          if (tmpArray.length == 16) {
            Array(tmpArray(14), tmpArray(8), tmpArray(10))
          } else {
            Array("ERROR", "ERROR", "ERROR")
          }
        }).filter(x => x(1).toUpperCase != "ERROR" && x(2).toUpperCase == "LOGIN")
      })
      loginSuccDStream.foreachRDD(rdd => {
        val now = getToday("yyyyMMddHHmmss")
        rdd.foreachPartition(part => {
          part.foreach(record => {
            if (!loginAcc.value.contains(record(0))) {
              loginAcc.add(record(0))
            }
          })
        })
        val dauCnt = loginAcc.value.length
        println(dauCnt)
        EsSpark.saveToEs(SparkContext.getOrCreate().makeRDD(Seq(Map("time" -> now, "dau" -> dauCnt))), "today_dau/" + now.toString.take(8))
      })

      ssc.start()
      ssc.awaitTerminationOrTimeout(getTsToSpecDate(1))
      ssc.stop(false, true)

    }
  }

}
