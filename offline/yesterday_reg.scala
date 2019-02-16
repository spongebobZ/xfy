package offline

import common.commonFunc.getSpecDate
import org.apache.spark.sql.SparkSession
import common.commonClasses.reg_log
import org.elasticsearch.spark.rdd.EsSpark

object yesterday_reg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("yesterday reg")
      .enableHiveSupport()
      .config("es.index.auto.create", "true")
      .config("pushdown", "true")
      .config("es.nodes", "master")
      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()

    val yesterday = getSpecDate(-1)
    val df_reg = spark.sql("select * from xfy.reg where reg_time='" + yesterday + "'")
    val regCnt = df_reg.filter("log_level='INFO'").count()
    val cntRDD = spark.sparkContext.makeRDD(Seq(reg_log(yesterday, regCnt)))
    EsSpark.saveToEs(cntRDD, s"$yesterday/reg")
    spark.stop()
  }


}
