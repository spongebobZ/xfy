package offline

import org.apache.spark.sql.SparkSession
import common.commonFunc.getSpecDate
import common.commonClasses.login_log
import org.elasticsearch.spark.rdd.EsSpark

object yesterday_dau {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("yesterday dau")
      .enableHiveSupport()
      .config("es.index.auto.create", "true")
      .config("pushdown", "true")
      .config("es.nodes", "master")
      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()

    val yesterday = getSpecDate(-1)
    val df_login = spark.sql("select * from xfy.login where login_date ='" + yesterday + "'")
    val loginCnt = df_login.filter("log_level='INFO'").distinct().count()
    val cntRDD = spark.sparkContext.makeRDD(Seq(login_log(yesterday, loginCnt)))
    EsSpark.saveToEs(cntRDD, s"$yesterday/login")
    spark.stop()
  }
}
