package offline

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL
import common.commonFunc._

object yesterday_sell {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("yesterday data")
      .enableHiveSupport()
      .config("es.index.auto.create", "true")
      .config("pushdown", "true")
      .config("es.nodes", "master")
      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()


    val yesterday = getSpecDate(-1)
    val df_prior = spark.sql("select * from xfy.yesterday_product_prior where order_date = '" + yesterday + "'")
    val df_order = spark.sql("select * from xfy.yesterday_orders where order_date = '" + yesterday + "'").selectExpr("order_id as order_id_tmp", "client")
    val df_prior_with_client = df_prior.join(df_order, df_prior("order_id") === df_order("order_id_tmp"))
    val df_distinct = df_prior_with_client.dropDuplicates(Seq("order_id"))
    import spark.implicits._
    var yesterday_cnt = 0L
    var pc_count = 0L
    var ios_count = 0L
    var android_count = 0L
    var yesterday_money = 0D
    var pc_money = 0D
    var ios_money = 0D
    var android_money = 0D
    if (!df_prior_with_client.rdd.isEmpty()) {
      yesterday_cnt = df_distinct.count()
      pc_count = df_distinct.filter("client = 'pc'").count()
      ios_count = df_distinct.filter("client = 'iOS'").count()
      android_count = yesterday_cnt - pc_count - ios_count

      val df_product = spark.sql("select * from xfy.product")
      val df_tmp = df_product.join(df_prior_with_client, df_product("productid") === df_prior_with_client("product_id"))
      yesterday_money = df_tmp.agg(sum("price")).first().getDouble(0)
      pc_money = df_tmp.filter("client = 'pc'").agg(sum("price")).first().getDouble(0)
      ios_money = df_tmp.filter("client = 'iOS'").agg(sum("price")).first().getDouble(0)
      android_money = yesterday_money - pc_money - ios_money
    }
    val df_format = Seq((yesterday_cnt, pc_count, ios_count, android_count, yesterday_money, pc_money, ios_money, android_money, yesterday))
      .toDF("total_orders", "pc_count", "ios_count", "android_count", "money_sum", "pc_money", "ios_money", "android_money", "order_date")
    EsSparkSQL.saveToEs(df_format, s"$yesterday/order_count")
    spark.stop()
  }

}
