package common

object commonClasses {

  case class topn_sell(date: Long, first_info: Map[String, String], second_info: Map[String, String], third_info: Map[String, String])

  case class reg_log(date: Long, regCnt: Long)

  case class login_log(date: Long, dau: Long)

}
