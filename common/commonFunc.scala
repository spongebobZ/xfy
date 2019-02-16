package common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object commonFunc {
  val dateformat = new SimpleDateFormat("yyyyMMdd")

  def getToday(formater: String = "") = {
    val date = new Date()
    if (formater.isEmpty) {
      dateformat.format(date).toLong
    } else {
      val specFormat = new SimpleDateFormat(formater)
      specFormat.format(date).toLong
    }
  }

  def getSpecDate(amount: Int) = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, amount)
    dateformat.format(cal.getTime).toLong
  }

  def getTsToSpecDate(amount: Int) = {
    val cal = Calendar.getInstance()
    val now = cal.getTime.getTime
    cal.add(Calendar.DATE, amount)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    val specDate = cal.getTime.getTime
    specDate - now
  }
}
