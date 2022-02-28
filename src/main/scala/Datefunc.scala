import java.text.SimpleDateFormat

object Datefunc {
  def getDateFromString(date:String) : java.sql.Timestamp =
  {
    val formatter = new SimpleDateFormat("december 16, 2014")
    val newformatted = new java.sql.Timestamp(formatter.parse(date).getTime)
    return newformatted
  }


}
