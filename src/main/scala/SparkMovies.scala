

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace, udf}

import java.util.Locale
//import java.sql.Date
import java.text.SimpleDateFormat


object SparkMovies {

  def getDateFromString(date:String): java.sql.Date =
  {

    val formatter = new SimpleDateFormat("MMMMM dd yyyy", Locale.ENGLISH)
    val newformatted = new java.sql.Date(formatter.parse(date).getTime)
    return newformatted
  }

 /* val formatter = new SimpleDateFormat("MMMMM dd yyyy", Locale.ENGLISH)
  val dateregex = "^\\d{5}-\\d{2}-\\d{2}$"
  val dateonly = dateregex.r

  def getDateFromString(stringdate: String):  java.sql.Date = {
    val newformatted = new java.sql.Date(formatter.parse(stringdate).getTime)

     stringdate.trim match
    {
      case dateonly() => newformatted
      case e:DateFormatException => errorHandler(e)

    }
    getDateFromString(stringdate)
  }*/


  /*val formatter = new SimpleDateFormat("MMMMM dd yyyy")
  def getDateFromString(stringdate: String): Either[String, java.util.Date] =
 {
   Try {
     formatter.parse(stringdate).getTime
   }
      match {
     case Success(s) => Right(s)
     case Failure(e: ParseException) => Left(s"bad format:$stringdate")
     case Failure(e: Throwable) => Left(s"Unknown error formatting: $stringdate")
   }
   getDateFromString(stringdate)
 }*/

  val getDateFromString_udf = udf(getDateFromString(_))

  /*case ioException:IOException => Left(ioException)
case e: Exception => Left(e)
val formatter = new SimpleDateFormat("MMMMM dd yyyy", Locale.ENGLISH)
val newformatted = new java.sql.Date(formatter.parse(date).getTime)
return newformatted*/






  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(name = "SparkMovies").config("spark.master", "local[*]").getOrCreate()
    import spark.sqlContext.implicits._

      val df = spark.read.format("csv")
               .option("escapeQuotes",true)
              .option("header",true)
             .load("file:///home/hduser/PRAC/HighestHolywoodGrossingMovies.csv")
      df.show(3,false)

     val newcol = df.columns

     val newformatcol = newcol.map(x=>x.replace(" ","")).map(x=>x.replace(("(in"),"")).
       map(x=>x.replace("$)",""))

     val newdf = df.toDF(newformatcol:_*)

    newdf.show(3,false)

    val df2 = newdf.withColumn("ReleaseDate",regexp_replace(col("ReleaseDate"),",",""))
    df2.show(2,false)

    val df3 = df2.withColumn("ReleaseDate",regexp_replace(col("ReleaseDate"),"NA","January 01 1900"))
    // Date function


    val df4 = df3.limit(5)
    val df5 = df4.withColumn("ReleaseDate", getDateFromString_udf(col("ReleaseDate")))
    df5.show(5,false)


    /*var strDate = "December 16 2015"
    //var format = DateTimeFormat.forPattern("MMM DD yyyy")
    val formatter = new SimpleDateFormat("MMM DD yyyy",Locale.ENGLISH)
    val newformatted = new java.sql.Timestamp(formatter.parse(strDate).getTime)
   // var date = format.parseDateTime(strDate)
    var td = trimLeft(newformatted)
    println("Here is the date with newformatted", td)*/




  }

}
