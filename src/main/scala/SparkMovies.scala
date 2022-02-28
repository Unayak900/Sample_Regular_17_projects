import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace, udf}

import java.text.SimpleDateFormat
import java.util.Locale



object SparkMovies {

  def getDateFromString(date:String): java.sql.Timestamp =
    {
    val formatter = new SimpleDateFormat("MMM DD YYYY", Locale.ENGLISH)
    val newformatted = new java.sql.Timestamp(formatter.parse(date).getTime)
    return newformatted
     }
    val getDateFromString_udf = udf(getDateFromString(_))


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
    // Date function





    val df3 = df2.withColumn("ReleaseDate",getDateFromString_udf(col("ReleaseDate")))
     df3.show(3,false)

    /*var strDate = "December 16 2015"
    //var format = DateTimeFormat.forPattern("MMM DD yyyy")
    val formatter = new SimpleDateFormat("MMM DD yyyy",Locale.ENGLISH)
    val newformatted = new java.sql.Timestamp(formatter.parse(strDate).getTime)
   // var date = format.parseDateTime(strDate)
    var td = trimLeft(newformatted)
    println("Here is the date with newformatted", td)*/


  }

}
