import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession




object MovieRatingProblem extends App {
  val conf = new SparkConf().setAppName("Movie rating problem")
  val sc = new SparkContext(conf)
  val spark = SparkSession
            .builder()
            .appName("Spark movie example")
            .getOrCreate()
            
  case class Movie(MID: Int, MNAME: String, GENRE: String)
  case class Rating(UID: Int, MOVIEID: Int, RATING: Int, TIMESTAMP: Long) 
  
  val movieDataFile = sc.textFile("mapreduce/movies.dat").map(x=>x.split("::"))
  val ratingDataFile = sc.textFile("mapreduce/ratings.dat").map(x=>x.split("::"))
  
  val movieMapped = movieDataFile.map{ (x=>Movie(MID = x(0).toInt, MNAME = x(1), GENRE = x(2))) }
  val ratingMapped = ratingDataFile.map{ (x=>Rating(UID = x(0).toInt, MOVIEID = x(1).toInt, 
      RATING = x(2).toInt, TIMESTAMP = x(3).toLong))}
  
  val ratingDF = spark.sqlContext.createDataFrame(ratingMapped)
  ratingDF.registerTempTable("rating_table")
  
  val movieDF = spark.sqlContext.createDataFrame(movieMapped)
  
  val filteredRatingData = spark.sqlContext.sql("select MOVIEID, COUNT(*) as count from rating_table where rating == 5 group by MOVIEID having count > 1000")
  
  movieDF.join(filteredRatingData, filteredRatingData("MOVIEID") === movieDF("MID"))
  .select("MOVIEID", "GENRE")
  .show()
      
      

            
}