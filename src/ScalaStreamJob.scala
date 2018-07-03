import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Durations


object ScalaStreamJob {
  
  val conf = new SparkConf().setAppName("Spark stream job").setMaster("local")
  val ssc = new StreamingContext(conf, Seconds(2))
  
  val lines = ssc.socketTextStream("localhost", 9988)
  val words = lines.flatMap{ x=>x.split(" ") }
  val wordPairs = words.map { x=> (x,1)}
  
  val wordCounts = wordPairs.reduceByKeyAndWindow(((a: Int, b: Int) => a+b), 
      Durations.seconds(30), Durations.seconds(10))
      wordCounts.print()
  
  ssc.start()
  ssc.awaitTermination()
  
  
  
  
  
  
  
  
  
}