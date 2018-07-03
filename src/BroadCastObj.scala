import scala.tools.scalap.Main
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object BroadCastObj extends App{
  
  val conf = new SparkConf().setAppName("Spark broadcast job").setMaster("local")
  val sc = new SparkContext(conf)
  
  case class TXN(amount: Int, date: String)
  case class USER(email: String, name: String)
  
  val txFile = sc.textFile("mapreduce/transactions.txt")
  val userFile = sc.textFile("mapreduce/users.txt")
  
  val txPairs = txFile.map(x=>x.split(","))
  val userPairs = userFile.map(x=>x.split(","))
  
  val txFiltered = txPairs.map{x=>(x(2).toInt, TXN(amount=x(3).toInt,date=x(1)))}
  val userFiltered = userPairs.map{x=>(x(0).toInt, USER(email = x(1), name = x(2)))}
  
  val userMapBroadCast = userFiltered.collectAsMap()
  sc.broadcast(userMapBroadCast)
  
  val maxTxn = txFiltered.reduceByKey((x,y) => if(x.amount > y.amount) x else y)
  val result = maxTxn.map(x=>(x._2.amount, x._2.date, userMapBroadCast.get(x._1).get.email))
  
  result.collect.foreach(print)
  
  
  
      
      
  
  
}