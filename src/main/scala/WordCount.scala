import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hpa on 2016/8/16 0016.
  */
object WordCount {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D://ftpdown//back//BOSS2POP_CPEINSTALL_2016052115433001.txt",1)
    val list: Array[String] = Array[String]("hello","world")
    val rdd = sc.parallelize(list)

    val rdd2 = sc.range(0,1,1)

    val words = lines.flatMap{line => line.split(" ")}

    val pairs = words.map{word => (word,1)}
    //pairs.reduceByKey((x,y) => x + y)
    val wordCountOrdered = pairs.reduceByKey(_+_).map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2,pair._1))

    wordCountOrdered.collect.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))
    while(true){

    }
    sc.stop()
  }
}
