package com.hpa.bigdata.kerneldecryption

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/11/21.
  */
object Lession16_CountSameLengthLines {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Lession_CountSameLengthLines").setMaster("local")

    val sc = new SparkContext(conf)

    val linesRDD = sc.textFile("D:\\idea_workspaces\\sparkdemo\\src\\main\\resources\\data\\lession16\\data.txt")
    val mapRDD = linesRDD.map(line => (line, 1))
    val reduceByKeyRDD = mapRDD.reduceByKey(_+_)

    /**
      * collect后Array的元素是Tuple
      */
    reduceByKeyRDD.collect().foreach(pair2 => println(pair2._1 + " : " + pair2._2))

    /**
      * 运行结果：
      * spark : 3
      * spark hadoop : 2
      * hadoop : 2
      * flume : 1
      * hadoop spark : 1
      * kafka : 1
      */
  }
}
