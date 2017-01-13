package com.hpa.bigdata.kerneldecryption

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hpa on 2016/11/20.
  */
object Lession15_RDDBasedOnLocalFile {
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("Lession15_RDDBasedOnLocalFile")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val lineLengths = sc.textFile("D:\\spark\\spark-sourcecode\\spark2.0.2\\README.md").map(line => line.length);

    val sum = lineLengths.reduce(_+_)

    println("the all line length of the local file: " + sum)
  }
}
