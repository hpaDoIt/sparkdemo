package com.hpa.bigdata.kerneldecryption

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hpa on 2016/11/20.
  */
object Lession15_RDDBasedOnCollections {
  def main(args : Array[String]){
    println("hello world!");
    val conf = new SparkConf().setAppName("Lession15_RDDBasedOnCollections")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    //创建一个Scala集合
    val collection = 1 to 100
    //val rdd = sc.parallelize(collection)
    //可以通过第二个参数指定并行度
    val rdd = sc.parallelize(collection, 10)

    //由于Scala自动类型推导，因此reduce的参数类型为int
    val sum = rdd.reduce(_ + _)

    println("1 + 2 + 3 + ... + n = " + sum)
  }
}
