package com.hpa.dataframe

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/12/11.
  */
object RDD2DataFrameByProgrammatically {
  def main(args: Array[String]){
    val conf = new SparkConf().
      setMaster("local[2]").
      setAppName("RDD2DataFrameByProgrammatically")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://slave01:9000/library/SparkSQL/Data/text.txt")

    val personRDD = lines.map(line => line.split(",")).collect().foreach(word => println(word))
  }
}
