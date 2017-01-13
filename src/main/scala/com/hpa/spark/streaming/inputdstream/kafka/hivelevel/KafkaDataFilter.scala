package com.hpa.spark.streaming.inputdstream.kafka.hivelevel

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Created by hpa on 2016/12/26.
  */
object KafkaDataFilter {
  def main(args: Array[String]){
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("LogStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "192.168.1.210:2181,192.168.1.220:2181,192.168.1.230:2181",
      "LogStreamingTopic",
      Map[String, Int]("peopleTopic" -> 0,"peopleTopic" -> 1, "peopleTopic" -> 2),
      StorageLevel.MEMORY_AND_DISK_SER)

    val kafkaStreamMap = kafkaStream.map(x => x._2.split(",", -1))

    kafkaStreamMap.foreachRDD((rdd: RDD[Array[String]], time: Time) => {

      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val path = new Path("hdfs://192.168.1.210:9000/tmp/kafka_data/peopleTopic")
      val outputStream: FSDataOutputStream = if(fs.exists(path)){
        fs.append(path)
      }else{
        fs.create(path)
      }

      val peopleDataFrame = rdd.map(w => People(w(0), w(1),w(2).toInt, w(3))).toDF()

      peopleDataFrame.createOrReplaceTempView("people")

      val resultPeopleDataFrame = spark.sql("SELECT name, phone, age FROM people " +
        "WHERE phone LIKE '%183111%' AND age BETWEEN 20 AND 30")

      if(resultPeopleDataFrame.count() > 0){
        try{
          resultPeopleDataFrame.foreach(record => outputStream.write((record + "\n").getBytes("UTF-8")))
        }catch {
          case e: Exception => e.printStackTrace()
        }finally{
          outputStream.close()
        }
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }
}

case class People(name: String, phone: String, age: Int, sex: String)
