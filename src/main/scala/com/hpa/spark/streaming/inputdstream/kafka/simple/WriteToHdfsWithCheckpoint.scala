package com.hpa.spark.streaming.inputdstream.kafka.simple

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hpa on 2016/12/22.
  */
object WriteToHdfsWithCheckpoint extends Serializable{
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <hdfspath> is a HDFS Path, like /user/admin/scalapath
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics, hdfspath) = args
    val conf = new SparkConf().setAppName("ConsumerKafkaToHdfsWithCheckPoint")

    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest")

    val checkpointPath = "hdfs://192.168.1.210:9000/spark_checkpoint"

    def function2CreateContext(): StreamingContext = {
      val ssc = new StreamingContext(conf, Seconds(2))
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      ssc.checkpoint(checkpointPath)

      messages.foreachRDD(rdd => {
        if(!rdd.isEmpty()){
          processRdd(rdd, hdfspath)
        /*rdd.foreachPartition{ partitionOfRecords =>
          val conf = new Configuration
          val fs = FileSystem.get(conf)
          val path = new Path(hdfspath)
          val outputStream: FSDataOutputStream = if(fs.exists(path)){
            fs.append(path)
          }else{
            fs.create(path)
          }
          try{
            partitionOfRecords.foreach(record => outputStream.write((record + "\n").getBytes("UTF-8")))
          }catch {
            case e: Exception => e.printStackTrace()
          }finally {
            outputStream.close()
          }*/

        }
      })
      ssc
    }

    val context = StreamingContext.getOrCreate(checkpointPath, function2CreateContext _)
    context.start()
    context.awaitTermination()
  }

  def processRdd(rdd: RDD[(String, String)], hdfspath: String): Unit = {
    val messages = rdd.map(_._2)
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val path = new Path(hdfspath)
    val outputStream: FSDataOutputStream = if(fs.exists(path)){
      fs.append(path)
    }else{
      fs.create(path)
    }
    try{
      messages.foreach(record => outputStream.write((record + "\n").getBytes("UTF-8")))
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      outputStream.close()
    }

  }

}
