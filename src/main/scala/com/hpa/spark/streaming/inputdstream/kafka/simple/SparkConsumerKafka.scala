package com.hpa.spark.streaming.inputdstream.kafka.simple

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by hpa on 2016/12/22.
  * 192.168.1.210:9092,192.168.1.220:9092,192.168.1.230:9092 kafka2hdfs kafka2hdfsGroup hdfs://192.168.1.210:9000/data/output
  * 集群提交命令：
  *  spark-submit --class com.hpa.spark.streaming.inputdstream.kafka.simple.SparkConsumerKafka --master spark://slave01:7077 sparkdemo-1.0-SNAPSHOT-jar-with-dependencies.jar 192.168.1.210:9092,192.168.1.220:9092,192.168.1.230:9092 kafka2hdfs kafka2hdfsgroup hdfs://slave01:9000/data/output
  */
object SparkConsumerKafka {
  def main(args: Array[String]){

    def functionToCreateContext(): StreamingContext = {
      if (args.length < 4) {
        System.err.println( s"""
                               |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
                               |  <brokers> is a list of one or more Kafka brokers
                               |  <topics> is a list of one or more kafka topics to consume from
                               |  <groupid> is a consume group
                               |  <         > is a HDFS Path, like /user/admin/scalapath
                               |
        """.stripMargin)
        System.exit(1)
      }

      /**
        * 从Program Arguments获取参数
        */
      val Array(brokers, topics, groupId, hdfsPath) = args

      /**
        * 创建SparkConf，并设置应用名称
        * 注意：如果本地模式运行Spark Application且在程序中设置setMaster，
        *       则必须设置为setMaster(local[2])，线程个数大于2。
        */
      val sparkConf = new SparkConf().setAppName("SparkConsumerKafka")
      //val sparkConf = new SparkConf().setAppName("SparkConsumerKafka").setMaster("local[2]")

      /**
        * 基于sparkConf构造Spark Streaming上下文，并指定批处理时间间隔
        * 这里流式计算每隔60秒执行一次
        */
      val ssc = new StreamingContext(sparkConf, Seconds(60))
      ssc.checkpoint("hdfs://192.168.1.210:9000/checkPoint/SparkConsumerKafka")
      /**
        * topic集合
        */
      val topicsSet = topics.split(",").toSet

      /**
        * auto.offset.reset：largest；smallest。
        *   表示当此groupId下的消费者在ZK中没有offset值时（比如新的groupId，或者ZK数据被清空） ，consumer应该从哪个offset开始消费。
        *   （1）largest表示接收最大的offset（即新消息）；
        *   （2）smallest表示从topic的开始位置消费所有消息。
        */
      val kafkaParams = Map[String, String](
        "metadata.broker.list" -> brokers,
        "group.id" -> groupId,
        "auto.offset.reset" -> "smallest"
      )

      val km = new KafkaManager(kafkaParams)

      /**
        * 创建Direct方式的InputDStream
        */
      val messages = km.createDirectDStream[String, String,
        StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

      messages.foreachRDD(rdd => {
        if(!rdd.isEmpty()){

          rdd.foreachPartition{ partitionOfRecords =>
            val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
            import spark.implicits._

            val messageList = new ListBuffer[String]
            var topic = ""
            partitionOfRecords.foreach(record => {
              messageList += record._2
              if(topic == ""){
                topic = record._3
              }
            })

            val conf = new Configuration()
            val fs = FileSystem.get(conf)

            if(topic != ""){
              val path = new Path(hdfsPath + "/" + topic +
                Random.nextInt(100) + topic +
                System.currentTimeMillis())
              val outputStream = if(fs.exists(path)){
                fs.append(path)
              }else{
                fs.create(path)
              }
              try{
                val peopleDataFrame = messageList.map(x => x.split(",", -1)).map(w => People(w(0), w(1), w(2).toInt, w(3))).toDF()
                peopleDataFrame.createOrReplaceTempView("people")

                val resultPeopleDataFrame = spark.sql("SELECT name, phone, age FROM people " +
                  "WHERE phone LIKE '%183111%' AND age BETWEEN 20 AND 30")
                resultPeopleDataFrame.foreach(message => outputStream.write((message + "\n").getBytes("UTF-8")))
              }catch {
                case e: Exception => e.printStackTrace()
              }finally {
                outputStream.close()
              }
            }
          }

          //更新ZK上的offset
          km.updateZKOffsets(rdd)
        }
      })

      ssc
    }

    //如果"checkPoint"中存在以前的记录，则重启streamingContext，读取以前保存的数据，否则创建新的StreamingContext
    val ssc = StreamingContext.getOrCreate("hdfs://192.168.1.210:9000/checkPoint/SparkConsumerKafka", functionToCreateContext _)
    //正式启动计算
    ssc.start()

    //等待执行结束（程序出错退出 或者 CTRL + C退出）
    ssc.awaitTermination()
  }

  def processRdd(rdd: RDD[(String, String)], hdfspath: String): Unit = {

  }

}

case class People(name: String, phone: String, age: Int, sex: String)