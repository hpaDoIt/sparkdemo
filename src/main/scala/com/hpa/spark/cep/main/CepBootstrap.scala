package com.hpa.spark.cep.main

import java.net.URI

import com.hpa.spark.adapter.bean.{HdfsConfig, KafkaConfig}
import com.hpa.spark.bean.Ods28Ltec1S1mmHm
import com.hpa.spark.cep.bean.DBConfig
import com.hpa.spark.streaming.inputdstream.kafka.simple.KafkaManager
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.util.Random

/**
  * Created by hpa on 2016/2/5.
  * 一、本地测试环境参数
  * 192.168.1.210:9092,192.168.1.220:9092,192.168.1.230:9092 kafka2hdfs kafka2hdfsGroup hdfs://192.168.1.210:9000/data/output
  * 集群提交命令：
  *  spark-submit --class com.hpa.spark.streaming.inputdstream.kafka.simple.Bootstap --master spark://slave01:7077 sparkdemo-1.0-SNAPSHOT-jar-with-dependencies.jar 192.168.1.210:9092,192.168.1.220:9092,192.168.1.230:9092 kafka2hdfs kafka2hdfsgroup hdfs://slave01:9000/data/output
  *
  *  本地模式命令：
  *  spark-submit --class com.hpa.spark.streaming.inputdstream.kafka.simple.Bootstap --master local[2] sparkdemo-1.0-SNAPSHOT-jar-with-dependencies.jar 192.168.1.210:9092,192.168.1.220:9092,192.168.1.230:9092 kafka2hdfs kafka2hdfsGroup hdfs://192.168.1.210:9000/data/output
  *
  *  二、测试服务器环境参数：
  * 10.1.235.47:9092,10.1.235.50:9093,10.1.235.51:9094 cepUserPhone cepUserPhoneGroup hdfs://10.1.235.51:8020 /filterdata/sparkcep/
  * 集群提交命令：
  *  spark-submit --class com.hpa.spark.streaming.inputdstream.kafka.simple.Bootstap --master spark://10.1.235.51:7077 sparkdemo-1.0-SNAPSHOT-jar-with-dependencies.jar 10.1.235.47:9092,10.1.235.50:9093,10.1.235.51:9094 cepUserPhone cepUserPhoneGroup hdfs://10.1.235.51:8020 /filterdata/sparkcep/
  *
  *  本地模式命令：
  *  spark-submit --class com.hpa.spark.streaming.inputdstream.kafka.simple.Bootstap --master local[2] sparkdemo-1.0-SNAPSHOT-jar-with-dependencies.jar 10.1.235.47:9092,10.1.235.50:9093,10.1.235.51:9094 cepUserPhone cepUserPhoneGroup hdfs://10.1.235.51:8020 /filterdata/sparkcep/
  *
  */
object CepBootstrap {
  var dbConfg: DBConfig = null

  def loadConfig() = {
    println("开始初始化数据源输入输出配置... ...")
    val config = ConfigFactory.load("datasource-config")

    dbConfg = DBConfig(
      config.getString("dbconfig.mysql.username"),
      config.getString("dbconfig.mysql.password"),
      config.getString("dbconfig.mysql.url")
    )
    println("初始化数据源输入输出配置成功!!!")
  }
  def main(args: Array[String]){
    loadConfig

    if (args.length < 5) {
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
    val Array(brokers, topics, groupId, defaultFS, hdfsPath) = args

    /**
      * 创建SparkConf，并设置应用名称
      * 注意：如果本地模式运行Spark Application且在程序中设置setMaster，
      *       则必须设置为setMaster(local[2])，线程个数大于2。
      */
    val sparkConf = new SparkConf().setAppName("SparkCEP")
    //val sparkConf = new SparkConf().setAppName("SparkCEP").setMaster("local[2]")

    /**
      * 基于sparkConf构造Spark Streaming上下文，并指定批处理时间间隔
      * 这里流式计算每隔60秒执行一次
      */
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    val sc = ssc.sparkContext
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "/usr/hive/warehouse_v1").getOrCreate()
    //val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "/apps/hive/warehouse").getOrCreate()

    val cpeUserInfo = spark.read.format("jdbc").
      option("url",dbConfg.url).
      option("user",dbConfg.username).
      option("password", dbConfg.password).
      option("dbtable", "cpe_user_info")

    val cpeUserInfoList = cpeUserInfo.load().collectAsList()
    //val cpeUserInfoDF = cpeUserInfo.load()

    //sc.broadcast(cpeUserInfoDF)
    sc.broadcast(cpeUserInfoList)

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

    val messageDS = messages.map(x => x._2.split("|" , -1))

    messageDS.foreachRDD((rdd: RDD[Array[String]], time: Time) => {
      if(rdd.count() != 0){
        import spark.implicits._

        //imsi, msisdn, tac, eci, apn
        val messageDF = rdd.map(w => Ods28Ltec1S1mmHm(w(5), w(7), w(41), w(42), w(45))).toDF()
        //创建动态数据的视图
        messageDF.createOrReplaceTempView("ODS28_LTEC1_S1MME_HM")

        //创建静态数据的视图
        //cpeUserInfoDF.createOrReplaceTempView("CEP_USER_INFO")
        //val resultCpeUserDataFrame = spark.sql("SELECT IMSI, MSISDN, TAC, ECI, APN FROM ODS28_LTEC1_S1MME_HM, CEP_USER_INFO WHERE MSISDN = PRODUCT_NO")
        val resultCpeUserDataFrame = spark.sql("SELECT IMSI, MSISDN, TAC, ECI, APN FROM ODS28_LTEC1_S1MME_HM")

        //var fs: FileSystem = null

        resultCpeUserDataFrame.foreachPartition(rowRDD => {

          val topic = "cepUserPhone"
          val conf = new Configuration()
          //conf.set("fs.defaultFS", hdfsPath)
          //conf.set("fs.defaultFS", defaultFS)
          val fs: FileSystem = FileSystem.get(URI.create(defaultFS + "/" + hdfsPath), conf)
          val path = new Path(hdfsPath + "/" + topic + "/" +
            Random.nextInt(100) + topic +
            System.currentTimeMillis())
          //val path = new Path(hdfsPath + "/" + topic + ".txt")
          val outputStream = if (fs.exists(path)) {
            fs.append(path)
          } else {
            fs.create(path)
          }

          //val outputStream = fs.create(path)

          try{
            rowRDD.foreach(row => {
              println("row: $row")
              if(row.length > 0){
                val messageList = new StringBuilder
                if(messageList.length > 0){
                  messageList.append("," + row(0).toString + "," + row(1).toString + "," + row(2).toString + "," + row(3).toString + "," + row(4).toString)
                }else{
                  messageList.append(row(0).toString + "," + row(1).toString + "," + row(2).toString + "," + row(3).toString + "," + row(4).toString)
                }
                println(s"向HDFS发送消息：$messages")
                outputStream.write((messageList.toString() + "\n").getBytes("UTF-8"))
              }

            })
            outputStream.hflush()
          }catch {
            case e:Exception => e.printStackTrace()
          }finally {
            if(outputStream != null){
              outputStream.close()
            }
            if(fs != null){
              fs.close()
            }
          }

        })

      }

    })

    //正式启动计算
    ssc.start()

    //等待执行结束（程序出错退出 或者 CTRL + C 退出）
    ssc.awaitTermination()
  }

}
