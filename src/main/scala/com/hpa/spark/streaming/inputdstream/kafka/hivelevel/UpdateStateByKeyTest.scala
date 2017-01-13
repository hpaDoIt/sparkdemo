package com.hpa.spark.streaming.inputdstream.kafka.hivelevel

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import net.liftweb.json._
import org.apache.spark.storage.StorageLevel
/**
  * Receiver模式
  * Created by hpa on 2016/12/25.
  */
object UpdateStateByKeyTest {
  def main(args: Array[String]): Unit = {
    def functionToCreateContext(): StreamingContext = {
      val sparkConf = new SparkConf().
        setAppName("UpdateStateByKeyTest").
        setMaster("local[2]")

      val ssc = new StreamingContext(sparkConf, Seconds(60))

      ssc.checkpoint("hdfs://192.168.1.210:9000/tmp/checkPoint")

      val zkQuorum = "192.168.1.210:2181,192.168.1.220:2181,192.168.1.230:2181"
      val consumerGroupName = "user_payment"
      val kafkaTopic = "user_payment"
      val kafkaThreadNum = 1

      val topicMap = kafkaTopic.split(",").
        map((_, kafkaThreadNum.toInt)).toMap

      //从Kafka读入数据并且解析json串
      val user_payment = KafkaUtils.createStream(ssc, zkQuorum, consumerGroupName, topicMap, StorageLevel.MEMORY_AND_DISK_SER).
        map(x =>{
          parse(x._2)
        })

      /**
        * 对一分钟的数据进行计算
        */
      val paymentSum = user_payment.map(jsonLine => {
        implicit val formats = DefaultFormats
        val user = (jsonLine \ "user").extract[String]
        val payment = (jsonLine \ "payment").extract[String]
        (user, payment.toDouble)
      }).filter(item => {
        if(item._1 != "zhangsan"){
          true
        }else{
          false
        }
      }).reduceByKey(_+_)

      //输出每分钟的计算结果
      paymentSum.print()

      //定义将以前的数据和最新一分钟的数据求和的函数
      val addFunction = (currValues: Seq[Double], preValueState: Option[Double]) => {
        val currentSum = currValues.sum
        val previousSum = preValueState.getOrElse(0.0)
        Some(currentSum + previousSum)
      }

      val totalPayment = paymentSum.updateStateByKey[Double](addFunction)

      //输出总计的结果
      totalPayment.print()

      ssc
    }

    //如果"checkPoint"中存在以前的记录，则重启streamingContext，读取以前保存的数据，否则创建新的StreamingContext
    val context = StreamingContext.getOrCreate("hdfs://192.168.1.210:9000/tmp/checkPoint", functionToCreateContext _)

    context.start()
    context.awaitTermination()
  }
}
