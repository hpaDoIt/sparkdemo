package com.hpa.adapter

import java.util.Properties

import scala.io.Source
import scala.reflect.io.Path

import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig


/**
  * Kafka Producer
  */
class KafkaProduceMsg0(brokerList : String, topic : String) {

  private val BROKER_LIST = brokerList //"master:9092,worker1:9092,worker2:9092"
  private val TARGET_TOPIC = topic //"new"

  /**
    * 1、配置属性
    * metadata.broker.list : kafka集群的broker，只需指定2个即可
    * serializer.class : 如何序列化发送消息
    * request.required.acks : 1代表需要broker接收到消息后acknowledgment,默认是0
    * producer.type : 默认就是同步sync
    */
  private val props = new Properties()
  props.put("metadata.broker.list", this.BROKER_LIST)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  props.put("producer.type", "async")

  /**
    * 2、创建Producer
    */
  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)

  /**
    * 3、产生并发送消息
    * 搜索目录dir下的所有包含“transaction”的文件并将每行的记录以消息的形式发送到kafka
    *
    */
  def sendMsg(msg: String) : Unit = {
    try{
      val message = new KeyedMessage[String, String](this.TARGET_TOPIC, msg)
      this.producer.send(message)
    }catch{
      case e : Exception => println(e)
    }
  }
  
  def closeProducer() {
    this.producer.close();
  }
}