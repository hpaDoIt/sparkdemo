package com.hpa.spark.streaming.inputdstream.kafka.simple

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}

import scala.reflect.ClassTag

/**
  * Created by hpa on 2016/12/21.
  *
  * KafkaManager必须序列化，否则：
  * java.io.NotSerializableException: com.hpa.spark.streaming.inputdstream.kafka.simple.KafkaManager
  */
class KafkaManager(kafkaParams: Map[String, String]) extends Serializable{
  private val kc = new KafkaCluster(kafkaParams)

  /**
    * 根据offset创建一个DStream
    * @param ssc
    * @param kafkaParams
    * @param topics
    * @tparam K
    * @tparam V
    * @tparam KD
    * @tparam VD
    * @return
    */
  def createDirectDStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag,VD <: Decoder[V]:ClassTag](ssc: StreamingContext, kafkaParams: Map[String, String],topics: Set[String]):
    InputDStream[(K, V, String)] = {

    val groupId = kafkaParams.get("group.id").get
    //在zookeeper上读取offsets前先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)

    val messages = {
      val partitionsE = kc.getPartitions(topics)
      if(partitionsE.isLeft){
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      }
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if(consumerOffsetsE.isLeft){
        throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
      }
      val consumerOffsets = consumerOffsetsE.right.get
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V, String)](ssc, kafkaParams, consumerOffsets,
        (mmd: MessageAndMetadata[K, V]) => (mmd.key(), mmd.message(), mmd.topic))
    }

    messages
  }

  /**
    * 得到offset
    * @param topics
    * @param groupId
    */
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit ={
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kc.getPartitions(Set(topic))
      if(partitionsE.isLeft){
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      }
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if(consumerOffsetsE.isLeft)
        hasConsumed = false
      if(hasConsumed){//消费过
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if(earliestLeaderOffsetsE.isLeft){
          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        }
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get

        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({case(tp, n) =>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            if(n < earliestLeaderOffset){
              println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
                " offsets已经过时，更新为" + earliestLeaderOffset)

              offsets += (tp -> earliestLeaderOffset)
            }
        })
        if(!offsets.isEmpty){
          kc.setConsumerOffsets(groupId, offsets)
        }
      }else{//没有消费过
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase())
        var leaderOffsets : Map[TopicAndPartition, LeaderOffset] = null
        if(reset == Some("smallest")){//从头消费
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if(leaderOffsetsE.isLeft){
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          }
          leaderOffsets = leaderOffsetsE.right.get
        }else{//从最新offset处消费
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if(leaderOffsetsE.isLeft)
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map{
          case(tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }
  /**
    * 更新zk上的offset
    * @param rdd
    */
  def updateZKOffsets(rdd: RDD[(String, String, String)]): Unit = {
    val groupId = kafkaParams.get("group.id").get

    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for(offsets <- offsetsList){
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if(o.isLeft){
        println(s"Error updating the offset to kafka cluster: ${o.left.get}")
      }
    }
  }

}
