package com.hpa.spark.streaming.inputdstream.kafka.consumer;

import com.hpa.spark.streaming.inputdstream.kafka.pojo.Student;
import com.hpa.spark.streaming.inputdstream.kafka.pojo.StudentDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hpa on 2016/12/28.
 * http://www.aboutyun.com/forum.php?mod=viewthread&tid=13405&page=1
 */
public class SparkStreamingConsumerPojo {
    public static void main(String[] args)throws Exception{
        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("SparkStreamingConsumerPojo");

        if(args.length < 1){
            System.out.println("please input the program args: local[2] or cluser");
            System.exit(-1);
        }
        if("local[2]".equals(args[0])){
            sparkConf.setMaster("local[2]");
        }else if("cluster".equals(args[0])){
            sparkConf.setMaster("spark://slave01:7077");
            sparkConf.set("driver.memory", "512m");
            sparkConf.set("spark.executor.memory", "512m");
            sparkConf.set("spark.cores.max", "2");
            sparkConf.setJars(new String[]{"../SparkStreamingConsumerPojo.jar",
                    "../spark-streaming-kafka-assembly_2.11-2.0.1.jar"});
        }else{
            System.out.println("please input the program args: local[2] or cluser");
            System.exit(-1);
        }

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        //1表示numThreads
        topicMap.put("kafka", 1);

        Map<String, String> kafkaParam = new HashMap<String, String>();
        //kafkaParam.put("zookeeper.connect", "192.168.1.210:2181,192.168.1.220:2181,192.168.1.230:2181");
        kafkaParam.put("zookeeper.connect", "10.1.235.49:2182");
        kafkaParam.put("group.id", "0");

        JavaPairReceiverInputDStream<String, Student> stream = KafkaUtils.createStream(
                jssc,
                String.class,
                Student.class,
                StringDecoder.class,
                StudentDecoder.class,
                kafkaParam,topicMap,
                StorageLevels.MEMORY_AND_DISK_2);

        JavaDStream<String> value = stream.map(new Function<Tuple2<String,Student>, String>() {

            public String call(Tuple2<String, Student> stringStudentTuple2) throws Exception {
                return "student:" + stringStudentTuple2._2().getId() + ": " + stringStudentTuple2._2().getName() + "!";
            }
        });
        value.print();
        jssc.start();
        jssc.awaitTermination();

    }
}
