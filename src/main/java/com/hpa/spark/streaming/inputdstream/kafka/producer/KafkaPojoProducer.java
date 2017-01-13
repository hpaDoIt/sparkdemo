package com.hpa.spark.streaming.inputdstream.kafka.producer;

import com.google.gson.Gson;
import com.hpa.spark.streaming.inputdstream.kafka.pojo.Student;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by hpa on 2016/12/28.
 */
public class KafkaPojoProducer {
    public static void main(String[] args){
        Properties props = new Properties();
        /*props.setProperty("metadata.broker.list",
                "192.168.1.210:9092,192.168.1.220:9092,192.168.1.230:9092");*/

        props.setProperty("metadata.broker.list",
                "10.1.235.49:9092,10.1.235.50:9093,10.1.235.51:9094");

        props.setProperty("serializer.class",
                "com.hpa.spark.streaming.inputdstream.kafka.pojo.StudentSerializer");

        props.setProperty("key.serializer.class",
                "kafka.serializer.StringEncoder");

        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, Student> producer = new Producer<String, Student>(config);

        Student student = new Student();
        student.setId("1212");
        student.setName("zhangsan");

        KeyedMessage<String, Student> message =
                new KeyedMessage<String, Student>("kafka", "student", student);

        try{
            while (true){
                producer.send(message);
                System.out.println("send " + new Gson().toJson(message));
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }

    }
}
