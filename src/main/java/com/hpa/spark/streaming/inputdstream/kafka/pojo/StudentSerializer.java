package com.hpa.spark.streaming.inputdstream.kafka.pojo;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang3.SerializationUtils;


/**
 * Created by hpa on 2016/12/28.
 */
public class StudentSerializer implements Encoder<Student>{
    public StudentSerializer(){

    }

    public StudentSerializer(VerifiableProperties properties){

    }


    public byte[] toBytes(Student student){
        return SerializationUtils.serialize(student);
    }
}
