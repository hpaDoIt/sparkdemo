package com.hpa.spark.streaming.inputdstream.kafka.pojo;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by hpa on 2016/12/28.
 */
public class StudentDecoder implements Decoder<Student>{
    public StudentDecoder(){

    }

    public StudentDecoder(VerifiableProperties properties){

    }

    public Student fromBytes(byte[] bytes) {
        return null;
    }
}
