package com.hpa.spark.streaming.inputdstream.kafka.pojo;

import java.io.Serializable;

/**
 * Created by hpa on 2016/12/28.
 */
public class Student implements Serializable
{
    private String id;
    private String name;

    public Student(){}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
